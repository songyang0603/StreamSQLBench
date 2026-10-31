"""Small Docker-runner worker for invocation-owned ordered-streaming topics.

This file is sent to the existing runner with ``python -c``.  It has no model
client and receives no Gold or Oracle.  Only explicitly named fresh topics are
created or cleaned up.
"""
from __future__ import annotations

import json
import re
import sys
import time


class PublicationOrderVerifier:
    """Validate the host's exact phase-aware publication key.

    Static snapshots deliberately precede live streams.  A static Kafka record
    therefore has a transport timestamp of zero even when a following live
    event's unchanged source timestamp predates the Unix epoch.  Comparing the
    two transport timestamps directly would reject a valid `(phase, ts, source,
    ordinal)` order.  Within each phase/topic, source time must still be
    nondecreasing.
    """

    def __init__(self):
        self.last_global_key = None
        self.last_phase_topic_timestamp_us = {}

    def check(self, row):
        phase = int(row["phase"])
        timestamp_us = int(row["ordering_timestamp_us"])
        timestamp_ms = int(row["timestamp_ms"])
        source = row["source"]
        ordinal = int(row["ordinal"])
        topic = row["topic"]
        if phase not in {0, 1}:
            raise ValueError("publication phase must be static=0 or live=1")
        if not isinstance(source, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source):
            raise ValueError("invalid publication source name")
        if ordinal < 0:
            raise ValueError("publication ordinal must be nonnegative")
        expected_timestamp_ms = 0 if phase == 0 else timestamp_us // 1000
        if timestamp_ms != expected_timestamp_ms:
            raise ValueError("Kafka timestamp does not match the declared ordering timestamp")

        phase_topic = (phase, topic)
        previous_topic_timestamp = self.last_phase_topic_timestamp_us.get(phase_topic)
        if previous_topic_timestamp is not None and timestamp_us < previous_topic_timestamp:
            raise ValueError(
                "topic publication timestamps are not nondecreasing within phase: " + topic
            )

        global_key = (phase, timestamp_us, source, ordinal)
        if self.last_global_key is not None and global_key < self.last_global_key:
            raise ValueError("global phase-aware publication keys are not nondecreasing")

        self.last_phase_topic_timestamp_us[phase_topic] = timestamp_us
        self.last_global_key = global_key
        return global_key


def wait_for_topic_absence(admin, topics, timeout_seconds=30):
    """Wait for Kafka metadata to stop advertising every invocation-owned topic."""
    deadline = time.monotonic() + timeout_seconds
    last_visible = list(topics)
    while True:
        metadata = admin.list_topics(timeout=10)
        last_visible = [topic for topic in topics if topic in metadata.topics]
        if not last_visible:
            return []
        if time.monotonic() >= deadline:
            raise TimeoutError("deleted topics remain visible: " + ", ".join(last_visible))
        time.sleep(.2)


def wait_for_topic_presence(admin, topics, timeout_seconds=30):
    """Wait until every newly created topic has usable partition metadata.

    Kafka's create-topic future can complete just before a subsequent metadata
    request on the client sees the new topics.  Treating that short propagation
    interval as a task failure leaves invocation-owned topics behind and makes
    large, otherwise valid runs flaky.  This wait is bounded and still rejects
    persistent missing topics or topic-level metadata errors.
    """
    deadline = time.monotonic() + timeout_seconds
    last_missing = list(topics)
    last_errors = {}
    while True:
        metadata = admin.list_topics(timeout=10)
        last_missing = [name for name in topics if name not in metadata.topics]
        last_errors = {
            name: str(metadata.topics[name].error)
            for name in topics
            if name in metadata.topics and metadata.topics[name].error is not None
        }
        if not last_missing and not last_errors:
            return metadata
        if time.monotonic() >= deadline:
            raise TimeoutError(str({
                "missing_topics": last_missing,
                "topic_errors": last_errors,
            }))
        time.sleep(.2)


def collection_position(consumer, topic_partition, frozen_end_offset):
    """Return a usable read position for a frozen committed output range.

    A fresh Kafka partition with no committed records has the exact range
    ``[0, 0)``.  Before the first poll, librdkafka may report an invalid
    negative position sentinel even though that empty range is already fully
    consumed.  Treat only the proven empty range as position zero; non-empty
    ranges must still use the consumer's observed position.
    """
    if frozen_end_offset < 0:
        raise ValueError("frozen output end offset must be nonnegative")
    if frozen_end_offset == 0:
        return 0
    return consumer.position([topic_partition])[0].offset


def refresh_initial_empty_end_offset(consumer, topic_partition, initial_end_offset,
                                     empty_visibility_grace_seconds=3):
    """Give a just-finished transactional sink time to become read-committed.

    The first watermark lookup may briefly report zero immediately after Flink
    reaches ``FINISHED``.  Refresh only that ambiguous zero during a short,
    bounded visibility grace.  A nonzero end offset is frozen immediately;
    a zero that remains zero for the full grace is a legitimate empty range.
    """
    if initial_end_offset < 0 or empty_visibility_grace_seconds < 0:
        raise ValueError("output offsets and visibility grace must be nonnegative")
    if initial_end_offset > 0 or empty_visibility_grace_seconds == 0:
        return initial_end_offset
    deadline = time.monotonic() + empty_visibility_grace_seconds
    while time.monotonic() < deadline:
        time.sleep(.2)
        low, high = consumer.get_watermark_offsets(topic_partition, timeout=10, cached=False)
        if low != 0:
            raise RuntimeError("fresh output topic lost its beginning")
        if high > 0:
            return high
    return 0


def describe_topic_retention_ms(admin, config_resource_class, topic_resource_type, topics):
    """Read back the effective per-topic time-retention setting.

    Dependencies are parameters so this small contract remains unit-testable
    without importing the broker client on developer machines.
    """
    resources = [config_resource_class(topic_resource_type, topic) for topic in topics]
    observed = {}
    for resource, future in admin.describe_configs(resources).items():
        config = future.result(timeout=25)
        entry = config.get("retention.ms")
        observed[resource.name] = None if entry is None else str(getattr(entry, "value", entry))
    return observed


def main():
    from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

    request = json.loads(sys.stdin.readline())
    topics = request["topics"]
    if not topics or any(not re.fullmatch(r"ssb\.ordered\.[a-f0-9]{24}\.[A-Za-z0-9_.-]+", t) for t in topics):
        raise ValueError("only invocation-owned ordered topics are accepted")
    bootstrap_servers = request.get("bootstrap_servers", "kafka:29092")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+:\d{1,5}(?:,[A-Za-z0-9_.-]+:\d{1,5})*", bootstrap_servers):
        raise ValueError("unsafe Kafka bootstrap server list")
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def verified_partition_counts(topic_names):
        metadata = wait_for_topic_presence(admin, topic_names)
        counts = {name: len(metadata.topics[name].partitions) for name in topic_names}
        partition_ids = {name: sorted(metadata.topics[name].partitions) for name in topic_names}
        if any(count != 1 for count in counts.values()) or any(ids != [0] for ids in partition_ids.values()):
            raise RuntimeError(str({"topic_partition_counts": counts, "topic_partition_ids": partition_ids}))
        return counts, partition_ids

    mode = request["mode"]
    if mode == "publish":
        for future in admin.create_topics([
            NewTopic(topic, 1, 1, config={"retention.ms": "-1"})
            for topic in topics
        ], operation_timeout=20).values():
            future.result(timeout=25)
        partition_counts, partition_ids = verified_partition_counts(topics)
        # Source timestamps are intentionally preserved and can be years old.
        # Broker time retention must therefore be disabled for these fresh,
        # invocation-owned topics; explicit cleanup still bounds their lifetime.
        retention_ms = describe_topic_retention_ms(
            admin, ConfigResource, ResourceType.TOPIC, topics
        )
        if retention_ms != {topic: "-1" for topic in topics}:
            raise RuntimeError(
                "invocation topics do not disable time retention: " + repr(retention_ms)
            )
        counts = {t: 0 for t in topics}
        errors = []
        order_verifier = PublicationOrderVerifier()
        published_messages_partition_zero = True
        producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True,
                             "acks": "all", "queue.buffering.max.kbytes": 32768})
        def delivered(error, message):
            nonlocal published_messages_partition_zero
            if error:
                errors.append(str(error))
            else:
                topic = message.topic()
                if message.partition() != 0:
                    published_messages_partition_zero = False
                if message.partition() != 0 or message.offset() != counts[topic]:
                    errors.append("noncontiguous input offsets: " + topic)
                counts[topic] += 1
        for line in sys.stdin:
            row = json.loads(line)
            topic = row["topic"]
            if topic not in counts:
                raise ValueError("undeclared topic")
            timestamp_ms = int(row.get("timestamp_ms", 0))
            order_verifier.check(row)
            while True:
                try:
                    producer.produce(topic, row["payload"].encode(), partition=0,
                                     key=row["key"].encode() if row.get("key") is not None else None,
                                     timestamp=timestamp_ms, on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(.05)
            producer.poll(0)
        remaining = producer.flush(60)
        if remaining or errors or counts != request["expected_counts"]:
            raise RuntimeError(str({"remaining": remaining, "errors": errors, "counts": counts}))
        print(json.dumps({
            "created_topics": topics,
            "retention_ms": retention_ms,
            "infinite_retention_verified": True,
            "acknowledged_counts": counts,
            "topic_partition_counts": partition_counts,
            "topic_partition_ids": partition_ids,
            "input_topic_partition_counts_verified": True,
            "all_published_messages_partition_zero": published_messages_partition_zero,
            "merged_publication_timestamps_nondecreasing_verified": True,
            "per_topic_publication_timestamps_nondecreasing_verified": True,
            "publication_order_key_fields": [
                "phase", "ordering_timestamp_us", "source", "ordinal"
            ],
            "timestamp_verification_basis": (
                "actual producer input control envelopes checked by phase-aware total key; "
                "within-phase per-topic source timestamps checked independently"
            ),
            "pre_epoch_live_timestamps_supported_without_payload_reclocking": True,
        }))
    elif mode == "collect":
        if len(topics) != 1:
            raise ValueError("collect requires one topic")
        topic = topics[0]
        partition_counts, partition_ids = verified_partition_counts(topics)
        consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": request["group"],
                             "enable.auto.commit": False, "isolation.level": "read_committed",
                             "enable.partition.eof": True})
        tp = TopicPartition(topic, 0)
        low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
        if low != 0:
            raise RuntimeError("fresh output topic lost its beginning")
        initial_high = high
        empty_visibility_grace_seconds = int(request.get("empty_visibility_grace_seconds", 3))
        high = refresh_initial_empty_end_offset(
            consumer, tp, high, empty_visibility_grace_seconds,
        )
        consumer.assign([TopicPartition(topic, 0, 0)])
        count = 0
        deadline = time.monotonic() + int(request.get("timeout_seconds", 300))
        try:
            while True:
                if collection_position(consumer, tp, high) >= high:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("cannot drain committed final output offsets")
                message = consumer.poll(.2)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(message.error()))
                if message.value() is None:
                    raise ValueError("Debezium sink unexpectedly emitted a tombstone")
                print(json.dumps({"offset": message.offset(), "value": message.value().decode()}), flush=True)
                count += 1
            print(json.dumps({"collection_complete": True, "isolation": "read_committed",
                              "initial_end_offset_exclusive": initial_high,
                              "end_offset_exclusive": high, "record_count": count,
                              "consumer_position": collection_position(consumer, tp, high),
                              "empty_visibility_grace_seconds": empty_visibility_grace_seconds,
                              "topic_partition_counts": partition_counts,
                              "topic_partition_ids": partition_ids,
                              "output_topic_partition_count_verified": True,
                              "all_collected_messages_partition_zero": True}))
        finally:
            consumer.close()
    elif mode == "verify_absent":
        metadata = admin.list_topics(timeout=10).topics
        remaining = [topic for topic in topics if topic in metadata]
        print(json.dumps({
            "remaining_topics": remaining,
            "topics_absent_verified": remaining == [],
            "verification_basis": "independent Kafka metadata listing without deletion",
        }))
    elif mode == "delete":
        # Caller must have observed FINISHED/CANCELED/FAILED and saved receipts.
        if request.get("job_terminal") is not True:
            raise ValueError("topic cleanup requires an observed terminal job")
        metadata = admin.list_topics(timeout=10).topics
        present = [t for t in topics if t in metadata]
        for future in admin.delete_topics(present, operation_timeout=20).values() if present else []:
            future.result(timeout=25)
        remaining = wait_for_topic_absence(admin, topics)
        print(json.dumps({
            "delete_acknowledged": present,
            "remaining_topics": remaining,
            "topics_absent_verified": remaining == [],
            "verification_basis": "post-delete Kafka metadata listing",
        }))
    else:
        raise ValueError("unsupported operation")


if __name__ == "__main__":
    main()
