"""Lossless parsing of original timestamp representations; never change payloads."""
import datetime as dt
import re
import sys
import unicodedata
from pathlib import Path
from streamsqlbench_curation import ordered_streaming
_original_timestamp_us=ordered_streaming.timestamp_us
_original_source_timestamp_us=ordered_streaming.source_timestamp_us


def normalized_iso(value):
    text=str(value)
    text="".join(str(unicodedata.decimal(c)) if c.isdecimal() else c for c in text)
    if text.endswith(" UTC"):
        text=text[:-4]+"+00:00"
    match=re.search(r"(\d{2}:\d{2}:\d{2})[.,](\d+)(?=(?:Z|[+-]\d{2}:\d{2})?$)",text)
    if match:
        digits=match.group(2)
        if len(digits)>6:
            if any(c!="0" for c in digits[6:]):
                raise ValueError("timestamp precision exceeds supported microseconds")
            digits=digits[:6]
        replacement=match.group(1)+"."+digits.ljust(6,"0")
        text=text[:match.start()]+replacement+text[match.end():]
    return text


def timestamp_us(value,unit):
    if unit=="iso" and value is not None and value!="":
        value=normalized_iso(value)
    elif unit=="delivery_center_12h_text" and value is not None and value!="":
        # Spider 2 delivery_center stores original stage clocks as
        # M/D/YYYY h:mm:ss AM/PM strings.  Parse that declared representation
        # only for the private producer ordering key; the source JSON payload
        # and the Flink SQL input strings remain byte-for-byte unchanged.
        try:
            parsed=dt.datetime.strptime(str(value).strip(),"%m/%d/%Y %I:%M:%S %p")
        except (TypeError,ValueError) as exc:
            raise ValueError("invalid declared delivery_center 12-hour timestamp") from exc
        value=parsed.replace(tzinfo=dt.timezone.utc).isoformat()
        unit="iso"
    return _original_timestamp_us(value,unit)


def source_timestamp_us(row, source):
    """Support a reviewed latest-original-field lifecycle clock.

    This changes only the producer ordering key.  Every source payload value is
    retained verbatim.  It is needed when an original completion timestamp is
    anomalously earlier than the original creation timestamp: publishing at
    the later original timestamp prevents either field from being revealed
    before its own clock.
    """
    adapter = source.get("timestamp_adapter")
    if isinstance(adapter, dict) and adapter.get("kind") == "date_plus_time_of_day" \
            and adapter.get("date_format") == "iso_datetime_midnight":
        required = {"kind", "date_field", "time_field", "date_format", "time_format", "naive_timezone"}
        if set(adapter) != required or adapter.get("time_format") != "hh:mm:ss" \
                or adapter.get("naive_timezone") != "UTC":
            raise ValueError("midnight-date composite adapter has unexpected fields")
        date_value = normalized_iso(row.get(adapter["date_field"]))
        time_value = normalized_iso(row.get(adapter["time_field"]))
        try:
            base = dt.datetime.fromisoformat(date_value)
            clock = dt.datetime.strptime(time_value, "%H:%M:%S").time()
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid original midnight-date plus time-of-day clock") from exc
        if base.time() != dt.time(0, 0):
            raise ValueError("declared date component contains a non-midnight time")
        combined = dt.datetime.combine(base.date(), clock, tzinfo=dt.timezone.utc)
        return timestamp_us(combined.isoformat(), "iso")
    if not isinstance(adapter, dict) or adapter.get("kind") != "max_non_null_timestamp":
        return _original_source_timestamp_us(row, source)
    required = {"kind", "fields", "timestamp_unit", "naive_timezone", "selection_contract"}
    if set(adapter) != required:
        raise ValueError("max-non-NULL timestamp adapter has unexpected fields")
    if adapter.get("selection_contract") != "latest_non_null_original_timestamp":
        raise ValueError("unsupported max-non-NULL timestamp selection contract")
    if adapter.get("timestamp_unit") == "iso" and adapter.get("naive_timezone") != "UTC":
        raise ValueError("max-non-NULL ISO timestamps must declare naive_timezone=UTC")
    fields = adapter.get("fields")
    if not isinstance(fields, list) or len(fields) < 2 or len(fields) != len(set(fields)):
        raise ValueError("max-non-NULL timestamp fields must be a unique list")
    values = [timestamp_us(row.get(field), adapter.get("timestamp_unit"))
              for field in fields if row.get(field) not in {None, ""}]
    if not values:
        raise ValueError("all declared original availability timestamps are missing")
    return max(values)


def install():
    ordered_streaming.timestamp_us=timestamp_us
    ordered_streaming.source_timestamp_us=source_timestamp_us
