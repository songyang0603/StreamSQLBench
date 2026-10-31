#!/usr/bin/env python3
"""Fetch the pinned Apache Flink Kafka connector from Maven Central."""
from pathlib import Path
import argparse
import hashlib
import urllib.request

DESTINATION = Path(__file__).resolve().parents[1] / 'runtime/vendor/flink-sql-connector-kafka-3.4.0-1.20.jar'
URL = 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar'
EXPECTED = 'b860724fd97d7da0901bfc3859680442f4c1aa2b35ab944407558b393a1cf93e'


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    if DESTINATION.exists():
        data = DESTINATION.read_bytes()
    else:
        with urllib.request.urlopen(URL, timeout=120) as response:
            data = response.read()
    if hashlib.sha256(data).hexdigest() != EXPECTED:
        raise ValueError('connector checksum mismatch; existing files were not changed')
    if not DESTINATION.exists():
        DESTINATION.parent.mkdir(parents=True, exist_ok=True)
        DESTINATION.write_bytes(data)
    print(f'Verified connector: {DESTINATION.name}, {len(data)} bytes')


if __name__ == '__main__':
    main()
