# Third-party notices

Original StreamSQLBench project code follows the repository's Apache License 2.0, copied in `LICENSE`.

`runtime_support/compat/spider2_exec_result_compat_v1.py` derives its table-comparison semantics from `xlang-ai/Spider2`, `spider2-lite/evaluation_suite/evaluate.py`, under the MIT License (Copyright (c) 2024 bird_sql). The implementation retains its provenance notice and adds fixed-manifest/missing-prediction accounting. See [licenses/Spider2-MIT.txt](licenses/Spider2-MIT.txt) and the [upstream license](https://github.com/xlang-ai/Spider2/blob/main/LICENSE).

Apache Flink, Apache Kafka, and the Flink Kafka connector are external Apache projects. Their binaries are obtained separately by Docker and `scripts/fetch_connector.py`, with the connector version and digest fixed. Python, pandas, NumPy, sqlglot, python-dateutil, six, and confluent-kafka retain their respective upstream licenses. They are installed as dependencies; no model weights or upstream repository checkout is bundled.

A software license does not supersede the source databases' or benchmark datasets' access and redistribution terms. This code directory contains no benchmark source rows, Gold data, reference SQL, or private credentials.
