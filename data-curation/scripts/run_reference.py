"""Run the existing real Kafka/Flink pipeline with lossless original time parsing."""
from timestamp_compat import install
install()
from streamsqlbench_curation.ordered_streaming import main

if __name__=="__main__":
    raise SystemExit(main())
