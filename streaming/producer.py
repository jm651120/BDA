#!/usr/bin/env python3
"""
producer.py  –  Send each row of a CSV file to a Kafka topic at a fixed rate.

Usage:
  python producer.py --csv streaming.csv --topic streaming --brokers kafkainterface.ddns.net:9092 --rate 2

Requires: pip install kafka-python
"""
import csv, json, time, argparse
from kafka import KafkaProducer

def build_producer(brokers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode(),  # dict → JSON bytes
        key_serializer=lambda k: k.encode() if k else None,
        retries=3,
        linger_ms=5
    )

def stream_csv(csv_path: str, producer: KafkaProducer, topic: str,
               rate: float = 1.0) -> None:
    """Send each CSV row as a message; key = row number (string)."""
    period = 1.0 / rate
    with open(csv_path, newline="") as fh:
        rdr = csv.DictReader(fh)
        for idx, row in enumerate(rdr):
            producer.send(topic, value=row, key=str(idx))
            producer.flush()
            print(f"sent row {idx}")
            time.sleep(period)
    producer.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Input CSV file")
    ap.add_argument("--topic", required=True, help="Kafka topic")
    ap.add_argument("--brokers", default="localhost:9092",
                    help="Comma-separated broker list")
    ap.add_argument("--rate", type=float, default=2,
                    help="Rows per second (default 2)")
    args = ap.parse_args()

    prod = build_producer(args.brokers)
    stream_csv(args.csv, prod, args.topic, args.rate)
