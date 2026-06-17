"""
Fraud Stream Job — real Apache Flink replacement for the Python mimic in
demo/stream_processor/processor.py.

Pipeline:
  raw.claims.new (Kafka source)
    -> keyBy(provider_npi) -> ProviderEnrichFunction   (Flink keyed state, RocksDB, 30d TTL)
    -> keyBy(member_id)    -> MemberEnrichFunction      (Flink keyed state, RocksDB, 90d TTL)
    -> ScoreFunction        (rule scoring + fraud-scorer HTTP call)
    -> scored.claims (always) / alerts.fraud (score >= threshold)

Same scoring logic as the Python mimic, but the rolling provider/member stats
now live in genuine Flink keyed state instead of Redis.
"""
import json
import os

import requests
from pyflink.common import Time, Types
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common import WatermarkStrategy

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda.streaming.svc.cluster.local:9092")
FRAUD_SCORER_URL = os.getenv("FRAUD_SCORER_URL", "http://fraud-scorer.ml.svc.cluster.local:8000")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.65"))

PROCEDURE_RISK = {
    "27447": 0.90, "29827": 0.85, "27130": 0.88, "29881": 0.80,
    "93306": 0.65, "99291": 0.70,
    "99213": 0.10, "99223": 0.25, "93000": 0.20,
    "80053": 0.05, "85025": 0.05, "80061": 0.05, "82947": 0.05,
    "97110": 0.10, "97012": 0.10, "97140": 0.10, "97530": 0.10,
}

ORTHO_PROCS = {"27447", "29827", "29881", "27130"}
CARDIO_DIAGS = {"I10", "I25.10", "I48.0", "I50.9"}


def _ttl(days: int) -> StateTtlConfig:
    return (
        StateTtlConfig.new_builder(Time.days(days))
        .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .never_return_expired()
        .build()
    )


class ProviderEnrichFunction(KeyedProcessFunction):
    """Keyed by provider_npi. Tracks 30-day rolling claim count / avg amount."""

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("provider-stats", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(_ttl(30))
        self.state = runtime_context.get_state(descriptor)

    def process_element(self, claim, ctx: "KeyedProcessFunction.Context"):
        stats = self.state.value() or {"sum": 0.0, "count": 0}
        stats["sum"] += claim["amount"]
        stats["count"] += 1
        self.state.update(stats)

        claim["claim_count_30d"] = stats["count"]
        claim["avg_amount_30d"] = round(stats["sum"] / stats["count"], 2)
        yield claim


class MemberEnrichFunction(KeyedProcessFunction):
    """Keyed by member_id. Tracks 90-day rolling claim count."""

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("member-stats", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(_ttl(90))
        self.state = runtime_context.get_state(descriptor)

    def process_element(self, claim, ctx: "KeyedProcessFunction.Context"):
        count = (self.state.value() or 0) + 1
        self.state.update(count)

        claim["member_claim_count_90d"] = count
        yield claim


def _apply_rules(claim: dict) -> tuple:
    score = 0.0
    triggers = []
    avg = claim["avg_amount_30d"]

    if avg > 0 and claim["amount"] > avg * 3.0:
        score = max(score, 0.78)
        triggers.append("AMOUNT_3X_PROVIDER_AVG")

    if claim["amount"] > 50_000:
        score = max(score, 0.88)
        triggers.append("EXTREME_AMOUNT")

    if claim.get("procedure_code") in ORTHO_PROCS and claim.get("diagnosis_code") in CARDIO_DIAGS:
        score = max(score, 0.93)
        triggers.append("PROCEDURE_DIAGNOSIS_MISMATCH")

    if claim["claim_count_30d"] > 200:
        score = max(score, 0.62)
        triggers.append("HIGH_VOLUME_PROVIDER")

    return round(score, 4), triggers


class ScoreFunction(MapFunction):
    """Rule scoring + ML scoring via the fraud-scorer HTTP endpoint."""

    def open(self, runtime_context: RuntimeContext):
        self.session = requests.Session()

    def map(self, claim):
        from datetime import datetime

        rule_score, triggers = _apply_rules(claim)

        now = datetime.now()
        body = {
            "claim_id": claim["claim_id"],
            "amount": claim["amount"],
            "claim_count_30d": claim["claim_count_30d"],
            "avg_amount_30d": claim["avg_amount_30d"],
            "hour_of_day": now.hour,
            "is_weekend": int(now.weekday() >= 5),
            "procedure_risk": PROCEDURE_RISK.get(claim.get("procedure_code", ""), 0.30),
            "member_claim_count_90d": claim["member_claim_count_90d"],
        }
        try:
            resp = self.session.post(f"{FRAUD_SCORER_URL}/score", json=body, timeout=2.0)
            resp.raise_for_status()
            ml_score = resp.json()["fraud_probability"]
        except Exception:
            ml_score = 0.0

        final = round(max(rule_score, ml_score * 0.6 + rule_score * 0.4), 4)
        risk = (
            "CRITICAL" if final >= 0.90 else
            "HIGH" if final >= 0.70 else
            "MEDIUM" if final >= 0.40 else
            "LOW"
        )

        claim["fraud_score"] = final
        claim["rule_score"] = rule_score
        claim["ml_score"] = ml_score
        claim["risk_level"] = risk
        claim["triggered_rules"] = triggers
        claim["scored_at"] = now.isoformat()
        return claim


class AboveThreshold(FilterFunction):
    def filter(self, claim):
        return claim["fraud_score"] >= FRAUD_THRESHOLD


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.claims.new")
        .set_group_id("flink-fraud-stream-v2")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    claims = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "raw-claims-source")
        .map(json.loads, output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda c: c["provider_npi"], key_type=Types.STRING())
        .process(ProviderEnrichFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda c: c["member_id"], key_type=Types.STRING())
        .process(MemberEnrichFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    scored = claims.map(ScoreFunction(), output_type=Types.PICKLED_BYTE_ARRAY())

    scored_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("scored.claims")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    scored.map(json.dumps, output_type=Types.STRING()).sink_to(scored_sink)

    alerts_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("alerts.fraud")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    (
        scored.filter(AboveThreshold())
        .map(json.dumps, output_type=Types.STRING())
        .sink_to(alerts_sink)
    )

    env.execute("fraud-stream-job")


if __name__ == "__main__":
    main()
