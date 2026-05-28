"""
Stream Processor — Demo Component

In production this role is played by Apache Flink on LKE running three jobs:
  1. ClaimEnrichmentJob    — attaches provider/member context
  2. RuleBasedScoringJob   — deterministic fraud rules (stateful, 30-day windows)
  3. MLScoringJob          — calls BentoML inference endpoint
  4. AlertGenerationJob    — generates alerts above threshold

In the demo, all four jobs are combined in this single Python consumer.
Provider/member state that Flink would keep in RocksDB is stored in Redis here.

Flow:
  Redpanda raw.claims.new
    → enrich (Redis lookup)
    → rule scoring
    → ML scoring (fraud-scorer API)
    → combine scores
    → publish to scored.claims + alerts.fraud
"""
import json
import logging
import os
import time
from datetime import datetime

import redis
import requests
from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────────
KAFKA_BROKERS    = os.getenv("KAFKA_BROKERS",    "localhost:19092")
FRAUD_SCORER_URL = os.getenv("FRAUD_SCORER_URL", "http://localhost:8000")
REDIS_HOST       = os.getenv("REDIS_HOST",       "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT",   "6379"))
METRICS_PORT     = int(os.getenv("METRICS_PORT", "8001"))
FRAUD_THRESHOLD  = float(os.getenv("FRAUD_THRESHOLD", "0.65"))

# ── Prometheus metrics ──────────────────────────────────────────────────────────
CLAIMS_TOTAL   = Counter("stream_processor_claims_total",        "Claims processed",   ["result"])
ALERTS_TOTAL   = Counter("stream_processor_fraud_alerts_total",  "Fraud alerts raised", ["risk_level"])
SCORE_DURATION = Histogram(
    "stream_processor_scoring_duration_seconds",
    "End-to-end scoring latency",
    buckets=[.01, .025, .05, .1, .25, .5, 1.0],
)
ACTIVE_PROVS   = Gauge("stream_processor_active_providers", "Providers tracked in Redis")

# ── Procedure risk lookup (simplified; production uses a feature store) ──────────
PROCEDURE_RISK = {
    "27447": 0.90, "29827": 0.85, "27130": 0.88, "29881": 0.80,  # high-risk ortho
    "93306": 0.65, "99291": 0.70,                                  # medium-risk cardio
    "99213": 0.10, "99223": 0.25, "93000": 0.20,                  # routine
    "80053": 0.05, "85025": 0.05, "80061": 0.05, "82947": 0.05,   # lab
    "97110": 0.10, "97012": 0.10, "97140": 0.10, "97530": 0.10,   # physio
}

# ── Redis connection ─────────────────────────────────────────────────────────────

def connect_redis() -> redis.Redis:
    for attempt in range(20):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            log.info("Redis connected")
            return r
        except Exception as e:
            log.info(f"Waiting for Redis ({attempt + 1}/20): {e}")
            time.sleep(2)
    raise RuntimeError("Redis unavailable")

# ── State management (mimics Flink RocksDB keyed state) ─────────────────────────

def get_provider_stats(r: redis.Redis, npi: str, amount: float) -> dict:
    """
    Rolling provider stats — in Flink this is a keyed state with a 30-day
    sliding event-time window. Here we use Redis hashes with 30-day TTL.
    """
    key = f"ps:{npi}"
    pipe = r.pipeline()
    pipe.hincrbyfloat(key, "sum",   amount)
    pipe.hincrby(key,      "count", 1)
    pipe.expire(key, 30 * 86400)
    pipe.hgetall(key)
    result = pipe.execute()
    stats  = result[3]

    count = int(stats.get("count", 1))
    avg   = float(stats.get("sum",   amount)) / count

    ACTIVE_PROVS.set(len(r.keys("ps:*")))

    return {"claim_count_30d": count, "avg_amount_30d": round(avg, 2)}

def get_member_stats(r: redis.Redis, member_id: str) -> dict:
    key = f"ms:{member_id}"
    r.hincrby(key, "count", 1)
    r.expire(key, 90 * 86400)
    count = int(r.hget(key, "count") or 1)
    return {"member_claim_count_90d": count}

# ── Rule-based scoring (mimics Flink RuleBasedScoringJob) ───────────────────────

def apply_rules(claim: dict, pstats: dict) -> tuple:
    score    = 0.0
    triggers = []
    avg      = pstats["avg_amount_30d"]

    # R1: Amount > 3× provider 30-day average
    if avg > 0 and claim["amount"] > avg * 3.0:
        score = max(score, 0.78)
        triggers.append("AMOUNT_3X_PROVIDER_AVG")

    # R2: Extreme absolute amount (> R$50 000)
    if claim["amount"] > 50_000:
        score = max(score, 0.88)
        triggers.append("EXTREME_AMOUNT")

    # R3: Orthopedic procedure with cardiovascular diagnosis
    ortho_procs  = {"27447", "29827", "29881", "27130"}
    cardio_diags = {"I10", "I25.10", "I48.0", "I50.9"}
    if (claim.get("procedure_code") in ortho_procs and
            claim.get("diagnosis_code") in cardio_diags):
        score = max(score, 0.93)
        triggers.append("PROCEDURE_DIAGNOSIS_MISMATCH")

    # R4: Phantom billing — very high volume provider (> 200 claims/30d)
    if pstats["claim_count_30d"] > 200:
        score = max(score, 0.62)
        triggers.append("HIGH_VOLUME_PROVIDER")

    return round(score, 4), triggers

# ── ML scoring (mimics Flink MLScoringJob calling BentoML) ──────────────────────

def call_ml_scorer(claim: dict, pstats: dict, mstats: dict) -> float:
    now  = datetime.now()
    body = {
        "claim_id":               claim["claim_id"],
        "amount":                 claim["amount"],
        "claim_count_30d":        pstats["claim_count_30d"],
        "avg_amount_30d":         pstats["avg_amount_30d"],
        "hour_of_day":            now.hour,
        "is_weekend":             int(now.weekday() >= 5),
        "procedure_risk":         PROCEDURE_RISK.get(claim.get("procedure_code", ""), 0.30),
        "member_claim_count_90d": mstats["member_claim_count_90d"],
    }
    try:
        resp = requests.post(f"{FRAUD_SCORER_URL}/score", json=body, timeout=2.0)
        resp.raise_for_status()
        return resp.json()["fraud_probability"]
    except Exception as e:
        log.warning(f"ML scorer unavailable ({e}), using rule score only")
        return 0.0

# ── Core processing pipeline ─────────────────────────────────────────────────────

def process(claim: dict, r: redis.Redis, producer: Producer):
    t0 = time.time()

    # Step 1 — Enrich
    pstats = get_provider_stats(r, claim["provider_npi"], claim["amount"])
    mstats = get_member_stats(r,   claim["member_id"])

    # Step 2 — Rule score
    rule_score, triggers = apply_rules(claim, pstats)

    # Step 3 — ML score
    ml_score = call_ml_scorer(claim, pstats, mstats)

    # Step 4 — Combine (weighted blend favouring rules when they fire)
    final = round(max(rule_score, ml_score * 0.6 + rule_score * 0.4), 4)

    risk = (
        "CRITICAL" if final >= 0.90 else
        "HIGH"     if final >= 0.70 else
        "MEDIUM"   if final >= 0.40 else
        "LOW"
    )

    scored = {
        **claim,
        "fraud_score":     final,
        "rule_score":      rule_score,
        "ml_score":        ml_score,
        "risk_level":      risk,
        "triggered_rules": triggers,
        "provider_stats":  pstats,
        "scored_at":       datetime.now().isoformat(),
    }

    # Step 5 — Publish to scored.claims (always)
    producer.produce(
        topic="scored.claims",
        key=claim["claim_id"].encode(),
        value=json.dumps(scored).encode(),
    )

    # Step 6 — Publish alert if above threshold
    if final >= FRAUD_THRESHOLD:
        producer.produce(
            topic="alerts.fraud",
            key=claim["claim_id"].encode(),
            value=json.dumps(scored).encode(),
        )
        ALERTS_TOTAL.labels(risk_level=risk).inc()
        log.warning(
            f"FRAUD [{risk:8s}] {claim['claim_id']}  "
            f"provider={claim['provider_npi']}  "
            f"amount=R${claim['amount']:>10,.2f}  "
            f"score={final}  rules={triggers}"
        )

    producer.poll(0)

    result_label = "fraud" if final >= FRAUD_THRESHOLD else "legitimate"
    CLAIMS_TOTAL.labels(result=result_label).inc()
    SCORE_DURATION.observe(time.time() - t0)

# ── Kafka setup ──────────────────────────────────────────────────────────────────

def make_consumer() -> Consumer:
    for attempt in range(20):
        try:
            c = Consumer({
                "bootstrap.servers":  KAFKA_BROKERS,
                "group.id":           "stream-processor-demo",
                "auto.offset.reset":  "earliest",
                "enable.auto.commit": True,
            })
            c.subscribe(["raw.claims.new"])
            log.info("Kafka consumer ready — subscribed to raw.claims.new")
            return c
        except Exception as e:
            log.info(f"Waiting for Kafka ({attempt + 1}/20): {e}")
            time.sleep(3)
    raise RuntimeError("Kafka unavailable")

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BROKERS, "acks": "1"})

# ── Main ─────────────────────────────────────────────────────────────────────────

def main():
    log.info("Stream processor starting...")
    start_http_server(METRICS_PORT)
    log.info(f"Prometheus metrics → http://0.0.0.0:{METRICS_PORT}/metrics")

    r        = connect_redis()
    consumer = make_consumer()
    producer = make_producer()

    log.info("Processing claims from raw.claims.new ...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"Kafka error: {msg.error()}")
                continue
            try:
                claim = json.loads(msg.value().decode("utf-8"))
                process(claim, r, producer)
            except Exception as e:
                log.error(f"Processing error: {e}  raw={msg.value()[:200]}")
                CLAIMS_TOTAL.labels(result="error").inc()
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
