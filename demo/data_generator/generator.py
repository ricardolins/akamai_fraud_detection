#!/usr/bin/env python3
"""
Healthcare Claims Data Generator — Demo Component

Simulates a claims adjudication system by:
  1. Inserting claims into PostgreSQL (source of truth)
  2. Publishing claims directly to Redpanda raw.claims.new (direct ingestion path)

In production: only step 1 happens here; Debezium handles CDC to Redpanda.
For demo reliability: we also publish directly so the demo works even if
Debezium CDC setup has timing issues during docker-compose startup.
"""
import os, time, uuid, json, random, logging
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BROKERS     = os.getenv("KAFKA_BROKERS",     "localhost:19092")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "claims_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "fraud_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_pass")
CLAIMS_PER_SEC    = float(os.getenv("CLAIMS_PER_SECOND", "2"))
FRAUD_RATE        = float(os.getenv("FRAUD_RATE",        "0.08"))

# ── Provider catalog ────────────────────────────────────────────────────────────
# NPI-005 is marked as fraudulent — it will generate suspicious claims
PROVIDERS = [
    {"npi": "NPI-001", "name": "Cardio Clinic SP",   "specialty": "CARDIOLOGY",    "avg": 850.0,  "fraud": False},
    {"npi": "NPI-002", "name": "Ortho Hospital",     "specialty": "ORTHOPEDICS",   "avg": 2200.0, "fraud": False},
    {"npi": "NPI-003", "name": "MedLab Diagnostics", "specialty": "LABORATORY",    "avg": 180.0,  "fraud": False},
    {"npi": "NPI-004", "name": "PhysioPlus",         "specialty": "PHYSIOTHERAPY", "avg": 320.0,  "fraud": False},
    {"npi": "NPI-005", "name": "Suspect Clinic",     "specialty": "CARDIOLOGY",    "avg": 850.0,  "fraud": True},
]

PROCEDURES = {
    "CARDIOLOGY":    [("99213", 180), ("93000", 350), ("93306", 1200), ("99223", 450)],
    "ORTHOPEDICS":   [("99213", 180), ("27447", 8500), ("29827", 3200), ("99291", 650)],
    "LABORATORY":    [("80053", 45),  ("85025", 30),   ("80061", 55),  ("82947", 25)],
    "PHYSIOTHERAPY": [("97110", 95),  ("97012", 85),   ("97140", 110), ("97530", 105)],
}

DIAGNOSES = {
    "CARDIOLOGY":    ["I10", "I25.10", "I48.0", "Z82.49"],
    "ORTHOPEDICS":   ["M17.11", "M75.1", "S72.001A", "M54.5"],
    "LABORATORY":    ["Z00.00", "E11.9", "I10", "Z13.6"],
    "PHYSIOTHERAPY": ["M54.5", "M75.1", "G89.29", "S93.401A"],
}

# ── Claim builders ──────────────────────────────────────────────────────────────

def make_legitimate_claim(provider: dict) -> dict:
    spec        = provider["specialty"]
    code, base  = random.choice(PROCEDURES[spec])
    return {
        "claim_id":       f"CLM-{uuid.uuid4().hex[:8].upper()}",
        "provider_npi":   provider["npi"],
        "provider_name":  provider["name"],
        "member_id":      f"MBR-{random.randint(1000, 9999):04d}",
        "procedure_code": code,
        "diagnosis_code": random.choice(DIAGNOSES[spec]),
        "amount":         round(base * random.uniform(0.85, 1.15), 2),
        "service_date":   (datetime.now() - timedelta(days=random.randint(0, 3))).strftime("%Y-%m-%d"),
        "submitted_at":   datetime.now().isoformat(),
        "status":         "PENDING",
        "_is_fraud":      False,
    }

def make_fraudulent_claim(provider: dict) -> dict:
    claim = make_legitimate_claim(provider)
    fraud_type = random.choice(["upcoding", "excess_amount", "diagnosis_mismatch"])

    if fraud_type == "upcoding":
        # Bill expensive orthopedic surgery for a cardiology visit
        claim["procedure_code"] = "27447"
        claim["amount"]         = round(random.uniform(7000, 12000), 2)

    elif fraud_type == "excess_amount":
        claim["amount"] = round(provider["avg"] * random.uniform(5.0, 10.0), 2)

    elif fraud_type == "diagnosis_mismatch":
        # Orthopedic procedure with cardiovascular diagnosis
        claim["procedure_code"] = "29827"
        claim["diagnosis_code"] = "I10"
        claim["amount"]         = round(random.uniform(3000, 8000), 2)

    claim["_is_fraud"] = True
    return claim

# ── Persistence ─────────────────────────────────────────────────────────────────

def connect_postgres() -> psycopg2.extensions.connection:
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                dbname=POSTGRES_DB, user=POSTGRES_USER,
                password=POSTGRES_PASSWORD, connect_timeout=5,
            )
            log.info("PostgreSQL connected")
            return conn
        except psycopg2.OperationalError:
            log.info(f"Waiting for PostgreSQL ({attempt + 1}/30)...")
            time.sleep(2)
    raise RuntimeError("PostgreSQL unavailable after 30 attempts")

def insert_claim(conn, claim: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO claims (
                claim_id, provider_npi, member_id, procedure_code,
                diagnosis_code, amount, service_date, submitted_at, status
            ) VALUES (
                %(claim_id)s, %(provider_npi)s, %(member_id)s, %(procedure_code)s,
                %(diagnosis_code)s, %(amount)s, %(service_date)s, %(submitted_at)s, %(status)s
            ) ON CONFLICT (claim_id) DO NOTHING
        """, claim)
    conn.commit()

def connect_redpanda() -> Producer:
    for attempt in range(20):
        try:
            p = Producer({"bootstrap.servers": KAFKA_BROKERS, "acks": "1"})
            p.list_topics(timeout=5)
            log.info("Redpanda connected")
            return p
        except Exception as e:
            log.info(f"Waiting for Redpanda ({attempt + 1}/20): {e}")
            time.sleep(3)
    raise RuntimeError("Redpanda unavailable after 20 attempts")

# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    log.info(f"Generator starting — {CLAIMS_PER_SEC} claims/s, {FRAUD_RATE*100:.0f}% fraud rate")

    pg       = connect_postgres()
    producer = connect_redpanda()
    interval = 1.0 / CLAIMS_PER_SEC
    total    = 0
    frauds   = 0

    while True:
        t0       = time.time()
        provider = random.choice(PROVIDERS)
        is_fraud = random.random() < FRAUD_RATE or provider["fraud"]
        claim    = make_fraudulent_claim(provider) if is_fraud else make_legitimate_claim(provider)

        try:
            insert_claim(pg, claim)
            producer.produce(
                topic="raw.claims.new",
                key=claim["provider_npi"].encode(),
                value=json.dumps({k: v for k, v in claim.items() if not k.startswith("_")}).encode(),
            )
            producer.poll(0)

            total  += 1
            frauds += int(is_fraud)

            if total % 10 == 0:
                flag = "*** FRAUD ***" if is_fraud else ""
                log.info(
                    f"[{total:5d}] {claim['claim_id']}  provider={claim['provider_npi']}"
                    f"  amount=R${claim['amount']:>10,.2f}  {flag}"
                )
            if total % 50 == 0:
                log.info(f"--- Summary: {total} claims | {frauds} fraud ({frauds/total*100:.1f}%) ---")

        except Exception as e:
            log.error(f"Error on claim {claim.get('claim_id')}: {e}")
            try:
                pg = connect_postgres()
            except Exception:
                pass

        elapsed = time.time() - t0
        time.sleep(max(0, interval - elapsed))

if __name__ == "__main__":
    main()
