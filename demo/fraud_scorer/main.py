"""
Fraud Scorer API — Demo Component

In production this role is played by BentoML serving an XGBoost/LightGBM model
registered in MLflow. Here we use a GradientBoostingClassifier trained on
synthetic data so the demo is fully self-contained.

Endpoints:
  POST /score     — score a single claim
  GET  /healthz   — liveness probe
  GET  /metrics   — Prometheus metrics
"""
import logging
import numpy as np
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from model import load_or_train

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Model lifecycle ─────────────────────────────────────────────────────────────
model = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global model
    model = load_or_train()
    log.info("Model ready — API accepting requests")
    yield

# ── App ─────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Fraud Scorer API", version="1.0.0", lifespan=lifespan)

# ── Prometheus metrics ──────────────────────────────────────────────────────────
INFERENCES = Counter(
    "fraud_scorer_inferences_total",
    "Total inference requests",
    ["risk_level"],
)
LATENCY = Histogram(
    "fraud_scorer_latency_seconds",
    "Inference latency in seconds",
    buckets=[.001, .005, .01, .025, .05, .1, .25],
)

# ── Schema ──────────────────────────────────────────────────────────────────────
class ScoreRequest(BaseModel):
    claim_id:               str
    amount:                 float
    claim_count_30d:        int
    avg_amount_30d:         float
    hour_of_day:            int
    is_weekend:             int
    procedure_risk:         float   # 0.0 – 1.0, from procedure risk table
    member_claim_count_90d: int

class ScoreResponse(BaseModel):
    claim_id:          str
    fraud_probability: float
    risk_level:        str          # LOW | MEDIUM | HIGH | CRITICAL
    model_version:     str = "gbm-synthetic-v1"

# ── Routes ──────────────────────────────────────────────────────────────────────

@app.post("/score", response_model=ScoreResponse)
def score(req: ScoreRequest):
    with LATENCY.time():
        features = np.array([[
            req.amount,
            req.claim_count_30d,
            req.avg_amount_30d,
            req.amount / max(req.avg_amount_30d, 1.0),   # deviation ratio
            req.hour_of_day,
            req.is_weekend,
            req.procedure_risk,
            req.member_claim_count_90d,
        ]])
        prob = float(model.predict_proba(features)[0][1])

    risk = (
        "CRITICAL" if prob >= 0.90 else
        "HIGH"     if prob >= 0.70 else
        "MEDIUM"   if prob >= 0.40 else
        "LOW"
    )
    INFERENCES.labels(risk_level=risk).inc()
    return ScoreResponse(
        claim_id=req.claim_id,
        fraud_probability=round(prob, 4),
        risk_level=risk,
    )

@app.get("/healthz")
def health():
    return {"status": "ok", "model": "loaded" if model else "not_loaded"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
