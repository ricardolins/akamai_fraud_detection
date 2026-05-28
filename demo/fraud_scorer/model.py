"""
Fraud Detection Model — trains on startup using synthetic data.

Features (8):
  0  amount                  — claim amount in BRL
  1  claim_count_30d         — how many claims this provider submitted in 30 days
  2  avg_amount_30d          — provider's average claim amount over 30 days
  3  amount_ratio            — amount / avg_amount_30d (deviation from provider norm)
  4  hour_of_day             — submission hour (0-23)
  5  is_weekend              — 1 if Saturday or Sunday
  6  procedure_risk          — risk score of procedure code (0.0–1.0)
  7  member_claim_count_90d  — how many claims this member had in 90 days

In production: model is trained on Spark over Iceberg gold-zone features,
managed in MLflow, and served via BentoML. Here we train on synthetic data
to keep the demo self-contained.
"""
import os
import pickle
import logging
import numpy as np

log = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "/app/model.pkl")


def _generate_synthetic_data():
    np.random.seed(42)
    n_legit, n_fraud = 9500, 500

    legit = np.column_stack([
        np.random.lognormal(6.5,  0.5,  n_legit),            # amount ~R$650
        np.random.poisson(15,           n_legit),             # claim_count_30d
        np.random.lognormal(6.3,  0.3,  n_legit),            # avg_amount_30d
        np.random.normal(1.0,     0.15, n_legit).clip(0.5, 2.0),  # amount_ratio
        np.random.randint(8,      19,   n_legit),             # business hours
        np.random.binomial(1,     0.28, n_legit),             # 28% weekends
        np.random.beta(2,         8,    n_legit),             # low procedure risk
        np.random.poisson(8,            n_legit),             # member history
    ])

    fraud = np.column_stack([
        np.random.lognormal(9.0,  0.7,  n_fraud),            # much higher amounts
        np.random.poisson(120,          n_fraud),             # very high volume
        np.random.lognormal(6.3,  0.3,  n_fraud),            # avg similar (camouflage)
        np.random.uniform(4.0,    12.0, n_fraud),             # high deviation ratio
        np.random.choice([2, 3, 4, 22, 23], n_fraud),        # odd hours
        np.random.binomial(1,     0.65, n_fraud),             # more weekends
        np.random.beta(7,         2,    n_fraud),             # high-risk procedures
        np.random.poisson(1,            n_fraud),             # fake/new members
    ])

    X = np.vstack([legit, fraud])
    y = np.array([0] * n_legit + [1] * n_fraud)
    perm = np.random.permutation(len(y))
    return X[perm], y[perm]


def train_model():
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, average_precision_score

    log.info("Training fraud model on synthetic data...")
    X, y = _generate_synthetic_data()
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    model = GradientBoostingClassifier(
        n_estimators=150, max_depth=4, learning_rate=0.08,
        subsample=0.8, random_state=42,
    )
    model.fit(X_tr, y_tr)

    probs = model.predict_proba(X_te)[:, 1]
    log.info(
        f"Model trained — AUC-ROC: {roc_auc_score(y_te, probs):.4f}  "
        f"AP: {average_precision_score(y_te, probs):.4f}"
    )

    os.makedirs(os.path.dirname(MODEL_PATH) or ".", exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    return model


def load_or_train():
    if os.path.exists(MODEL_PATH):
        log.info(f"Loading model from {MODEL_PATH}")
        with open(MODEL_PATH, "rb") as f:
            return pickle.load(f)
    return train_model()
