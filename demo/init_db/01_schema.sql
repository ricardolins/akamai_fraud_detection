-- Healthcare fraud detection demo — PostgreSQL schema
-- wal_level=logical is required for Debezium CDC (set in docker-compose command)

CREATE TABLE IF NOT EXISTS providers (
    npi         VARCHAR(20)  PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    specialty   VARCHAR(100) NOT NULL,
    license_no  VARCHAR(50),
    status      VARCHAR(20)  DEFAULT 'ACTIVE',
    created_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS members (
    member_id   VARCHAR(20) PRIMARY KEY,
    plan_id     VARCHAR(20),
    status      VARCHAR(20) DEFAULT 'ACTIVE',
    enrolled_at DATE        DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS claims (
    id              SERIAL      PRIMARY KEY,
    claim_id        VARCHAR(30) UNIQUE NOT NULL,
    provider_npi    VARCHAR(20) NOT NULL REFERENCES providers(npi),
    member_id       VARCHAR(20) NOT NULL,
    procedure_code  VARCHAR(10) NOT NULL,
    diagnosis_code  VARCHAR(15) NOT NULL,
    amount          NUMERIC(12,2) NOT NULL,
    service_date    DATE        NOT NULL,
    submitted_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    status          VARCHAR(20) DEFAULT 'PENDING',
    created_at      TIMESTAMP   DEFAULT NOW()
);

-- Required for Debezium FULL CDC (captures before/after on UPDATE and DELETE)
ALTER TABLE claims REPLICA IDENTITY FULL;

CREATE INDEX idx_claims_provider  ON claims(provider_npi);
CREATE INDEX idx_claims_member    ON claims(member_id);
CREATE INDEX idx_claims_submitted ON claims(submitted_at);
CREATE INDEX idx_claims_status    ON claims(status);
