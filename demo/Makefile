.PHONY: start stop restart logs logs-alerts logs-flink logs-bronze status clean etl-silver etl-gold etl creds

# ── Credenciais do Linode Object Storage (Terraform outputs) ──────────────────
# Execute 'make creds' uma vez antes de 'make start' para exportar as variáveis.
# Ou exporte manualmente:
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_ENDPOINT_URL_S3=https://br-gru-1.linodeobjects.com
#   export BUCKET=fraud-datalake

creds:
	$(eval export AWS_ACCESS_KEY_ID=$(shell cd ../infra/terraform && terraform output -raw object_storage_access_key))
	$(eval export AWS_SECRET_ACCESS_KEY=$(shell cd ../infra/terraform && terraform output -raw object_storage_secret_key))
	$(eval export AWS_ENDPOINT_URL_S3=$(shell cd ../infra/terraform && terraform output -raw object_storage_endpoint))
	$(eval export BUCKET=$(shell cd ../infra/terraform && terraform output -raw object_storage_bucket))
	@echo "✓ Linode Object Storage credentials exported"

start:
	docker compose up --build -d
	@echo ""
	@echo "Starting up... (~3 min na primeira vez — Flink e Spark images compilam do source)"
	@echo ""
	@echo "  Redpanda Console  →  http://localhost:8080"
	@echo "  Grafana           →  http://localhost:3000   (admin/admin)"
	@echo "  Fraud Scorer API  →  http://localhost:8000/docs"
	@echo "  MLflow            →  http://localhost:5000"
	@echo "  Prometheus        →  http://localhost:9090"
	@echo "  Nessie API        →  http://localhost:19120/api/v1/config"
	@echo ""

stop:
	docker compose down

restart: stop start

# ── Logs ──────────────────────────────────────────────────────────────────────

logs:
	docker compose logs -f flink-fraud-stream data-generator fraud-scorer

logs-alerts:
	docker compose logs -f flink-fraud-stream | grep -E "FRAUD|CRITICAL|HIGH"

logs-flink:
	docker compose logs -f flink-fraud-stream

logs-bronze:
	docker compose logs -f bronze-consumer

status:
	docker compose ps

# ── Spark Batch ETL ───────────────────────────────────────────────────────────
# Rode após o data-generator estar rodando por alguns minutos
# para que existam dados em bronze/ no Object Storage.

etl-silver:
	@echo "Rodando Spark silver ETL (bronze Object Storage → silver Iceberg via Nessie)..."
	docker compose --profile etl run --rm spark-silver-etl

etl-gold:
	@echo "Rodando Spark gold features (silver Iceberg → gold feature tables)..."
	docker compose --profile etl run --rm spark-gold-features

etl: etl-silver etl-gold

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	docker compose down -v --rmi local
	@echo "Containers, volumes e imagens locais removidos."
