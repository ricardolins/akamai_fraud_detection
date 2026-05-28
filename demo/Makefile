.PHONY: start stop restart logs status clean

start:
	docker compose up --build -d
	@echo ""
	@echo "Starting up... (takes ~60s on first run while images build)"
	@echo ""
	@echo "  Redpanda Console  →  http://localhost:8080"
	@echo "  Grafana           →  http://localhost:3000   (admin/admin)"
	@echo "  Fraud Scorer API  →  http://localhost:8000/docs"
	@echo "  MLflow            →  http://localhost:5000"
	@echo "  Prometheus        →  http://localhost:9090"
	@echo ""

stop:
	docker compose down

restart: stop start

logs:
	docker compose logs -f stream-processor data-generator fraud-scorer

logs-alerts:
	docker compose logs -f stream-processor | grep "FRAUD"

status:
	docker compose ps

clean:
	docker compose down -v --rmi local
	@echo "All containers, volumes and local images removed."
