.PHONY: up down logs producer consumer reset status

# Start all services
up:
	docker-compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "  Kafka UI:  http://localhost:8090"
	@echo "  Postgres:  localhost:5432  (fraud_db / fraud_user / fraud_pass)"
	@echo ""
	@echo "Run 'make logs' to watch the stream."

# Stop everything
down:
	docker-compose down

# Wipe volumes and start fresh
reset:
	docker-compose down -v
	docker-compose up -d

# Watch all logs
logs:
	docker-compose logs -f producer consumer

# Watch producer only
producer:
	docker-compose logs -f producer

# Watch consumer (fraud alerts) only
consumer:
	docker-compose logs -f consumer

# Show running containers and stats
status:
	docker-compose ps
	@echo ""
	@docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

# Run analysis notebook (requires local Python env)
notebook:
	pip install -r requirements.txt
	jupyter notebook notebooks/fraud_analysis.py

# Quick DB check — count alerts
db-check:
	docker exec -it fraud-postgres psql -U fraud_user -d fraud_db -c \
		"SELECT final_rule, COUNT(*) FROM fraud_alerts GROUP BY final_rule ORDER BY 2 DESC;"

# Install local dev dependencies (for running outside Docker)
install:
	pip install -r requirements.txt
