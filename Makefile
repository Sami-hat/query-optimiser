.PHONY: help build up down restart logs shell db-shell setup-test demo-reset clean health status

help:
	@echo "PostgreSQL Query Optimizer - Docker Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@echo "  build        Build Docker images"
	@echo "  up           Start all services"
	@echo "  down         Stop all services"
	@echo "  restart      Restart all services"
	@echo "  logs         View logs (all services)"
	@echo "  logs-app     View application logs"
	@echo "  logs-db      View database logs"
	@echo "  shell        Access application shell"
	@echo "  db-shell     Access PostgreSQL shell"
	@echo "  setup-test   Set up test database (destructive: drops demo tables)"
	@echo "  demo-reset   Drop indexes created during a demo (makes 'before' slow again)"
	@echo "  health       Check health of all services"
	@echo "  status       Show status of all services"
	@echo "  clean        Stop services and remove volumes"
	@echo "  clean-all    Remove everything including images"
	@echo ""

# Detect docker compose version (v2 uses space, v1 uses hyphen)
DOCKER_COMPOSE := $(shell docker compose version > /dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d
	@echo "Services started. Access:"
	@echo "  Frontend:  http://localhost"
	@echo "  API:       http://localhost:8000"
	@echo "  API Docs:  http://localhost:8000/docs"
	@echo "  Database:  localhost:5433"

down:
	$(DOCKER_COMPOSE) down

restart:
	$(DOCKER_COMPOSE) restart

logs:
	$(DOCKER_COMPOSE) logs -f

logs-app:
	$(DOCKER_COMPOSE) logs -f app

logs-db:
	$(DOCKER_COMPOSE) logs -f postgres

shell:
	$(DOCKER_COMPOSE) exec app bash

db-shell:
	$(DOCKER_COMPOSE) exec postgres psql -U postgres -d pg_analyser

setup-test:
	@echo "Setting up test database..."
	$(DOCKER_COMPOSE) exec app python3 scripts/setup_test_db.py

# Drops every index the demo may have created, keeping the ones setup_test_db.py
# seeds (primary keys, unique constraints, and the foreign-key indexes). Run this
# before a demo so the "before" query is slow again.
demo-reset:
	@$(DOCKER_COMPOSE) exec -T postgres psql -U $${DB_USER:-postgres} -d $${DB_NAME:-pg_analyser} -q -c "\
	DO \$$\$$ \
	DECLARE r RECORD; \
	BEGIN \
	  FOR r IN \
	    SELECT indexname FROM pg_indexes \
	    WHERE schemaname = 'public' \
	      AND indexname NOT IN ( \
	        'idx_products_category', 'idx_orders_user', 'idx_order_items_order', \
	        'idx_order_items_product', 'idx_reviews_product', 'idx_reviews_user') \
	      AND indexname NOT IN ( \
	        SELECT conname FROM pg_constraint WHERE contype IN ('p','u')) \
	  LOOP \
	    EXECUTE format('DROP INDEX IF EXISTS public.%I', r.indexname); \
	    RAISE NOTICE 'dropped %', r.indexname; \
	  END LOOP; \
	END \$$\$$;"
	@echo "Demo indexes dropped - 'before' queries are slow again."

health:
	@echo "Checking service health..."
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "API Health:"
	@curl -s http://localhost:8000/health | python3 -m json.tool || echo "API not responding"
	@echo ""
	@echo "Frontend:"
	@curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" http://localhost/
	@echo ""
	@echo "Database:"
	@$(DOCKER_COMPOSE) exec postgres pg_isready -U postgres

status:
	$(DOCKER_COMPOSE) ps

clean:
	$(DOCKER_COMPOSE) down -v
	@echo "All services stopped and volumes removed"

clean-all:
	$(DOCKER_COMPOSE) down -v --rmi all
	@echo "Everything removed (containers, volumes, images)"

rebuild:
	$(DOCKER_COMPOSE) down
	$(DOCKER_COMPOSE) build --no-cache
	$(DOCKER_COMPOSE) up -d

backup-db:
	@echo "Creating database backup..."
	@mkdir -p backups
	$(DOCKER_COMPOSE) exec postgres pg_dump -U postgres pg_analyser | gzip > backups/pg_analyser_$$(date +%Y%m%d_%H%M%S).sql.gz
	@echo "Backup created in backups/"

restore-db:
	@echo "Restoring database from backup..."
	@echo "Available backups:"
	@ls -1 backups/*.sql.gz
	@read -p "Enter backup filename: " backup; \
	gunzip -c $$backup | $(DOCKER_COMPOSE) exec -T postgres psql -U postgres pg_analyser
