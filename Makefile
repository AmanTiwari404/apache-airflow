# ============================================================================
#  Convenience shortcuts. Run `make <target>`.
#  (Recipe lines below are indented with a TAB, as Make requires.)
# ============================================================================

# The DAG we trigger via `make trigger`.
DAG_ID := learning_pipeline

.PHONY: help start stop logs trigger status restart clean

help:            ## Show this help
	@echo "Available commands:"
	@echo "  make start    - start the whole Airflow stack (detached)"
	@echo "  make stop     - stop and remove the containers"
	@echo "  make logs     - follow the scheduler logs"
	@echo "  make trigger  - trigger the '$(DAG_ID)' DAG via the Airflow CLI"
	@echo "  make status   - show the state of all containers"
	@echo "  make restart  - stop then start again"
	@echo "  make clean    - stop AND delete the metadata DB volume (full reset)"

start:           ## Start all services in the background
	docker compose up -d

stop:            ## Stop and remove all containers
	docker compose down

logs:            ## Follow the scheduler logs (Ctrl-C to exit)
	docker compose logs -f airflow-scheduler

trigger:         ## Unpause + trigger a run of the learning DAG
	docker compose exec airflow-scheduler airflow dags unpause $(DAG_ID)
	docker compose exec airflow-scheduler airflow dags trigger $(DAG_ID)

status:          ## Show container status
	docker compose ps

restart: stop start   ## Restart the whole stack

clean:           ## Full reset: remove containers AND the database volume
	docker compose down -v
