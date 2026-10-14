# Lối tắt cho các bước thường dùng. Chạy `make help` để xem danh sách.
PY ?= python
export PYTHONPATH := src

.PHONY: help up up-all down ps logs test test-docker download batch official e2 live-status psql results

help:
	@grep -E '^[a-z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

up: ## Bật hạ tầng: Kafka, Postgres, Grafana
	docker compose up -d kafka kafka-init postgres grafana

up-all: ## Bật toàn bộ, gồm pipeline live (ws-producer, raw-archive, stream-candles)
	docker compose up -d --build

down: ## Tắt (giữ dữ liệu volume)
	docker compose down

ps: ## Trạng thái container
	docker compose ps

logs: ## Log pipeline live
	docker compose logs -f --tail=50 ws-producer stream-candles

test: ## Chạy test trên host (cần Python 3.11/3.12 + Java 17/21)
	$(PY) -m pytest

test-docker: ## Chạy test trong image Docker (không cần cài gì trên host)
	docker compose run --rm --no-deps -v $(PWD)/tests:/app/tests -v $(PWD)/pytest.ini:/app/pytest.ini \
		ws-producer python -m pytest

download: ## Tải aggTrades + klines theo config/settings.yaml
	$(PY) -m bdi.ingest.download

official: ## Nạp klines official vào parquet + Postgres
	$(PY) -m bdi.batch.load_klines

batch: ## Dựng nến batch từ aggTrades
	$(PY) -m bdi.batch.build_batch

e2: ## Đối soát batch vs official + vẽ hình E2
	$(PY) -m bdi.reconcile.compare --left batch --right official --run-id e2
	$(PY) experiments/e2_batch_vs_official.py --run-id e2

live-status: ## Kiểm tra nhanh luồng live có đang ra nến không
	docker compose exec -T postgres psql -U bdi -d bdi -c \
	"SELECT source, symbol, max(minute) AS last_minute, count(*) FROM candles WHERE source='stream' GROUP BY 1,2 ORDER BY 2"

psql: ## Mở psql
	docker compose exec postgres psql -U bdi -d bdi

results: ## Nạp results/*.csv vào Postgres + dựng lại dashboard "Kết quả thí nghiệm"
	$(PY) experiments/publish_results.py
	$(PY) grafana/build_results_dashboard.py
