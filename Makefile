BINARY ?= target/release/amberio
REDIS_URL ?= redis://127.0.0.1:6379
NODES ?= 3
MIN_WRITE_REPLICAS ?= 2
TOTAL_SLOTS ?= 2048
TLA2TOOLS_JAR ?=

.PHONY: integration tla

integration:
	uv run --project integration integration/run_all.py \
		--binary $(BINARY) \
		--redis-url $(REDIS_URL) \
		--nodes $(NODES) \
		--min-write-replicas $(MIN_WRITE_REPLICAS) \
		--total-slots $(TOTAL_SLOTS) \
		--build-if-missing

tla:
	@if [ -z "$(TLA2TOOLS_JAR)" ]; then \
		echo "TLA2TOOLS_JAR is required. Example:"; \
		echo "  make tla TLA2TOOLS_JAR=/path/to/tla2tools.jar"; \
		exit 1; \
	fi
	uv run --project integration integration/008_tla_trace_check.py \
		--binary $(BINARY) \
		--redis-url $(REDIS_URL) \
		--nodes $(NODES) \
		--min-write-replicas $(MIN_WRITE_REPLICAS) \
		--total-slots $(TOTAL_SLOTS) \
		--build-if-missing \
		--tlc-jar $(TLA2TOOLS_JAR)
