.PHONY: help deps start stats
.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  make deps   Install npm deps under Node 20"
	@echo "  make start  Start snapshot-server (foreground)"
	@echo "  make stats  Print per-request snapshot stats"
	@echo ""
	@echo "Snapshot runs are scheduler-driven. To trigger generation for an"
	@echo "org, toggle OrganisationConfig.enableSqliteSnapshotGeneration in"
	@echo "the avni-webapp admin UI; snapshot-server picks it up on its next"
	@echo "tick (default 1h, see SCHEDULER_TICK_INTERVAL_MS)."

deps:
	npm install

start:
	npm start

stats:
	npm run stats
