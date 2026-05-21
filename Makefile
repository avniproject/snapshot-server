PORT   ?= 3000
mode   ?= normal

.PHONY: help deps start stats request
.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  make deps                                          Install npm deps under Node 20"
	@echo "  make start                                         Start snapshot-server (foreground)"
	@echo "  make stats                                         Print per-request snapshot stats"
	@echo "  make request dbUser=<dbUser> [mode=normal|clean]   Enqueue a snapshot run"
	@echo ""
	@echo "Override the HTTP port if needed:  make request dbUser=foo PORT=8080"

deps:
	npm install

start:
	npm start

stats:
	npm run stats

request:
	@if [ -z "$(dbUser)" ]; then \
		echo "Usage: make request dbUser=<dbUser> [mode=normal|clean]"; \
		exit 1; \
	fi
	@curl -sS -X POST http://localhost:$(PORT)/requests \
		-H 'Content-Type: application/json' \
		-d '{"dbUser":"$(dbUser)","mode":"$(mode)","requestedBy":"make"}'
	@echo
