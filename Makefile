.PHONY: up down test cover it lint bench demo

SHELL := /bin/sh
MIN_COVERAGE := 90

up:
	docker compose up --wait localstack
	docker compose run --rm init

down:
	docker compose down -v

test:
	go test -race -cover ./...

cover:
	@pkgs=$$(go list ./internal/domain/... ./internal/app/... 2>/dev/null); \
	if [ -z "$$pkgs" ]; then echo "No packages to check"; exit 0; fi; \
	go test -race -coverprofile=coverage.out $$pkgs && \
	total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	echo "Total domain/app coverage: $${total}% (min: $(MIN_COVERAGE)%)"; \
	awk -v cov="$$total" -v min="$(MIN_COVERAGE)" 'BEGIN { if (cov + 0 < min + 0) { exit 1 } }'

it:
	go test -tags=integration -race -v ./...

lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "Unformatted files:"; echo "$$unformatted"; exit 1; fi
	go vet -tags=integration ./...
	golangci-lint run --build-tags=integration ./...
	staticcheck -tags=integration ./...
	govulncheck ./...

bench:
	go test -bench=BenchmarkAssign1M -benchmem -run='^$$' ./internal/domain/...

demo:
	@echo "make demo will be enabled once mockapi, dispatcher, worker, and seed are wired in M1/M2 (006-T004)."
	@exit 1
