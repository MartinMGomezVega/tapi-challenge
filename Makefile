.PHONY: up down test test-full cover it it-full lint bench demo

SHELL := /bin/sh
MIN_COVERAGE := 90

up:
	docker compose up --wait localstack
	docker compose run --rm init

down:
	docker compose down -v

test:
	go test -short -race -cover ./...

test-full:
	go test -race ./...

cover:
	@pkgs=$$(go list ./internal/domain/... ./internal/app/... 2>/dev/null); \
	if [ -z "$$pkgs" ]; then echo "No packages to check"; exit 0; fi; \
	go test -short -race -coverprofile=coverage.out $$pkgs && \
	total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	echo "Total domain/app coverage: $${total}% (min: $(MIN_COVERAGE)%)"; \
	awk -v cov="$$total" -v min="$(MIN_COVERAGE)" 'BEGIN { if (cov + 0 < min + 0) { exit 1 } }'

it:
	go test -tags=integration -short -race -v ./...

it-full:
	go test -tags=integration -race -v -timeout 15m ./...

lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "Unformatted files:"; echo "$$unformatted"; exit 1; fi
	go vet -tags=integration ./...
	golangci-lint run --build-tags=integration ./...
	staticcheck -tags=integration ./...
	govulncheck ./...

bench:
	go test -bench='Benchmark(Assign1M|StreamingBackfill1M|StreamingBackfill4M)' -benchmem -run='^$$' ./internal/domain/...

demo:
	chmod +x scripts/demo.sh
	./scripts/demo.sh

tf-validate:
	terraform fmt -check -recursive deploy/terraform
	for dir in deploy/terraform/modules/data deploy/terraform/modules/messaging deploy/terraform/stack deploy/terraform/envs/dev deploy/terraform/envs/staging deploy/terraform/envs/prod; do \
		terraform -chdir="$$dir" init -backend=false -input=false && terraform -chdir="$$dir" validate || exit 1; \
	done
	terraform -chdir=deploy/terraform/modules/data test
	terraform -chdir=deploy/terraform/modules/messaging test
	terraform -chdir=deploy/terraform/stack test
