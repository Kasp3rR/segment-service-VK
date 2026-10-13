.PHONY: build test check integration integration-kafka coverage up down migrate
build:
	go build -o bin/api ./cmd/api
	go build -o bin/migrate ./cmd/migrate
	go build -o bin/worker ./cmd/worker
test:
	go test -race ./...
check:
	test -z "$$(gofmt -l cmd internal integration)"
	go vet ./...
integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL to a real PostgreSQL instance'; exit 1)
	go test -race -tags=integration -count=1 ./integration
coverage:
	go test -race -coverprofile=coverage.out ./internal/service ./internal/distribution
	go tool cover -func=coverage.out
up:
	docker compose up -d --build --wait
down:
	docker compose down
migrate:
	go run ./cmd/migrate -direction=up

integration-kafka:
	@test -n "$(TEST_DATABASE_URL)" -a -n "$(TEST_KAFKA_BROKERS)" || (echo 'Set TEST_DATABASE_URL and TEST_KAFKA_BROKERS'; exit 1)
	go test -race -tags=integration,kafka -count=1 -timeout=300s ./integration
