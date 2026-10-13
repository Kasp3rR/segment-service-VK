.PHONY: build test check integration coverage up down migrate
build:
	go build -o bin/api ./cmd/api
	go build -o bin/migrate ./cmd/migrate
test:
	go test -race ./...
check:
	test -z "$$(gofmt -l cmd internal integration)"
	go vet ./...
integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL to a real PostgreSQL instance'; exit 1)
	go test -race -tags=integration -count=1 ./integration
coverage:
	go test -race -coverprofile=coverage.out ./internal/service
	go tool cover -func=coverage.out
up:
	docker compose up -d --build --wait
down:
	docker compose down
migrate:
	go run ./cmd/migrate -direction=up
