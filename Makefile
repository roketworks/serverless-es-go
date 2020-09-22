AWS_ENDPOINT ?= http://localhost:4566

.PHONY: mod
mod:
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor

build:
	CGO_ENABLED=0 GOOS=linux go build -v ./...

test:
	go test -v ./...

cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic .

.PHONY: setup
setup:
	docker-compose up -d
	./scripts/create-dynamodb-table.sh $(AWS_ENDPOINT)

.PHONY: down
down:
	docker-compose down --remove-orphans