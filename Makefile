export AWS_DEFAULT_REGION=eu-west-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

AWS_ENDPOINT ?= http://localhost:4566

.PHONY: mod
mod:
	GO111MODULE=on go mod tidy

build:
	CGO_ENABLED=0 GOOS=linux go build -v ./...

test:
	go test -v ./...

cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: setup
setup:
	docker-compose up -d
	./scripts/create-dynamodb-table.sh $(AWS_ENDPOINT)

.PHONY: down
down:
	docker-compose down --remove-orphans