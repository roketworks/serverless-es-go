mod:
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor

build:
	go build -v ./eventstore
	go build -v ./cmd/sns-to-dynamodb
	go build -v ./cmd/dynamodb-stream-lambda

test:
	go test -v ./eventstore