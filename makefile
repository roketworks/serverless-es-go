.PHONY: mod build test create-test-tables

DYNAMODB_ENDPOINT ?= http://localhost:8042
SQS_ENDPOINT ?= http://localhost:9324

mod:
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor

build:
	CGO_ENABLED=0 GOOS=linux go build -v ./eventstore
	CGO_ENABLED=0 GOOS=linux go build -v ./cmd/sns-to-dynamodb
	CGO_ENABLED=0 GOOS=linux go build -v ./cmd/dynamodb-stream-lambda

test:
	go test -v ./eventstore

create-local-tables:
	AWS_DEFAULT_REGION=eu-west-1 AWS_ACCESS_KEY_ID=fake_key AWS_SECRET_ACCESS_KEY=fake_secret \
		aws dynamodb create-table --endpoint-url $(DYNAMODB_ENDPOINT) --table-name sequences --billing-mode PAY_PER_REQUEST \
		--attribute-definitions AttributeName=sequence,AttributeType=S \
		--key-schema AttributeName=sequence,KeyType=HASH

	AWS_DEFAULT_REGION=eu-west-1 AWS_ACCESS_KEY_ID=fake_key AWS_SECRET_ACCESS_KEY=fake_secret \
		aws dynamodb create-table --endpoint-url $(DYNAMODB_ENDPOINT) --table-name eventstore --billing-mode PAY_PER_REQUEST \
		--attribute-definitions \
			AttributeName=streamId,AttributeType=S \
			AttributeName=version,AttributeType=N \
			AttributeName=position,AttributeType=N \
			AttributeName=active,AttributeType=N \
			AttributeName=committedAt,AttributeType=N \
		--key-schema AttributeName=streamId,KeyType=HASH AttributeName=version,KeyType=RANGE \
		--global-secondary-indexes \
			"IndexName=active-committedAt-index,KeySchema=[{AttributeName=active,KeyType=HASH},{AttributeName=committedAt,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
			"IndexName=active-position-index,KeySchema=[{AttributeName=active,KeyType=HASH},{AttributeName=position,KeyType=RANGE}],Projection={ProjectionType=ALL}"

create-local-queues:
	AWS_DEFAULT_REGION=eu-west-1 AWS_ACCESS_KEY_ID=fake_key AWS_SECRET_ACCESS_KEY=fake_secret \
		aws sqs create-queue --endpoint $(SQS_ENDPOINT) --queue-name projections.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true