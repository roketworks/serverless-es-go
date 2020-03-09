mod:
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor

build:
	go build -v ./eventstore
	go build -v ./cmd/sns-to-dynamodb
	go build -v ./cmd/dynamodb-stream-lambda

test:
	go test -v ./eventstore

create-test-tables:
	aws dynamodb create-table --region eu-west-1 --endpoint-url $(dynamodb_endpoint) --table-name sequences --billing-mode PAY_PER_REQUEST \
		--attribute-definitions AttributeName=sequence,AttributeType=S \
		--key-schema AttributeName=sequence,KeyType=HASH

	aws dynamodb create-table --region eu-west-1 --endpoint-url $(dynamodb_endpoint) --table-name eventstore --billing-mode PAY_PER_REQUEST \
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