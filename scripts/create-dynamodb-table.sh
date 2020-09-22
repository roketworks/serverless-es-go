#! /bin/bash

aws dynamodb create-table --endpoint-url $1 --table-name eventstore --billing-mode PAY_PER_REQUEST \
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