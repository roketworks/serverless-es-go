package eventstore

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSave(t *testing.T) {
	awsSession := session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-west-1")}))
	dynamoSvc := dynamodb.New(awsSession)

	es := &EventStore{Db: dynamoSvc, TableName: "es-test-stack-EventStoreTable-ONSDU4YWZ62N"}
	bytes := []byte("string")

	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	err := Save(es, streamId, 1, bytes)
	assert.Nil(t, err)

	err = Save(es, streamId, 2, bytes)
	assert.Nil(t, err)

	err = Save(es, streamId, 3, bytes)
	assert.Nil(t, err)

	err = Save(es, streamId, 4, bytes)
	assert.Nil(t, err)
}

func TestGetByStreamId(t *testing.T) {
	awsSession := session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-west-1")}))
	dynamoSvc := dynamodb.New(awsSession)
	es := &EventStore{Db: dynamoSvc, TableName: "es-test-stack-EventStoreTable-ONSDU4YWZ62N"}

	streamId := "stream-aef89dfa-9174-4a28-8d19-69c8fe401bc5"

	GetByStreamId(es, &QueryParams{StreamId: streamId, Version: 1})
}
