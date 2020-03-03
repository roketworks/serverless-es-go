package eventstore

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const tableName = "es-test-stack-EventStoreTable-ONSDU4YWZ62N"

func TestSaveShouldSaveToNewStream(t *testing.T) {
	awsSession := session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-west-1")}))
	dynamoSvc := dynamodb.New(awsSession)

	es := &DynamoDbEventStore{Db: dynamoSvc, TableName: tableName}
	bytes := []byte("string")

	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	err := Save(es, streamId, 1, bytes)
	assert.Nil(t, err)
}

func TestGetStreamByIdShouldGetEventsInStream(t *testing.T) {
	awsSession := session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-west-1")}))
	dynamoSvc := dynamodb.New(awsSession)

	es := &DynamoDbEventStore{Db: dynamoSvc, TableName: tableName}

	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	eventData, _ := ioutil.ReadFile("test_event_data.json")

	_ = Save(es, streamId, 1, eventData)
	_ = Save(es, streamId, 2, eventData)
	_ = Save(es, streamId, 3, eventData)
	_ = Save(es, streamId, 4, eventData)

	events, err := GetByStreamId(es, &QueryParams{StreamId: streamId, Version: 1})
	assert.Nil(t, err)
	assert.Equal(t, len(events), 4)
}
