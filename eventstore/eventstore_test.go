package eventstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/roketworks/serverless-es-go/test"
	"github.com/stretchr/testify/assert"
)

var region = test.Configuration.Aws.Region
var key = test.Configuration.Aws.AccessKey
var secret = test.Configuration.Aws.AccessSecret
var endpoint = test.Configuration.Aws.DynamoDb.Endpoint
var esTableName = test.Configuration.Aws.DynamoDb.Es.TableName
var sequencesTableName = test.Configuration.Aws.DynamoDb.Sequences.TableName

var awsConfig = aws.Config{
	Endpoint:    aws.String(endpoint),
	Region:      aws.String(region),
	Credentials: credentials.NewStaticCredentials("default", key, secret),
}

var awsSession = session.Must(session.NewSession(&awsConfig))
var dynamoSvc = dynamodb.New(awsSession)
var es = &DynamoDbEventStore{Db: dynamoSvc, EventTable: esTableName, SequenceTable: sequencesTableName}

func TestSaveShouldSaveToNewStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	_, err := Save(es, streamId, 1, []byte(eventData))
	assert.Nil(t, err)
}

func TestGetStreamByIdShouldGetEventsInStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	initialPosition, err := getLatestMessagePosition(es)
	assert.Nil(t, err)

	_, _ = Save(es, streamId, 1, []byte(eventData))
	_, _ = Save(es, streamId, 2, []byte(eventData))
	_, _ = Save(es, streamId, 3, []byte(eventData))
	_, _ = Save(es, streamId, 4, []byte(eventData))

	events, err := GetByStreamId(es, &GetStreamInput{StreamId: streamId, Version: 1})

	assert.Nil(t, err)
	assert.Equal(t, len(events), 4)

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+1, event.Version)
		assert.Equal(t, initialPosition+i+1, event.MessagePosition)
		assert.NotSame(t, new(time.Time), event.CommittedAt)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func TestGetAllStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	initialPosition, err := getLatestMessagePosition(es)
	assert.Nil(t, err)

	_, _ = Save(es, streamId, 1, []byte(eventData))
	_, _ = Save(es, streamId, 2, []byte(eventData))
	_, _ = Save(es, streamId, 3, []byte(eventData))
	_, _ = Save(es, streamId, 4, []byte(eventData))

	events, err := GetAllStream(es, initialPosition+1)

	assert.Nil(t, err)
	assert.Equal(t, len(events), 4)

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+1, event.Version)
		assert.Equal(t, initialPosition+i+1, event.MessagePosition)
		assert.NotSame(t, new(time.Time), event.CommittedAt)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

const eventData string = `
{
  "Test": "Value"
}`
