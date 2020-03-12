package esgo

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

var es *DynamoDbEventStore

func init() {
	es = getEventStore()
}

func TestSaveShouldSaveToNewStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	_, err := Save(es, streamId, 1, "event-type", []byte(eventData))
	assert.Nil(t, err)
}

func TestGetStreamByIdShouldGetEventsInStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	preCommitTime := getTimestamp()
	_, _ = Save(es, streamId, 1, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 2, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 3, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 4, "event-type", []byte(eventData))
	postCommit := getTimestamp()

	events, err := GetByStreamId(es, &GetStreamInput{StreamId: streamId, Version: 1})

	assert.Nil(t, err)
	assert.Equal(t, len(events), 4)

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+1, event.Version)
		assert.GreaterOrEqual(t, event.CommittedAt, preCommitTime)
		assert.LessOrEqual(t, event.CommittedAt, postCommit)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func TestGetLatestByStreamId(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	_, _ = Save(es, streamId, 1, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 2, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 3, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 4, "event-type", []byte(eventData))

	version, err := GetLatestByStreamId(es, streamId)
	assert.Nil(t, err)
	assert.Equal(t, 4, version)

	version, err = GetLatestByStreamId(es, "non-existing-stream")
	assert.Nil(t, err)
	assert.Equal(t, 0, version)
}

func TestGetLatestByAllStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	initialPosition, err := GetLatestByAllStream(es)
	assert.Nil(t, err)

	_, _ = Save(es, streamId, 1, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 2, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 3, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 4, "event-type", []byte(eventData))

	version, err := GetLatestByAllStream(es)
	assert.Nil(t, err)
	assert.Equal(t, initialPosition+4, version)
}

func TestGetAllStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	initialPosition, err := GetLatestByAllStream(es)
	assert.Nil(t, err)

	preCommitTime := getTimestamp()
	_, _ = Save(es, streamId, 1, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 2, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 3, "event-type", []byte(eventData))
	_, _ = Save(es, streamId, 4, "event-type", []byte(eventData))
	postCommit := getTimestamp()

	events, err := GetAllStream(es, initialPosition)

	assert.Nil(t, err)
	assert.Equal(t, len(events), 4)

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+1, event.Version)
		assert.Equal(t, initialPosition+1+int64(i), event.MessagePosition)
		assert.GreaterOrEqual(t, event.CommittedAt, preCommitTime)
		assert.LessOrEqual(t, event.CommittedAt, postCommit)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func getEventStore() *DynamoDbEventStore {
	region := testConfig.Aws.Region
	key := testConfig.Aws.AccessKey
	secret := testConfig.Aws.AccessSecret
	endpoint := testConfig.Aws.DynamoDb.Endpoint
	esTableName := testConfig.Aws.DynamoDb.TableName

	var awsConfig = aws.Config{
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials("default", key, secret),
	}

	var awsSession = session.Must(session.NewSession(&awsConfig))
	var dynamoSvc = dynamodb.New(awsSession)
	return &DynamoDbEventStore{Db: dynamoSvc, EventTable: esTableName}
}

const eventData string = `
{
  "Test": "Value"
}`
