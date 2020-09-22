package pkg

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
	config := aws.NewConfig()
	config.Endpoint = aws.String("http://localhost:4566")
	config.Region = aws.String("eu-west-1")
	config.WithCredentials(credentials.NewStaticCredentials("test" , "test", "test"))

	var awsSession = session.Must(session.NewSession(config))
	var dynamoSvc = dynamodb.New(awsSession)
	es = NewEventStore(dynamoSvc, "eventstore")
}

func TestSaveShouldSaveToNewStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	_, err := es.Save(streamId, 1, "event-type", []byte(eventData))
	assert.Nil(t, err)
}

func TestDynamoDbEventStore_ReadStreamEventsForward_FromStart_ToEnd(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	preCommitTime := getTimestamp()
	saveTestEvents(streamId, 4, 1)
	postCommit := getTimestamp()

	events, err := es.ReadStreamEventsForward(streamId, PositionStart, PositionEnd)

	assert.Nil(t, err)
	assert.Equal(t, 4, len(events))

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+1, event.Version)
		assert.GreaterOrEqual(t, event.CommittedAt, preCommitTime)
		assert.LessOrEqual(t, event.CommittedAt, postCommit)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func TestDynamoDbEventStore_ReadStreamEventsForward_FromPositionInStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	preCommitTime := getTimestamp()
	saveTestEvents(streamId, 4, 1)
	postCommit := getTimestamp()

	events, err := es.ReadStreamEventsForward(streamId, 3, 2)

	assert.Nil(t, err)
	assert.Equal(t, 2, len(events))

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+3, event.Version)
		assert.GreaterOrEqual(t, event.CommittedAt, preCommitTime)
		assert.LessOrEqual(t, event.CommittedAt, postCommit)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func TestDynamoDbEventStore_ReadStreamEventsBackward_FromEnd(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	preCommitTime := getTimestamp()
	saveTestEvents(streamId, 4, 1)
	postCommit := getTimestamp()

	events, err := es.ReadStreamEventsBackward(streamId, PositionEnd, PositionStart)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(events))
	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, 4-i, event.Version)
		assert.GreaterOrEqual(t, event.CommittedAt, preCommitTime)
		assert.LessOrEqual(t, event.CommittedAt, postCommit)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func TestDynamoDbEventStore_ReadStreamEventsBackward_FromPosition(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	preCommitTime := getTimestamp()
	saveTestEvents(streamId, 4, 1)
	postCommit := getTimestamp()

	events, err := es.ReadStreamEventsBackward(streamId, 3, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(events))
	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, 3-i, event.Version)
		assert.GreaterOrEqual(t, event.CommittedAt, preCommitTime)
		assert.LessOrEqual(t, event.CommittedAt, postCommit)
		assert.Equal(t, []byte(eventData), event.Data)
	}
}

func TestDynamoDbEventStore_ReadAllEventsForward_FromStart_ToEnd(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	lastEvent, err := es.ReadAllEventsBackward(PositionEnd, 1)
	assert.Nil(t, err)

	initialPosition := lastEvent[0].MessagePosition
	saveTestEvents(streamId, 10, 1)

	events, err := es.ReadAllEventsForward(PositionStart, PositionEnd)
	assert.Nil(t, err)
	assert.Equal(t, initialPosition + 10, int64(len(events)))

	for i := 0; i < int(initialPosition) + 10; i++ {
		assert.Equal(t, int64(i)+1, events[i].MessagePosition)
	}
}

func TestDynamoDbEventStore_ReadAllEventsForward_FromPosition(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	lastEvent, err := es.ReadAllEventsBackward(PositionEnd, 1)
	assert.Nil(t, err)

	initialPosition := lastEvent[0].MessagePosition
	saveTestEvents(streamId, 10, 1)

	events, err := es.ReadAllEventsForward(initialPosition+1, 5)
	assert.Nil(t, err)
	assert.Equal(t,  5, len(events))

	for i := 0; i < 5; i++ {
		assert.Equal(t, initialPosition+1+int64(i), events[i].MessagePosition)
	}
}

func TestDynamoDbEventStore_ReadAllEventsBackward_FromStart_ToEnd(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	lastEvent, err := es.ReadAllEventsBackward(PositionEnd, 1)
	assert.Nil(t, err)

	initialPosition := lastEvent[0].MessagePosition
	saveTestEvents(streamId, 10, 1)

	events, err := es.ReadAllEventsBackward(PositionEnd, PositionStart)
	assert.Nil(t, err)
	assert.Equal(t, initialPosition+10, int64(len(events)))

	for i := 0; i < int(initialPosition)+10; i++ {
		assert.Equal(t, initialPosition+10-int64(i), events[i].MessagePosition)
	}
}

func TestDynamoDbEventStore_ReadAllEventsBackward_FromPosition(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	lastEvent, err := es.ReadAllEventsBackward(PositionEnd, 1)
	assert.Nil(t, err)

	initialPosition := lastEvent[0].MessagePosition
	saveTestEvents(streamId, 10, 1)

	events, err := es.ReadAllEventsBackward(initialPosition+1, 5)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(events))

	for i := 0; i < 5; i++ {
		assert.Equal(t, initialPosition+1-int64(i), events[i].MessagePosition)
	}
}

func saveTestEvents(stream string, count int, start int) {
	for i := 0; i < count; i++  {
		if _, err := es.Save(stream, start + i, "testevent", []byte(eventData)); err != nil {
			panic(err)
		}
	}
}

const eventData string = `
{
  "Test": "Value"
}`
