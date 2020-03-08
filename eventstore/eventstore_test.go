package eventstore

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	esTableName  = "eventstore"
	seqTableName = "sequences"
)

var awsSession = session.Must(session.NewSession(&aws.Config{Region: aws.String("eu-west-1")}))
var dynamoSvc = dynamodb.New(awsSession)
var es = &DynamoDbEventStore{Db: dynamoSvc, EventTable: esTableName, SequenceTable: seqTableName}

func TestSaveShouldSaveToNewStream(t *testing.T) {
	bytes := []byte("string")
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())

	_, err := Save(es, streamId, 1, bytes)
	assert.Nil(t, err)
}

func TestGetStreamByIdShouldGetEventsInStream(t *testing.T) {
	streamId := fmt.Sprintf("stream-%v", uuid.New().String())
	eventData, _ := ioutil.ReadFile("test_event_data.json")

	_, _ = Save(es, streamId, 1, eventData)
	_, _ = Save(es, streamId, 2, eventData)
	_, _ = Save(es, streamId, 3, eventData)
	_, _ = Save(es, streamId, 4, eventData)

	events, err := GetByStreamId(es, &GetStreamInput{StreamId: streamId, Version: 1})

	assert.Nil(t, err)
	assert.Equal(t, len(events), 4)

	for i, event := range events {
		assert.Equal(t, streamId, event.StreamId)
		assert.Equal(t, i+1, event.Version)
		assert.NotSame(t, new(time.Time), event.CommittedAt)
		assert.Equal(t, eventData, event.Data)
	}
}
