package esgo

import (
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DynamoDbEventStore struct {
	Db         dynamodbiface.DynamoDBAPI
	EventTable string
}

const (
	PositionStart = 0
	PositionEnd   = -1
)

type Event struct {
	StreamId        string `dynamodbav:"streamId"`
	Version         int    `dynamodbav:"version"`
	CommittedAt     int64  `dynamodbav:"committedAt"`
	MessagePosition int64  `dynamodbav:"position"`
	Type            string `dynamodbav:"type"`
	Data            []byte `dynamodbav:"eventData"`
}

// NewEventStore return an eventstore
func NewEventStore(dynamodb dynamodbiface.DynamoDBAPI, table string) *DynamoDbEventStore {
	return &DynamoDbEventStore{
		Db:         dynamodb,
		EventTable: table,
	}
}

// ReadStreamEventsForward reads from the specified stream id starting at specified index and reads forward by the count
// Query is inclusive of start position
func (es *DynamoDbEventStore) ReadStreamEventsForward(streamId string, start int64, count int64) ([]Event, error) {
	keyCondition := "streamId = :s"
	expressionValues := map[string]*dynamodb.AttributeValue{
		":s": {
			S: aws.String(streamId),
		},
	}

	if start != PositionStart {
		keyCondition += " AND version >= :v"
		expressionValues[":v"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(start, 10)),
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(es.EventTable),
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: expressionValues,
		ScanIndexForward:          aws.Bool(true),
	}

	if count != PositionEnd {
		input.Limit = aws.Int64(count)
	}

	res, err := queryEvents(es, input)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ReadStreamEventsBackward reads from the specified stream id starting at specified index and reads backward by the count
// Query is inclusive of start position
func (es *DynamoDbEventStore) ReadStreamEventsBackward(streamId string, start int64, count int64) ([]Event, error) {
	keyCondition := "streamId = :s"
	expressionValues := map[string]*dynamodb.AttributeValue{
		":s": {
			S: aws.String(streamId),
		},
	}

	if start != PositionEnd {
		keyCondition += " AND version <= :v"
		expressionValues[":v"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(start, 10)),
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(es.EventTable),
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: expressionValues,
		ScanIndexForward:          aws.Bool(false),
	}

	if count != PositionStart {
		input.Limit = aws.Int64(count)
	}

	res, err := queryEvents(es, input)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ReadAllEventsForward reads from events across all streams starting at specified index and reads forward by the count
// Query is inclusive of start position
func (es *DynamoDbEventStore) ReadAllEventsForward(position int64, count int64) ([]Event, error) {
	keyCondition := "active = :a"
	var expressionNames map[string]*string = nil
	expressionValues := map[string]*dynamodb.AttributeValue{
		":a": {
			N: aws.String("1"),
		},
	}

	if position != PositionStart {
		keyCondition += " AND #position >= :p"
		expressionNames = map[string]*string{
			"#position": aws.String("position"),
		}
		expressionValues[":p"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(position, 10)),
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(es.EventTable),
		IndexName:                 aws.String("active-position-index"),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeNames:  expressionNames,
		ExpressionAttributeValues: expressionValues,
		ScanIndexForward:          aws.Bool(true),
	}

	if count != PositionEnd {
		input.Limit = aws.Int64(count)
	}

	if res, err := queryEvents(es, input); err != nil {
		return nil, err
	} else {
		return res, nil
	}
}

// ReadAllEventsBackward reads from events across all streams starting at specified index and reads backward by the count
// Query is inclusive of start position
func (es *DynamoDbEventStore) ReadAllEventsBackward(position int64, count int64) ([]Event, error) {
	keyCondition := "active = :a"
	var expressionNames map[string]*string = nil
	expressionValues := map[string]*dynamodb.AttributeValue{
		":a": {
			N: aws.String("1"),
		},
	}

	if position != PositionEnd {
		keyCondition += " AND #position <= :p"
		expressionNames = map[string]*string{
			"#position": aws.String("position"),
		}
		expressionValues[":p"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(position, 10)),
		}
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(es.EventTable),
		IndexName:                 aws.String("active-position-index"),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeNames:  expressionNames,
		ExpressionAttributeValues: expressionValues,
		ScanIndexForward:          aws.Bool(false),
	}

	if count != PositionStart {
		input.Limit = aws.Int64(count)
	}

	if res, err := queryEvents(es, input); err != nil {
		return nil, err
	} else {
		return res, nil
	}
}

// Save a new event to a specified stream. Returns the global position of the message.
func (es *DynamoDbEventStore) Save(streamId string, expectedVersion int, eventType string, event []byte) (int64, error) {
	commitTime := strconv.FormatInt(getTimestamp(), 10)
	lastEvent, err := es.ReadAllEventsBackward(PositionEnd, 1)
	if err != nil {
		return -1, err
	}

	var lastPosition int64
	if lastEvent == nil || len(lastEvent) == 0 {
		lastPosition = 0
	} else {
		lastPosition = lastEvent[0].MessagePosition
	}

	position := lastPosition + 1

	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"streamId": {
				S: aws.String(streamId),
			},
			"committedAt": {
				N: aws.String(commitTime),
			},
			"version": {
				N: aws.String(strconv.Itoa(expectedVersion)),
			},
			"active": {
				N: aws.String("1"),
			},
			"position": {
				N: aws.String(strconv.FormatInt(position, 10)),
			},
			"type": {
				S: aws.String(eventType),
			},
			"eventData": {
				B: event,
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#position": aws.String("position"),
		},
		ConditionExpression: aws.String("attribute_not_exists(version) AND attribute_not_exists(#position)"),
		ReturnValues:        aws.String("NONE"),
		TableName:           aws.String(es.EventTable),
	}

	_, err = es.Db.PutItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return -1, errors.New("A commit already exists with the specified version")
			default:
				return -1, aerr
			}
		}
		return -1, err
	}

	return position, nil
}

func queryEvents(es *DynamoDbEventStore, queryInput *dynamodb.QueryInput) ([]Event, error) {
	queryFunc := func(lastKey map[string]*dynamodb.AttributeValue) ([]Event, map[string]*dynamodb.AttributeValue, error) {
		queryInput.ExclusiveStartKey = lastKey
		result, err := es.Db.Query(queryInput)
		if err != nil {
			return nil, nil, err
		}

		var events []Event
		err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &events)
		if err != nil {
			return nil, nil, err
		}

		return events, result.LastEvaluatedKey, nil
	}

	var res []Event
	results, lastKey, err := queryFunc(nil)

	if err != nil {
		return nil, err
	}

	for {
		for _, r := range results {
			res = append(res, r)
		}

		if lastKey == nil || int64(len(results)) >= *queryInput.Limit {
			break
		}

		if results, lastKey, err = queryFunc(lastKey); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func getTimestamp() int64 {
	now := time.Now()
	nano := now.UnixNano()
	return nano / 1000000
}
