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

type DynamoDBEventStore struct {
	DB         dynamodbiface.DynamoDBAPI
	EventTable string
}

const (
	PositionStart = 0
	PositionEnd   = -1
)

type Event struct {
	StreamID        string `dynamodbav:"streamId"`
	Version         int    `dynamodbav:"version"`
	CommittedAt     int64  `dynamodbav:"committedAt"`
	MessagePosition int64  `dynamodbav:"position"`
	Type            string `dynamodbav:"type"`
	Data            []byte `dynamodbav:"eventData"`
}

// NewEventStore return an eventstore
func NewEventStore(dynamodb dynamodbiface.DynamoDBAPI, table string) *DynamoDBEventStore {
	return &DynamoDBEventStore{
		DB:         dynamodb,
		EventTable: table,
	}
}

// ReadStreamEventsForward reads from the specified stream id starting at specified index and reads forward by the count
// Query is inclusive of start position
func (es *DynamoDBEventStore) ReadStreamEventsForward(streamID string, start int64, count int64) ([]Event, error) {
	keyCondition := "streamID = :s"
	expressionValues := map[string]*dynamodb.AttributeValue{
		":s": {
			S: aws.String(streamID),
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

// ReadStreamEventsBackward reads from the specified stream starting at specified index and reads backward by the count
// Query is inclusive of start position
func (es *DynamoDBEventStore) ReadStreamEventsBackward(streamID string, start int64, count int64) ([]Event, error) {
	keyCondition := "streamID = :s"
	expressionValues := map[string]*dynamodb.AttributeValue{
		":s": {
			S: aws.String(streamID),
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
func (es *DynamoDBEventStore) ReadAllEventsForward(position int64, count int64) ([]Event, error) {
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

	res, err := queryEvents(es, input)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ReadAllEventsBackward reads from events from all streams starting at specified index and reads backward by the count
// Query is inclusive of start position
func (es *DynamoDBEventStore) ReadAllEventsBackward(position int64, count int64) ([]Event, error) {
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

	res, err := queryEvents(es, input)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Save a new event to a specified stream. Returns the global position of the message.
func (es *DynamoDBEventStore) Save(streamID string, version int, eventType string, event []byte) (int64, error) {
	commitTime := strconv.FormatInt(getTimestamp(), 10)
	lastEvent, err := es.ReadAllEventsBackward(PositionEnd, 1)
	if err != nil {
		return -1, err
	}

	var lastPosition int64
	if len(lastEvent) == 0 {
		lastPosition = 0
	} else {
		lastPosition = lastEvent[0].MessagePosition
	}

	position := lastPosition + 1

	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"streamID": {
				S: aws.String(streamID),
			},
			"committedAt": {
				N: aws.String(commitTime),
			},
			"version": {
				N: aws.String(strconv.Itoa(version)),
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

	_, err = es.DB.PutItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return -1, errors.New("a commit already exists with the specified version")
			default:
				return -1, aerr
			}
		}
		return -1, err
	}

	return position, nil
}

func queryEvents(es *DynamoDBEventStore, queryInput *dynamodb.QueryInput) ([]Event, error) {
	queryFunc := func(lastKey map[string]*dynamodb.AttributeValue) ([]Event, map[string]*dynamodb.AttributeValue, error) {
		queryInput.ExclusiveStartKey = lastKey
		result, err := es.DB.Query(queryInput)
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
		res = append(res, results...)

		if lastKey == nil || (queryInput.Limit != nil && int64(len(res)) >= *queryInput.Limit) {
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
