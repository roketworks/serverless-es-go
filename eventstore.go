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

type GetStreamInput struct {
	StreamId string
	Version  int
}

type Event struct {
	StreamId        string `dynamodbav:"streamId"`
	Version         int    `dynamodbav:"version"`
	CommittedAt     int64  `dynamodbav:"committedAt"`
	MessagePosition int64  `dynamodbav:"position"`
	Type            string `dynamodbav:"type"`
	Data            []byte `dynamodbav:"eventData"`
}

func GetByStreamId(es *DynamoDbEventStore, params *GetStreamInput) ([]Event, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(es.EventTable),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("streamId = :a AND version >= :v"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":a": {
				S: aws.String(params.StreamId),
			},
			":v": {
				N: aws.String(strconv.Itoa(params.Version)),
			},
		},
	}

	res, err := queryEvents(es, input)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func GetLatestByStreamId(es *DynamoDbEventStore, streamId string) (int, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(es.EventTable),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("streamId = :a"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":a": {
				S: aws.String(streamId),
			},
		},
		Limit:            aws.Int64(1),
		ScanIndexForward: aws.Bool(false),
	}

	if res, err := queryEvents(es, input); err != nil {
		return -1, err
	} else {
		if len(res) == 0 {
			return 0, nil
		}
		return res[0].Version, nil
	}
}

func GetLatestByAllStream(es *DynamoDbEventStore) (int64, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(es.EventTable),
		KeyConditionExpression: aws.String("active = :a"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":a": {
				N: aws.String("1"),
			},
		},
		IndexName:        aws.String("active-position-index"),
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int64(1),
	}

	if res, err := queryEvents(es, input); err != nil {
		return -1, err
	} else {
		if len(res) == 0 {
			return 0, nil
		}
		return res[0].MessagePosition, nil
	}
}

func GetAllStream(es *DynamoDbEventStore, position int64) ([]Event, error) {
	input := &dynamodb.QueryInput{
		TableName: aws.String(es.EventTable),
		ExpressionAttributeNames: map[string]*string{
			"#position": aws.String("position"),
		},
		KeyConditionExpression: aws.String("active = :a AND #position > :p"),
		IndexName:              aws.String("active-position-index"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":p": {
				N: aws.String(strconv.FormatInt(position, 10)),
			},
			":a": {
				N: aws.String("1"),
			},
		},
	}

	if res, err := queryEvents(es, input); err != nil {
		return nil, err
	} else {
		return res, nil
	}
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

	for _, r := range results {
		res = append(res, r)
	}

	for lastKey != nil {
		for _, r := range results {
			res = append(res, r)
		}

		results, lastKey, err = queryFunc(lastKey)

		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func Save(es *DynamoDbEventStore, streamId string, expectedVersion int, eventType string, event []byte) (int64, error) {
	commitTime := strconv.FormatInt(getTimestamp(), 10)
	lastPosition, err := GetLatestByAllStream(es)
	if err != nil {
		return -1, err
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

func getTimestamp() int64 {
	now := time.Now()
	nano := now.UnixNano()
	return nano / 1000000
}
