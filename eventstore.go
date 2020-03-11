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
	Db            dynamodbiface.DynamoDBAPI
	EventTable    string
	SequenceTable string
}

type GetStreamInput struct {
	StreamId string
	Version  int
}

type Event struct {
	StreamId        string `dynamodbav:"streamId"`
	Version         int    `dynamodbav:"version"`
	CommittedAt     int64  `dynamodbav:"committedAt"`
	MessagePosition int    `dynamodbav:"position"`
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
		return res[0].Version, nil
	}
}

func GetAllStream(es *DynamoDbEventStore, sequence int) ([]Event, error) {
	input := &dynamodb.QueryInput{
		TableName: aws.String(es.EventTable),
		//ConsistentRead:         aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"#position": aws.String("position"),
		},
		KeyConditionExpression: aws.String("active = :a AND #position >= :p"),
		IndexName:              aws.String("active-position-index"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":p": {
				N: aws.String(strconv.Itoa(sequence)),
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

func Save(es *DynamoDbEventStore, streamId string, expectedVersion int, eventType string, event []byte) (int, error) {
	commitTime := strconv.FormatInt(getTimestamp(), 10)

	position, err := updateMessagePosition(es)

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
			"position": {
				N: aws.String(strconv.Itoa(position)),
			},
			"active": {
				N: aws.String("1"),
			},
			"type": {
				S: aws.String(eventType),
			},
			"eventData": {
				B: event,
			},
		},
		ConditionExpression: aws.String("attribute_not_exists(version)"),
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

func getLatestMessagePosition(es *DynamoDbEventStore) (int, error) {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(es.SequenceTable),
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"#sequence": aws.String("sequence"),
		},
		KeyConditionExpression: aws.String("#sequence = :v"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				S: aws.String("messages"),
			},
		},
	}

	queryOutput, err := es.Db.Query(input)
	if err != nil {
		return -1, err
	}

	if *queryOutput.Count == 0 {
		if err = insertSequence(es); err != nil {
			return -1, err
		}
		return 0, nil
	}

	if *queryOutput.Count > 1 {
		countErr := errors.New("more than 1 matching sequence found")
		return -1, countErr
	}

	currentValue, err := strconv.Atoi(*queryOutput.Items[0]["counter"].N)
	if err != nil {
		return -1, err
	}

	return currentValue, nil
}

func updateMessagePosition(es *DynamoDbEventStore) (int, error) {
	currentValue, err := getLatestMessagePosition(es)
	if err != nil {
		return -1, err
	}

	newValue := currentValue + 1

	updateInput := &dynamodb.UpdateItemInput{
		TableName:    aws.String(es.SequenceTable),
		ReturnValues: aws.String("UPDATED_NEW"),
		ExpressionAttributeNames: map[string]*string{
			"#counter": aws.String("counter"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":c": {
				N: aws.String(strconv.Itoa(currentValue)),
			},
			":ns": {
				N: aws.String(strconv.Itoa(newValue)),
			},
		},
		ConditionExpression: aws.String("(#counter = :c)"),
		UpdateExpression:    aws.String("SET #counter = :ns"),
		Key: map[string]*dynamodb.AttributeValue{
			"sequence": {
				S: aws.String("messages"),
			},
		},
	}

	_, err = es.Db.UpdateItem(updateInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return -1, errors.New("sequence not at expected value")
			default:
				return -1, aerr
			}
		}
		return -1, err
	}

	return newValue, nil
}

func insertSequence(es *DynamoDbEventStore) error {
	putInput := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"sequence": {
				S: aws.String("messages"),
			},
			"counter": {
				N: aws.String("0"),
			},
		},
		ReturnValues: aws.String("NONE"),
		TableName:    aws.String(es.SequenceTable),
	}

	_, err := es.Db.PutItem(putInput)
	return err
}
