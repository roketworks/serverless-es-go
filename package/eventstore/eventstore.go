package eventstore

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

const AllStreamId string = "_all"

type QueryParams struct {
	StreamId string
	Version  int
}

type Event struct {
	StreamId        string    `dynamodbav:"streamId"`
	Version         int       `dynamodbav:"version"`
	CommittedAt     time.Time `dynamodbav:"committedAt,unixtime"`
	MessagePosition int       `dynamodbav:"position"`
	Data            []byte    `dynamodbav:"eventData"`
}

func GetByStreamId(es *DynamoDbEventStore, params *QueryParams) ([]Event, error) {

	queryFunc := func(lastKey map[string]*dynamodb.AttributeValue) ([]Event, map[string]*dynamodb.AttributeValue, error) {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(es.EventTable),
			ExclusiveStartKey:      lastKey,
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

		result, err := es.Db.Query(input)
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

func Save(es *DynamoDbEventStore, streamId string, expectedVersion int, event []byte) (int, error) {
	now := time.Now()
	nano := now.UnixNano()
	commitTime := strconv.FormatInt(nano/1000000, 10)

	position, err := getLatestMessagePosition(es)

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
		err = insertSequence(es)
	}

	if *queryOutput.Count > 1 {
		countErr := errors.New("more than 1 matching sequence found")
		return -1, countErr
	}

	currentValue, err := strconv.Atoi(*queryOutput.Items[0]["counter"].N)
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
