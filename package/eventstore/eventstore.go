package eventstore

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DynamoDbEventStore struct {
	Db        dynamodbiface.DynamoDBAPI
	TableName string
}

type QueryParams struct {
	StreamId string
	Version  int
}

type Event struct {
	StreamId    string    `dynamodbav:"streamId"`
	CommittedAt time.Time `dynamodbav:"committedAt,unixtime"`
	Version     int       `dynamodbav:"version"`
	Data        []byte    `dynamodbav:"eventData"`
}

func GetByStreamId(es *DynamoDbEventStore, params *QueryParams) ([]Event, error) {
	results, err := queryInternal(es, params)
	if err != nil {
		return nil, nil
	}

	return results, err
}

func queryInternal(es *DynamoDbEventStore, params *QueryParams) ([]Event, error) {

	queryFunc := func(lastKey map[string]*dynamodb.AttributeValue) ([]Event, map[string]*dynamodb.AttributeValue, error) {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(es.TableName),
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

func Save(es *DynamoDbEventStore, streamId string, expectedVersion int, event []byte) error {
	now := time.Now()
	nano := now.UnixNano()
	commitTime := strconv.FormatInt(nano/1000000, 10)

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
			"eventData": {
				B: event,
			},
		},
		ConditionExpression: aws.String("attribute_not_exists(version)"),
		ReturnValues:        aws.String("NONE"),
		TableName:           aws.String(es.TableName),
	}

	_, err := es.Db.PutItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return errors.New("A commit already exists with the specified version")
			default:
				return aerr
			}
		}
		return err
	}

	return nil
}
