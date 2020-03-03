package eventstore

import (
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type EventStore struct {
	Db        dynamodbiface.DynamoDBAPI
	TableName string
}

type QueryParams struct {
	StreamId string
	Version  int
}

type Event struct {
}

func GetByStreamId(es *EventStore, params *QueryParams) ([]map[string]*dynamodb.AttributeValue, error) {
	results, err := queryInternal(es, params)
	if err != nil {
		return nil, nil
	}

	// TODO: map to event struct
	return results, err
}

func queryInternal(es *EventStore, params *QueryParams) ([]map[string]*dynamodb.AttributeValue, error) {

	queryFunc := func(lastKey map[string]*dynamodb.AttributeValue) ([]map[string]*dynamodb.AttributeValue, map[string]*dynamodb.AttributeValue, error) {
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

		return result.Items, result.LastEvaluatedKey, nil
	}

	var res []map[string]*dynamodb.AttributeValue

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

		if lastKey == nil {
			return res, nil
		}

		results, lastKey, err = queryFunc(lastKey)

		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func Save(es *EventStore, streamId string, expectedVersion int, event []byte) error {
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
