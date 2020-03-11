package esgo

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type DynamoDbStreamHandlerInput struct {
	QueueNames []string
	Sqs        sqsiface.SQSAPI
}

func HandleDynamoDbStream(input *DynamoDbStreamHandlerInput, event events.DynamoDBEvent) error {
	for _, r := range event.Records {
		switch r.EventName {
		case "INSERT":
			fallthrough
		case "MODIFY":
			if err := sendEventToSqs(input, r); err != nil {
				return err
			}
		default:
		}
	}

	return nil
}

func sendEventToSqs(input *DynamoDbStreamHandlerInput, record events.DynamoDBEventRecord) error {
	var event Event
	if err := unmarshalStreamImage(record.Change.NewImage, &event); err != nil {
		return err
	}

	json, err := json.Marshal(event)
	if err != nil {
		return err
	}

	for _, queueName := range input.QueueNames {
		queueUrl, err := input.Sqs.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(queueName),
		})

		if err != nil {
			return err
		}

		sendMessage := sqs.SendMessageInput{
			QueueUrl:       queueUrl.QueueUrl,
			MessageGroupId: aws.String(event.StreamId),
			MessageBody:    aws.String(string(json)),
		}

		if _, err := input.Sqs.SendMessage(&sendMessage); err != nil {
			return err
		}
	}

	return nil
}

func unmarshalStreamImage(attribute map[string]events.DynamoDBAttributeValue, out interface{}) error {
	dbAttrMap := make(map[string]*dynamodb.AttributeValue)

	for k, v := range attribute {
		var dbAttr dynamodb.AttributeValue
		bytes, marshalErr := v.MarshalJSON()
		if marshalErr != nil {
			return marshalErr
		}
		if err := json.Unmarshal(bytes, &dbAttr); err != nil {
			return err
		}
		dbAttrMap[k] = &dbAttr
	}

	return dynamodbattribute.UnmarshalMap(dbAttrMap, out)
}
