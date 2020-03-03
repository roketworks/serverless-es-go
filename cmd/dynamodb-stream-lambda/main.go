package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var awsSession = session.Must(session.NewSession())
var dynamoSvc = dynamodb.New(awsSession)
var sqsSvc = sqs.New(awsSession)

func handler(e events.DynamoDBEvent) error {
	var item map[string]events.DynamoDBAttributeValue

	for _, v := range e.Records {
		switch v.EventName {
		case "INSERT":
			fallthrough
		case "MODIFY":
			item = v.Change.NewImage
			println(item)
		default:
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
