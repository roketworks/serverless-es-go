package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var awsSession = session.Must(session.NewSession(&aws.Config{}))
var snsSvc = sns.New(awsSession)
var dynamoSvc = dynamodb.New(awsSession)

func handler(e events.SNSEvent) error {
	for _, record := range e.Records {
		snsRecord := record.SNS
		fmt.Printf("[%s %s] Message = %s \n", record.EventSource, snsRecord.Timestamp, snsRecord.Message)
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
