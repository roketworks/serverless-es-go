package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/roketworks/esgo"
	"github.com/spf13/viper"

	_ "github.com/roketworks/esgo/config"
)

var awsSession = session.Must(session.NewSession())
var dynamoSvc = dynamodb.New(awsSession)
var sqsSvc = sqs.New(awsSession)

func handler(e events.DynamoDBEvent) error {
	queues := viper.GetStringSlice("projections.queues")

	handlerInput := &serverless_es_go.DynamoDbStreamHandlerInput{
		Sqs:        sqsSvc,
		QueueNames: queues,
	}

	if err := serverless_es_go.HandleDynamoDbStream(handlerInput, e); err != nil {
		return err
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
