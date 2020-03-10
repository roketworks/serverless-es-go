package eventstore

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/roketworks/serverless-es-go/test"
	"github.com/stretchr/testify/assert"
	"testing"

	_ "github.com/roketworks/serverless-es-go/test"
)

func TestHandleDynamoDbStream(t *testing.T) {
	region := test.Configuration.Aws.Region
	key := test.Configuration.Aws.AccessKey
	secret := test.Configuration.Aws.AccessSecret
	endpoint := test.Configuration.Aws.Sqs.Endpoint

	var awsConfig = aws.Config{
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials("default", key, secret),
	}

	var awsSession = session.Must(session.NewSession(&awsConfig))
	var sqsSvc = sqs.New(awsSession)
	handlerInput := DynamoDbStreamHandlerInput{
		Sqs:        sqsSvc,
		QueueNames: test.Configuration.Projections.QueueNames,
	}

	var testEvent events.DynamoDBEvent
	if err := json.Unmarshal([]byte(testStreamPayload), &testEvent); err != nil {
		panic(err)
	}

	err := HandleDynamoDbStream(&handlerInput, testEvent)
	assert.Nil(t, err)
}

const testStreamPayload = `
{
    "Records": [
        {
            "eventID": "7de3041dd709b024af6f29e4fa13d34c",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "eu-west-1",
            "dynamodb": {
                "ApproximateCreationDateTime": 1479499740,
                "Keys": {
                    "streamId": {
                        "S": "stream-1"
                    },
                    "version": {
                        "N": "1"
                    }
                },
                "NewImage": {
                    "streamId": {
                        "S": "stream-1"
                    },
                    "version": {
                        "N": "1"
                    },
                    "committedAt": {
                        "N": "1583853537244"
                    },
                    "messagePosition": {
                        "N": "1"
                    },
                    "eventData": {
                        "B": "CnsKICAiVGVzdCI6ICJWYWx1ZSIKfQ=="
                    }
                },
                "SequenceNumber": "13021600000000001596893679",
                "SizeBytes": 112,
                "StreamViewType": "NEW_IMAGE"
            },
            "eventSourceARN": "arn:aws:dynamodb:eu-west-1:000000000000:table/Test/stream/2020-01-01T00:00:00.000"
        }
    ]
}`
