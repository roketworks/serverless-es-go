package esgo

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandleDynamoDbStream(t *testing.T) {
	region := testConfig.Aws.Region
	key := testConfig.Aws.AccessKey
	secret := testConfig.Aws.AccessSecret
	endpoint := testConfig.Aws.Sqs.Endpoint

	var awsConfig = aws.Config{
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials("default", key, secret),
	}

	var awsSession = session.Must(session.NewSession(&awsConfig))
	var sqsSvc = sqs.New(awsSession)
	handler := &DynamoDbStreamHandler{
		Sqs:        sqsSvc,
		QueueNames: testConfig.Projections.QueueNames,
	}

	var testEvent events.DynamoDBEvent
	if err := json.Unmarshal([]byte(testStreamPayload), &testEvent); err != nil {
		panic(err)
	}

	err := handler.Handle(testEvent)
	assert.Nil(t, err)

	for _, queueName := range testConfig.Projections.QueueNames {
		queueUrl, _ := sqsSvc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})

		msg, _ := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: queueUrl.QueueUrl,
		})

		assert.NotNil(t, msg)
		assert.Equal(t, 1, len(msg.Messages))

		var event Event
		_ = json.Unmarshal([]byte(*msg.Messages[0].Body), &event)
	}
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
                        "N": "1583923566919"
                    },
                    "position": {
                        "N": "1"
                    },
					"type": {
                        "S": "testevent"
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
