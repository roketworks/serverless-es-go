package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestHandler(t *testing.T) {
	file, _ := ioutil.ReadFile("test_payload.json")

	var event events.DynamoDBEvent
	err := json.Unmarshal(file, &event)
	if err != nil {
		panic(err)
	}
	fmt.Println(event)

	handlerErr := handler(event)
	assert.Nil(t, handlerErr)
}
