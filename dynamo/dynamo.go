package dynamo

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	clientIDKey = "ClientID"
	idKey       = "ID"
	projPk      = clientIDKey + ", " + idKey
	ttlAttrName = "TTL"
)

var (
	// Can be modified during tests.
	table = "client-entity"
)

// Dynamo is a Datastore wrapper around a dynamoDB.
type Dynamo struct {
	*dynamodb.DynamoDB
}

// DeleteUserData uses clientID to query all items under this clientID and set
// ttl for them.
func (dynamo *Dynamo) DeleteUserData(clientID string, ttl int64) error {
	fmt.Println("Deleting user data for clientID", clientID)
	// Query to get keys for all items under clientID as the partition key.
	keyCond := expression.Key("ClientID").Equal(expression.Value(clientID))
	exprs := expression.NewBuilder().WithKeyCondition(keyCond)
	expr, err := exprs.Build()
	if err != nil {
		return fmt.Errorf("error building expression: %w", err)
	}

	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      aws.String(projPk),
		TableName:                 aws.String(table),
	}

	var keys []map[string]*dynamodb.AttributeValue
	err = dynamo.QueryPages(input,
		func(queryOut *dynamodb.QueryOutput, last bool) bool {
			keys = append(keys, queryOut.Items...)
			return last
		})
	if err != nil {
		return fmt.Errorf("error doing query: %w", err)
	}

	// For each key, update their ttl to 10 days after.
	update := expression.Set(expression.Name(ttlAttrName), expression.Value(ttl))
	expr, err = expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return fmt.Errorf("error building expression: %w", err)
	}
	for _, key := range keys {
		input := &dynamodb.UpdateItemInput{
			Key:                       key,
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			TableName:                 aws.String(table),
		}
		_, err := dynamo.UpdateItem(input)
		if err != nil {
			return fmt.Errorf("error updating ttl: %w", err)
		}
	}
	fmt.Println("Successfully set ttl for", len(keys), "records")

	return nil
}

// NewDynamo returns a dynamoDB client to be used.
func NewDynamo() (*Dynamo, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us_west2"
	}

	endpoint := os.Getenv("AWS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8000"
	}

	config := &aws.Config{
		Region:   aws.String(region),
		Endpoint: aws.String(endpoint),
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("error creating new AWS session: %w", err)
	}

	db := dynamodb.New(sess)
	return &Dynamo{db}, nil
}
