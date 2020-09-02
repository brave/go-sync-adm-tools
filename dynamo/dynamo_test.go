package dynamo

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/suite"
)

type TestEntity struct {
	ClientID string
	ID       string
	TTL      int64 `dynamodbav:",omitempty"`
}

// SyncEntityByClientIDID implements sort.Interface for []SyncEntity based on
// the string concatenation of ClientID and ID fields.
type TestEntityByClientIDID []TestEntity

func (a TestEntityByClientIDID) Len() int      { return len(a) }
func (a TestEntityByClientIDID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a TestEntityByClientIDID) Less(i, j int) bool {
	return a[i].ClientID+a[i].ID < a[j].ClientID+a[j].ID
}

// DeleteTable deletes table in dynamoDB.
func DeleteTable(dynamo *Dynamo) error {
	_, err := dynamo.DeleteTable(
		&dynamodb.DeleteTableInput{TableName: aws.String(table)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			// Return as successful if the table is not existed.
			if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return nil
			}
		} else {
			return fmt.Errorf("error deleting table: %w", err)
		}
	}

	return dynamo.WaitUntilTableNotExists(
		&dynamodb.DescribeTableInput{TableName: aws.String(table)})
}

// CreateTable creates table in dynamoDB.
func CreateTable(dynamo *Dynamo) error {
	_, b, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(b), "../")
	raw, err := ioutil.ReadFile(filepath.Join(root, "dynamo/schema/table.json"))
	if err != nil {
		return fmt.Errorf("error reading table.json: %w", err)
	}

	var input dynamodb.CreateTableInput
	err = json.Unmarshal(raw, &input)
	if err != nil {
		return fmt.Errorf("error unmarshalling raw data from table.json: %w", err)
	}
	input.TableName = aws.String(table)

	_, err = dynamo.CreateTable(&input)
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	return dynamo.WaitUntilTableExists(
		&dynamodb.DescribeTableInput{TableName: aws.String(table)})
}

func ScanTable(dynamo *Dynamo) ([]TestEntity, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(table),
	}
	out, err := dynamo.Scan(input)
	if err != nil {
		return nil, fmt.Errorf("scan table failed: %w", err)
	}
	items := []TestEntity{}
	err = dynamodbattribute.UnmarshalListOfMaps(out.Items, &items)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error after scanning table: %w", err)
	}

	sort.Sort(TestEntityByClientIDID(items))
	return items, nil
}

func PutTestItems(dynamo *Dynamo, items []TestEntity) error {
	for _, item := range items {
		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			return fmt.Errorf("marshal test entity error: %w", err)
		}
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(table),
		}
		_, err = dynamo.PutItem(input)
		if err != nil {
			return fmt.Errorf("error calling PutItem: %w", err)
		}
	}

	return nil
}

func ResetTable(dynamo *Dynamo) error {
	if err := DeleteTable(dynamo); err != nil {
		return fmt.Errorf("error deleting table to reset table: %w", err)
	}
	return CreateTable(dynamo)
}

type DynamoTestSuite struct {
	suite.Suite
	dynamo *Dynamo
}

func (suite *DynamoTestSuite) SetupSuite() {
	table = "client-entity-test-dynamo"
	var err error
	suite.dynamo, err = NewDynamo()
	suite.Require().NoError(err, "Failed to get dynamoDB session")
	suite.Require().NoError(
		CreateTable(suite.dynamo), "Failed to create table")
}

func (suite *DynamoTestSuite) SetupTest() {
	suite.Require().NoError(
		ResetTable(suite.dynamo), "Failed to reset table")
}

func (suite *DynamoTestSuite) TearDownTest() {
	suite.Require().NoError(
		DeleteTable(suite.dynamo), "Failed to delete table")
}

func (suite *DynamoTestSuite) TestDeleteUserData() {
	items := []TestEntity{
		{ClientID: "client1", ID: "client1"},
		{ClientID: "client2", ID: "client2"},
		{ClientID: "client1", ID: "123"},
		{ClientID: "client2", ID: "123"},
		{ClientID: "client1", ID: "456"},
	}
	expectedItems := []TestEntity{
		{ClientID: "client1", ID: "client1", TTL: 12345678},
		{ClientID: "client2", ID: "client2"},
		{ClientID: "client1", ID: "123", TTL: 12345678},
		{ClientID: "client2", ID: "123"},
		{ClientID: "client1", ID: "456", TTL: 12345678},
	}
	sort.Sort(TestEntityByClientIDID(items))
	sort.Sort(TestEntityByClientIDID(expectedItems))
	suite.Require().NoError(PutTestItems(suite.dynamo, items))
	dbItems, err := ScanTable(suite.dynamo)
	suite.Require().NoError(err, "Failed to scan table")
	suite.Assert().Equal(dbItems, items)

	err = suite.dynamo.DeleteUserData("client1", 12345678)
	suite.Require().NoError(err, "Failed to delete user data")
	dbItems, err = ScanTable(suite.dynamo)
	suite.Require().NoError(err, "Failed to scan table")
	suite.Assert().Equal(dbItems, expectedItems)
}

func TestDynamoTestSuite(t *testing.T) {
	suite.Run(t, new(DynamoTestSuite))
}
