package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go/service/cognitoidentityprovider/cognitoidentityprovideriface"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"
)

var envName string = os.Args[1]
var output string

var tableNames = [17]string{
	envName + "-contacts_api",
	envName + "-access_control_api",
	envName + "-audit_api",
	envName + "-commit_api",
	envName + "-data_warehouse_api",
	envName + "-entities_api",
	envName + "-firms_api",
	envName + "-notification_api",
	envName + "-onboarding_api",
	envName + "-relationships_api",
	envName + "-reports_api",
	envName + "-requests",
	envName + "-rule_api",
	envName + "-schema_api",
	envName + "-tasks_api",
	envName + "-template_api",
	envName + "-transactions_api",
}

var bucketNames = [3]string{
	"ingenio.ca-" + envName + "-documents-bucket",
	"ingenio.ca-" + envName + "-events-bucket",
	"ingenio.ca-" + envName + "-files-bucket",
}

func newDynamoDBClient() dynamodbiface.DynamoDBAPI {
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           "ingenio-dev",
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalf("session error, not created: %v", err)
		os.Exit(1)
	}
	return dynamodb.New(sess)
}

func newS3Client() s3iface.S3API {
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           "ingenio-dev",
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalf("session error, not created: %v", err)
		os.Exit(1)
	}
	return s3.New(sess)
}

func newCognitoClient() cognitoidentityprovideriface.CognitoIdentityProviderAPI {
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           "ingenio-dev",
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalf("session error, not created: %v", err)
		os.Exit(1)
	}
	return cognitoidentityprovider.New(sess)
}

func newParameterStoreClient() ssmiface.SSMAPI {
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           "ingenio-dev",
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalf("session error, not created: %v", err)
		os.Exit(1)
	}
	return ssm.New(sess)
}

func scanDynamoDBTable(svc dynamodbiface.DynamoDBAPI, tableName string) *dynamodb.ScanOutput {
	result, err := svc.Scan(&dynamodb.ScanInput{TableName: &tableName})
	if err != nil {
		log.Fatalf("failed to scan table %s: %v", tableName, err)
		os.Exit(1)
	}
	return result
}

func unmarshallResult(result *dynamodb.ScanOutput) []map[string]interface{} {
	items := make([]map[string]interface{}, 0)
	for _, value := range result.Items {
		item := make(map[string]interface{})
		err := dynamodbattribute.UnmarshalMap(value, &item)
		if err != nil {
			log.Fatalf("failed to unmarshall db object: %v", err)
		}
		items = append(items, item)
	}
	return items
}

func marshallToJson(items []map[string]interface{}) []byte {
	result, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		log.Fatalf("failed to marshall to json: %v", err)
	}
	return result
}

func writeJsonToFile(filename string, data []byte) error {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		log.Fatalf("failed to write to file: %v", err)
	}
	return nil
}

func writeTableToJson(svc dynamodbiface.DynamoDBAPI, tableName string) error {
	result := scanDynamoDBTable(svc, tableName)
	unmarshalledResult := unmarshallResult(result)
	json := marshallToJson(unmarshalledResult)

	filePath := filepath.Join("../output/dynamodb", strings.Replace(tableName+".json", envName, "envName", 1))
	fileDir := filepath.Dir(filePath)

	if err := os.MkdirAll(fileDir, os.ModePerm); err != nil {
		log.Fatalf("failed to created directories: %v", err)
	}

	writeJsonToFile(filePath, json)

	return nil
}

func downloadObject(svc s3iface.S3API, bucketName string, key string) error {
	getObjectInput := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	}
	result, err := svc.GetObject(getObjectInput)
	if err != nil {
		log.Fatalf("failed to download from bucket %s: %v", bucketName, err)
	}
	defer result.Body.Close()

	filePath := filepath.Join(strings.Replace("../output/s3/"+bucketName, envName, "envName", 1), key)
	fileDir := filepath.Dir(filePath)

	if err := os.MkdirAll(fileDir, os.ModePerm); err != nil {
		log.Fatalf("failed to created directories: %v", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("failed to create file %s: %v", filePath, err)
	}
	defer file.Close()

	_, err = io.Copy(file, result.Body)
	if err != nil {
		log.Fatalf("failed to copy s3 data to file %s: %v", filePath, err)
	}

	return nil
}

func downloadAllObjects(svc s3iface.S3API, bucketName string) error {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
	}
	result, err := svc.ListObjectsV2(listObjectsInput)
	if err != nil {
		log.Fatalf("failed to list s3 objects: %v", err)
	}
	for _, item := range result.Contents {
		downloadObject(svc, bucketName, *item.Key)
	}

	return nil
}

func writeCognitoToJson(svcCognito cognitoidentityprovideriface.CognitoIdentityProviderAPI, svcParameterStore ssmiface.SSMAPI) error {
	parameterResult, err := svcParameterStore.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String("/" + envName + "/userPoolId"),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		log.Fatalf("failed to get parameter: %v", err)
	}
	userPoolId := parameterResult.Parameter.Value

	cognitoResult, err := svcCognito.ListUsers(&cognitoidentityprovider.ListUsersInput{
		UserPoolId: userPoolId,
	})
	if err != nil {
		log.Fatalf("failed to list users: %v", err)
	}

	type Attribute struct {
		Name  string
		Value string
	}

	users := make([]map[string]interface{}, len(cognitoResult.Users))
	for i, user := range cognitoResult.Users {
		userMap := make(map[string]interface{})
		userMap["Username"] = *user.Username
		userMap["UserCreateDate"] = user.UserCreateDate
		userMap["UserLastModifiedDate"] = user.UserLastModifiedDate
		userMap["Enabled"] = *user.Enabled
		userMap["UserStatus"] = *user.UserStatus
		attributes := make([]Attribute, len(user.Attributes))
		for j, attr := range user.Attributes {
			attributes[j] = Attribute{Name: *attr.Name, Value: *attr.Value}
		}
		userMap["Attributes"] = attributes
		users[i] = userMap
	}

	json, err := json.Marshal(users)
	if err != nil {
		log.Fatalf("failed to marshal users: %v", err)
	}

	filePath := "../output/cognito/users.json"
	fileDir := filepath.Dir(filePath)

	if err := os.MkdirAll(fileDir, os.ModePerm); err != nil {
		log.Fatalf("failed to created directories: %v", err)
	}

	writeJsonToFile(filePath, json)

	return nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage (go run main.go envName output) - output is optional")
	}

	if len(os.Args) > 2 {
		output = os.Args[2]
	} else {
		output = "all"
	}

	if output == "all" || output == "dynamodb" {
		svcDynamoDB := newDynamoDBClient()
		for _, tableName := range tableNames {
			writeTableToJson(svcDynamoDB, tableName)
		}
	}

	if output == "all" || output == "s3" {
		svcS3 := newS3Client()
		for _, bucketName := range bucketNames {
			downloadAllObjects(svcS3, bucketName)
		}
	}

	if output == "all" || output == "cognito" {
		svcParameterStoreClient := newParameterStoreClient()
		svcCognito := newCognitoClient()
		writeCognitoToJson(svcCognito, svcParameterStoreClient)
	}
}
