package cos_connect_bucket

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

type COS_Instance struct {
	ApiKey            string
	ServiceInstanceID string
	AuthEndpoint      string
	ServiceEndpoint   string
	BucketName        string
}

func Connect(c COS_Instance) (bucket *s3.ListObjectsV2Output, object_keys []string, client *s3.S3, err error) {

	conf := aws.NewConfig().
		WithRegion("us-standard").
		WithEndpoint(c.ServiceEndpoint).
		WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(), c.AuthEndpoint, c.ApiKey, c.ServiceInstanceID)).
		WithS3ForcePathStyle(true)

	sess := session.Must(session.NewSession())
	client = s3.New(sess, conf)

	list_objects := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.BucketName),
	}

	bucket, err = client.ListObjectsV2(list_objects)

	type ob []map[string]string
	var jsonMap map[string]ob

	jsonBytes, _ := json.MarshalIndent(bucket, " ", " ")
	json.Unmarshal(jsonBytes, &jsonMap)
	objects := jsonMap["Contents"]

	for _, v := range objects {
		object_keys = append(object_keys, v["Key"])
	}

	return bucket, object_keys, client, err
}

func Check_keys(object_keys []string, key string) bool {
	for _, obj := range object_keys {
		if obj == key {
			return true
		}
	}
	return false
}

func Read_file_from_cos(c COS_Instance, key string, client *s3.S3) (err error) {
	// users will need to create bucket, key (flat string name)
	Input := s3.GetObjectInput{
		Bucket: aws.String(c.BucketName),
		Key:    aws.String(key),
	}

	// Call Function
	res, _ := client.GetObject(&Input)

	body, _ := ioutil.ReadAll(res.Body)

	data := string(body)

	file, _ := os.Create(key)

	if err != nil {
		log.Fatalln("Failed to create file: ", err)
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	_, err = file.WriteString(data)

	if err != nil {
		log.Fatalln("Failed to write file: ", err)
		log.Fatalln("Exit from main.go")
		os.Exit(1)
	}

	return err
}

func Write_file_to_cos(c COS_Instance, key string, client *s3.S3) (err error) {
	DataBytes, erri := ioutil.ReadFile(key)

	if erri != nil {
		log.Fatalf("Failed opening file, error: %s", erri)
		os.Exit(1)
	}

	content := bytes.NewReader([]byte(DataBytes))

	input := s3.PutObjectInput{
		Bucket: aws.String(c.BucketName),
		Key:    aws.String(key),
		Body:   content,
	}

	// Call Function to upload (Put) an object
	result, err := client.PutObject(&input)
	if result != nil {
		log.Print("Preprocessed Object pushed to Cloud Object Storage")
	}

	return err
}
