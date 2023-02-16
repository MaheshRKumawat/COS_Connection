package cos_connect_bucket

import (
	"encoding/json"

	"github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials/ibmiam"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
)

func Connect(apiKey, serviceInstanceID, authEndpoint, serviceEndpoint, bucketName string) (bucket *s3.ListObjectsV2Output, object_keys []string) {
	// Create config
	conf := aws.NewConfig().
		WithRegion("us-standard").
		WithEndpoint(serviceEndpoint).
		WithCredentials(ibmiam.NewStaticCredentials(aws.NewConfig(), authEndpoint, apiKey, serviceInstanceID)).
		WithS3ForcePathStyle(true)

	sess := session.Must(session.NewSession())
	client := s3.New(sess, conf)

	list_objects := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	bucket, _ = client.ListObjectsV2(list_objects)

	type ob []map[string]string
	var jsonMap map[string]ob

	jsonBytes, _ := json.MarshalIndent(bucket, " ", " ")
	json.Unmarshal(jsonBytes, &jsonMap)
	objects := jsonMap["Contents"]

	for _, v := range objects {
		object_keys = append(object_keys, v["Key"])
	}

	return bucket, object_keys
}
