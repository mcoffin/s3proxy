package s3proxy

import (
	"io"
	"net/http"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

type S3BucketServer struct {
	Bucket string
	sss *s3.S3
}

func NewS3BucketServer(bucketName string, region string) *S3BucketServer {
	return &S3BucketServer{
		Bucket: bucketName,
		sss: s3.New(aws.NewConfig().WithRegion(region)),
	}
}

func (self *S3BucketServer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	params := s3.GetObjectInput{
		Bucket: aws.String(self.Bucket),
		Key: aws.String(r.URL.Path),
	}
	resp, err := self.sss.GetObject(&params)
	if err != nil {
		azErr := err.(awserr.Error)
		errCode := azErr.Code()
		if errCode == "NoSuchKey" {
			http.NotFound(rw, r)
			return
		}

		rw.WriteHeader(500)
		rw.Write([]byte(fmt.Sprintf("%s: %s", azErr.Code(), azErr.Message())))
		return
	}
	rw.WriteHeader(200)
	io.Copy(rw, resp.Body)
}
