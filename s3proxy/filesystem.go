package s3proxy

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
	"os"
	"log"
	"strings"
	"time"
)

type S3BucketFileSystem struct {
	Bucket string
	sss *s3.S3
}

func NewS3BucketFileSystem(bucketName string, config *aws.Config) *S3BucketFileSystem {
	return &S3BucketFileSystem{
		Bucket: bucketName,
		sss: s3.New(config),
	}
}

func (self *S3BucketFileSystem) Open(name string) (http.File, error) {
	log.Printf("trying to open %s", name)
	params := s3.GetObjectInput{
		Bucket: aws.String(self.Bucket),
		Key: aws.String(name),
	}
	if !strings.HasSuffix(name, "/")  {
		resp, err := self.sss.GetObject(&params)
		if err == nil {
			return &s3BucketObject{
				Key: name,
				PossibleDir: false,
				response: resp,
			}, nil
		}
	}

	return &s3BucketObject{
		Key: *params.Key,
		PossibleDir: true,
		response: nil,
	}, nil
}

const s3BucketObjectMode os.FileMode = 0444

type s3BucketObject struct {
	Key string
	PossibleDir bool
	response *s3.GetObjectOutput
}

func (self *s3BucketObject) Read(p []byte) (int, error) {
	if self.response != nil {
		return self.response.Body.Read(p)
	} else {
		return -1, errors.New("Cannot read a non-existant file or directory!")
	}
}

func (self *s3BucketObject) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("Unsupported operation: Seek")
}

func (self *s3BucketObject) Close() error {
	if self.response != nil {
		return self.response.Body.Close()
	}
	return nil
}

func (self *s3BucketObject) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("Unsupported operation: Readdir")
}

func (self *s3BucketObject) Stat() (os.FileInfo, error) {
	return self, nil
}

func (self *s3BucketObject) Name() string {
	return self.Key
}

func (self *s3BucketObject) Size() int64 {
	if self.response != nil {
		return *self.response.ContentLength
	} else {
		return 0
	}
}

func (self *s3BucketObject) Mode() os.FileMode {
	if self.IsDir() {
		return os.ModeDir
	} else {
		return s3BucketObjectMode
	}
}

func (self *s3BucketObject) ModTime() time.Time {
	if self.response != nil {
		return *self.response.LastModified
	} else {
		return time.Now()
	}
}

func (self *s3BucketObject) IsDir() bool {
	return self.PossibleDir
}

func (self *s3BucketObject) Sys() interface{} {
	return nil
}
