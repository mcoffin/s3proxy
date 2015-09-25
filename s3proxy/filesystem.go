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

	// If we're here, the path MIGHT represent a "directory" in S3
	// To determine if it exists, we must ask S3 for a list of all objects in
	// the bucket that have the prefix of the path.
	listParams := s3.ListObjectsInput{
		Bucket: params.Bucket,
		Delimiter: aws.String("/"),
		Prefix: params.Key,
	}
	resp, err := self.sss.ListObjects(&listParams)
	if err != nil {
		return nil, err
	}
	// If it contains nothing, then the directory doesn't exist
	if len(resp.Contents) <= 0 {
		return nil, os.ErrNotExist
	}

	return &s3BucketDirectory {
		Key: *params.Key,
		response: resp,
	}, nil
}

type s3BucketDirEntry struct {
	obj *s3.Object
}

func (self s3BucketDirEntry) Name() string {
	return *self.obj.Key
}

func (self s3BucketDirEntry) Size() int64 {
	return *self.obj.Size
}

func (self s3BucketDirEntry) Mode() os.FileMode {
	return s3BucketObjectMode
}

func (self s3BucketDirEntry) ModTime() time.Time {
	return *self.obj.LastModified
}

func (self s3BucketDirEntry) IsDir() bool {
	return false
}

func (self s3BucketDirEntry) Sys() interface{} {
	return nil
}

type s3BucketDirectory struct {
	Key string
	response *s3.ListObjectsOutput
}

func (self *s3BucketDirectory) Read(p []byte) (int, error) {
	return 0, nil
}

func (self *s3BucketDirectory) Close() error {
	return nil
}

func (self *s3BucketDirectory) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("Unsupported operation: Seek")
}

func (self *s3BucketDirectory) Readdir(count int) ([]os.FileInfo, error) {
	nEntries := len(self.response.Contents)
	entries := make([]os.FileInfo, nEntries, nEntries)
	for i := range self.response.Contents {
		entries[i] = &s3BucketDirEntry{
			obj: self.response.Contents[i],
		}
	}
	return entries, nil
}

func (self *s3BucketDirectory) Stat() (os.FileInfo, error) {
	return self, nil
}

func (self *s3BucketDirectory) Name() string {
	return self.Key
}

func (self *s3BucketDirectory) Size() int64 {
	return 0
}

func (self *s3BucketDirectory) Mode() os.FileMode {
	return os.ModeDir
}

func (self *s3BucketDirectory) ModTime() time.Time {
	return time.Now()
}

func (self *s3BucketDirectory) IsDir() bool {
	return true
}

func (self *s3BucketDirectory) Sys() interface{} {
	return nil
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
