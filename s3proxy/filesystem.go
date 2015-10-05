package s3proxy

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
	"os"
	"io"
	"log"
	"strings"
	"time"
	"bytes"
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
			bko := s3BucketObject{
				Key: name,
				PossibleDir: false,
				response: resp,
				fileReader: nil,
			}
			err = bko.createFileReader()
			return &bko, err
		}
	}

	// If we're here, the path MIGHT represent a "directory" in S3
	// To determine if it exists, we must ask S3 for a list of all objects in
	// the bucket that have the prefix of the path.
	dirKey := strings.TrimLeft(name, "/")
	var maxKeys int64 = 1000
	listParams := s3.ListObjectsInput{
		Bucket: params.Bucket,
		Prefix: aws.String(dirKey),
		MaxKeys: &maxKeys,
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
	DirKey string
	obj *s3.Object
}

func (self s3BucketDirEntry) Name() string {
	toTrim := strings.TrimLeft(self.DirKey, "/") + "/"
	trimmed := strings.TrimLeft(*self.obj.Key, toTrim)
	return trimmed
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
	position int
	response *s3.ListObjectsOutput
}

func (self *s3BucketDirectory) Read(p []byte) (int, error) {
	return 0, nil
}

func (self *s3BucketDirectory) Close() error {
	return nil
}

func (self *s3BucketDirectory) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("Unsupported operation: Seek on directory")
}

func (self *s3BucketDirectory) Readdir(count int) ([]os.FileInfo, error) {
	nEntries := len(self.response.Contents)
	log.Printf("Directory (%s) has %d contents\n", self.Key, nEntries)
	entries := make([]os.FileInfo, 0, nEntries)
	entriesChan := make(chan os.FileInfo)
	go func() {
		defer close(entriesChan)
		n := 0
		for i := self.position; i < len(self.response.Contents); i++ {
			n++
			if n > count {
				self.position = i - 1
				return
			}

			e := &s3BucketDirEntry{
				DirKey: self.Key,
				obj: self.response.Contents[i],
			}
			if len(e.Name()) != 0 {
				entriesChan <- e
			}
		}
		if n <= count {
			entriesChan <- nil
		}
	}()
	var err error = nil
	for e := range entriesChan {
		if e == nil {
			err = io.EOF
		} else {
			entries = append(entries, e)
		}
	}
	return entries, err
}

func (self *s3BucketDirectory) Stat() (os.FileInfo, error) {
	return self, nil
}

func (self *s3BucketDirectory) Name() string {
	return self.Key
}

func (self *s3BucketDirectory) Size() int64 {
	var contents int64 = int64(len(self.response.Contents))
	contents -= 1
	return contents
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
	fileReader *bytes.Reader
}

func (self *s3BucketObject) createFileReader() error {
	buf := bytes.NewBuffer([]byte{})
	l, err := io.Copy(buf, self.response.Body)
	if err != nil {
		return err
	}
	if l != self.Size() {
		return errors.New("Content length mismatch")
	}
	self.fileReader = bytes.NewReader(buf.Bytes())
	return nil
}

func (self *s3BucketObject) Read(p []byte) (int, error) {
	if self.response != nil {
		if self.fileReader == nil {
			return self.response.Body.Read(p)
		} else {
			return self.fileReader.Read(p)
		}
	} else {
		return -1, errors.New("Cannot read a non-existant file or directory!")
	}
}

func (self *s3BucketObject) Seek(offset int64, whence int) (int64, error) {
	if self.fileReader == nil {
		buf := bytes.NewBuffer([]byte{})
		l, err := io.Copy(buf, self.response.Body)
		if err != nil {
			return 0, err
		}
		if l != self.Size() {
			return l, errors.New("Content length mismatch")
		}
		self.fileReader = bytes.NewReader(buf.Bytes())
	}
	return self.fileReader.Seek(offset, whence)
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
