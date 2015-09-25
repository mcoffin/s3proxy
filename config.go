package main

import (
	"net/http"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/mcoffin/s3proxy/s3proxy"
)

type BucketConfig struct {
	Name string `yaml:"name"`
	Region string `yaml:"region"`
	Path string `yaml:"path"`
}

func (self BucketConfig) FileSystem() http.FileSystem {
	awsConfig := aws.NewConfig().WithRegion(self.Region)
	return s3proxy.NewS3BucketFileSystem(self.Name, awsConfig)
}

type Config struct {
	Buckets []BucketConfig `yaml:"buckets"`
}
