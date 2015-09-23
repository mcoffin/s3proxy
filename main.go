package main

import (
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/codegangsta/negroni"
	"github.com/mcoffin/s3proxy/s3proxy"
	"net/http"
	"log"
	"net"
)

func main() {
	bind := flag.String("bind", ":8080", "bind address")
	bindType := flag.String("bindtype", "tcp", "bind address type")
	bucket := flag.String("bucket", "", "bucket name")
	region := flag.String("region", "us-east-1", "bucket region")
	flag.Parse()

	mux := http.NewServeMux()
	awsConfig := aws.NewConfig().WithRegion(*region)
	fs := s3proxy.NewS3BucketFileSystem(*bucket, awsConfig)
	bucketHandler := http.FileServer(fs)
	mux.Handle("/", bucketHandler)

	n := negroni.Classic()
	n.UseHandler(mux)

	l, err := net.Listen(*bindType, *bind)
	if err != nil {
		log.Fatal(err)
	}
	http.Serve(l, n)
}
