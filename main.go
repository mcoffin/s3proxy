package main

import (
	"flag"
	"github.com/codegangsta/negroni"
	"github.com/mcoffin/s3proxy/s3proxy"
	"net/http"
)

func main() {
	bind := flag.String("bind", ":8080", "bind address")
	bucket := flag.String("bucket", "", "bucket name")
	region := flag.String("region", "us-east-1", "bucket region")
	flag.Parse()

	mux := http.NewServeMux()
	bucketHandler := s3proxy.NewS3BucketServer(*bucket, *region)
	mux.Handle("/", bucketHandler)

	n := negroni.Classic()
	n.UseHandler(mux)
	n.Run(*bind)
}
