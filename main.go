package main

import (
	"flag"
	"github.com/codegangsta/negroni"
	"gopkg.in/yaml.v2"
	"net/http"
	"log"
	"net"
	"io/ioutil"
)

func main() {
	bind := flag.String("bind", ":8080", "bind address")
	bindType := flag.String("bindtype", "tcp", "bind address type")
	configFilename := flag.String("config", "s3proxy.yml", "config file path")
	flag.Parse()

	cfgBuf, err := ioutil.ReadFile(*configFilename)
	if err != nil {
		log.Fatal(err)
	}
	var cfg Config
	err = yaml.Unmarshal(cfgBuf, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	for i := range cfg.Buckets {
		bucketConfig := cfg.Buckets[i]
		fs := bucketConfig.FileSystem()
		bucketHandler := http.FileServer(fs)
		mux.Handle(bucketConfig.Path, bucketHandler)
	}

	// Make sure to free the config buffer so it doesn't uselessly sit in RAM
	cfgBuf = nil

	n := negroni.Classic()
	n.UseHandler(mux)

	l, err := net.Listen(*bindType, *bind)
	if err != nil {
		log.Fatal(err)
	}
	http.Serve(l, n)
}
