package main

import (
	"context"
	"flag"
	"log"

	"cloud.google.com/go/storage"
	"github.com/cirruslabs/google-storage-proxy/proxy"
)

func main() {
	var (
		port          int64
		bucketName    string
		defaultPrefix string
	)

	flag.Int64Var(&port, "port", 8080, "Port to serve")
	flag.StringVar(&bucketName, "bucket", "", "Google Storage Bucket Name")
	flag.StringVar(&defaultPrefix, "prefix", "", "Optional prefix for all objects. For example, use --prefix=foo/ to work under foo directory in a bucket.")
	flag.Parse()

	if bucketName == "" {
		log.Fatal("Please specify Google Cloud Storage Bucket")
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create a storage client: %s", err)
	}

	bucketHandler := client.Bucket(bucketName)
	storageProxy := proxy.NewStorageProxy(bucketHandler, defaultPrefix)

	err = storageProxy.Serve(port)
	if err != nil {
		log.Fatalf("Failed to start proxy: %s", err)
	}
}
