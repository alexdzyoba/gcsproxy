package main

import (
	"context"
	"log"

	"cloud.google.com/go/storage"
	"github.com/koding/multiconfig"

	"github.com/alexdzyoba/gcsproxy/proxy"
)

type GCSProxy struct {
	Port   int64  `default:"8080"`
	Bucket string `required:"true"`
	Prefix string
}

func main() {
	conf := new(GCSProxy)
	multiconfig.New().MustLoad(conf)

	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create a storage client: %s", err)
	}

	bucketHandler := client.Bucket(conf.Bucket)
	storageProxy := proxy.NewStorageProxy(bucketHandler, conf.Prefix)

	err = storageProxy.Serve(conf.Port)
	if err != nil {
		log.Fatalf("Failed to start proxy: %s", err)
	}
}
