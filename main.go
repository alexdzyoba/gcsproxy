package main

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/koding/multiconfig"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

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

	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	logger := log.With().Str("component", "main").Logger()
	client, err := storage.NewClient(context.Background())
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create a storage client")
	}

	bucket := client.Bucket(conf.Bucket)

	proxyLogger := log.With().Str("component", "proxy").Logger()
	storageProxy := proxy.NewStorageProxy(bucket, conf.Prefix, proxyLogger)

	err = storageProxy.Serve(conf.Port)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start proxy")
	}
}
