package main

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/koding/multiconfig"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/alexdzyoba/gcsproxy/proxy"
)

var (
	requestsCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "http_requests_total"},
		[]string{"code", "method"},
	)

	requestsDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "http_duration_seconds"},
		[]string{"code", "method"},
	)
)

type GCSProxy struct {
	ServerPort    int    `default:"8080"`
	TelemetryPort int    `default:"9090"`
	Bucket        string `required:"true"`
	Prefix        string
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

	// Run Metrics server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	go func() {
		_ = http.ListenAndServe(fmt.Sprintf(":%d", conf.TelemetryPort), metricsMux)
	}()

	bucket := client.Bucket(conf.Bucket)

	proxyLogger := log.With().Str("component", "proxy").Logger()
	storageProxy := proxy.NewStorageProxy(bucket, conf.Prefix, proxyLogger)

	// Run Proxy server
	proxyMux := http.NewServeMux()
	proxyMux.Handle("/", promhttp.InstrumentHandlerDuration(requestsDuration,
		promhttp.InstrumentHandlerCounter(requestsCounter, storageProxy),
	))

	logger.Info().Msg("starting proxy...")
	err = http.ListenAndServe(fmt.Sprintf(":%d", conf.ServerPort), proxyMux)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start proxy")
	}
}
