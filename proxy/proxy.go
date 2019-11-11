package proxy

import (
	"bufio"
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
)

type StorageProxy struct {
	bucket *storage.BucketHandle
	prefix string
	logger zerolog.Logger
}

func NewStorageProxy(bucket *storage.BucketHandle, prefix string, logger zerolog.Logger) *StorageProxy {
	return &StorageProxy{
		bucket: bucket,
		prefix: prefix,
		logger: logger,
	}
}

func (proxy StorageProxy) objectName(name string) string {
	return proxy.prefix + name
}

func (proxy StorageProxy) Serve(port int64) error {
	http.HandleFunc("/", proxy.handler)

	addr := fmt.Sprintf(":%d", port)
	proxy.logger.Info().Msgf("starting storage proxy at %s", addr)
	return http.ListenAndServe(addr, nil)
}

func (proxy StorageProxy) handler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path[0] == '/' {
		path = path[1:]
	}

	handlerLogger := proxy.logger.With().Str("method", r.Method).Str("path", path).Logger()

	switch r.Method {
	case "GET":
		proxy.downloadBlob(w, path, handlerLogger)
	case "HEAD":
		proxy.checkBlobExists(w, path, handlerLogger)
	case "POST":
		proxy.uploadBlob(w, r, path, handlerLogger)
	case "PUT":
		proxy.uploadBlob(w, r, path, handlerLogger)
	default:
		proxy.logger.Error().Msgf("method %s not allowed", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (proxy StorageProxy) downloadBlob(w http.ResponseWriter, name string, logger zerolog.Logger) {
	object := proxy.bucket.Object(proxy.objectName(name))
	if object == nil {
		logger.Error().Msg("object not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Set content type header from object attrs
	attrs, err := object.Attrs(context.Background())
	if err != nil {
		logger.Error().Err(err).Msg("failed to get attributes")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h := w.Header()
	h.Add("Content-Type", attrs.ContentType)

	reader, err := object.NewReader(context.Background())
	if err != nil {
		logger.Error().Err(err).Msg("failed to create object reader")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer reader.Close()

	bufferedReader := bufio.NewReader(reader)
	_, err = bufferedReader.WriteTo(w)
	if err != nil {
		logger.Error().Err(err).Msg("failed to write response")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	logger.Info().Msg("success")
}

func (proxy StorageProxy) checkBlobExists(w http.ResponseWriter, name string, logger zerolog.Logger) {
	object := proxy.bucket.Object(proxy.objectName(name))
	if object == nil {
		logger.Error().Msg("object not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// lookup attributes to see if the object exists
	attrs, err := object.Attrs(context.Background())
	if err != nil || attrs == nil {
		logger.Error().Err(err).Msg("failed to get attributes")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	logger.Info().Msg("success")
}

func (proxy StorageProxy) uploadBlob(w http.ResponseWriter, r *http.Request, name string, logger zerolog.Logger) {
	object := proxy.bucket.Object(proxy.objectName(name))

	writer := object.NewWriter(context.Background())
	defer writer.Close()

	_, err := bufio.NewWriter(writer).ReadFrom(bufio.NewReader(r.Body))
	if err != nil {
		logger.Error().Err(err).Msg("failed to write object")
		w.WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("failed create object %s", name)
		w.Write([]byte(errorMsg))
		return
	}
	w.WriteHeader(http.StatusCreated)
	logger.Info().Msg("success")
}
