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

func (sp StorageProxy) objectName(name string) string {
	return sp.prefix + name
}

func (sp StorageProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path[0] == '/' {
		path = path[1:]
	}

	sp.logger = sp.logger.With().Str("method", r.Method).Str("path", path).Logger()

	switch r.Method {
	case "GET":
		sp.downloadBlob(w, path)
	case "HEAD":
		sp.checkBlobExists(w, path)
	case "POST":
		sp.uploadBlob(w, r, path)
	case "PUT":
		sp.uploadBlob(w, r, path)
	default:
		sp.logger.Error().Msgf("method %s not allowed", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (sp StorageProxy) downloadBlob(w http.ResponseWriter, name string) {
	object := sp.bucket.Object(sp.objectName(name))
	if object == nil {
		sp.logger.Error().Msg("object not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Set content type header from object attrs
	attrs, err := object.Attrs(context.Background())
	if err != nil {
		sp.logger.Error().Err(err).Msg("failed to get attributes")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h := w.Header()
	h.Add("Content-Type", attrs.ContentType)

	reader, err := object.NewReader(context.Background())
	if err != nil {
		sp.logger.Error().Err(err).Msg("failed to create object reader")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer reader.Close()

	bufferedReader := bufio.NewReader(reader)
	_, err = bufferedReader.WriteTo(w)
	if err != nil {
		sp.logger.Error().Err(err).Msg("failed to write response")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	sp.logger.Info().Msg("success")
}

func (sp StorageProxy) checkBlobExists(w http.ResponseWriter, name string) {
	object := sp.bucket.Object(sp.objectName(name))
	if object == nil {
		sp.logger.Error().Msg("object not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// lookup attributes to see if the object exists
	attrs, err := object.Attrs(context.Background())
	if err != nil || attrs == nil {
		sp.logger.Error().Err(err).Msg("failed to get attributes")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	sp.logger.Info().Msg("success")
}

func (sp StorageProxy) uploadBlob(w http.ResponseWriter, r *http.Request, name string) {
	object := sp.bucket.Object(sp.objectName(name))

	writer := object.NewWriter(context.Background())
	defer writer.Close()

	_, err := bufio.NewWriter(writer).ReadFrom(bufio.NewReader(r.Body))
	if err != nil {
		sp.logger.Error().Err(err).Msg("failed to write object")
		w.WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("failed create object %s", name)
		_, _ = w.Write([]byte(errorMsg))
		return
	}
	w.WriteHeader(http.StatusCreated)
	sp.logger.Info().Msg("success")
}
