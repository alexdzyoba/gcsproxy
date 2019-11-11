package proxy

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"

	"cloud.google.com/go/storage"
)

type StorageProxy struct {
	bucket *storage.BucketHandle
	prefix string
}

func NewStorageProxy(bucket *storage.BucketHandle, prefix string) *StorageProxy {
	return &StorageProxy{
		bucket: bucket,
		prefix: prefix,
	}
}

func (proxy StorageProxy) objectName(name string) string {
	return proxy.prefix + name
}

func (proxy StorageProxy) Serve(port int64) error {
	http.HandleFunc("/", proxy.handler)

	addr := fmt.Sprintf(":%d", port)
	log.Printf("starting storage proxy at %s\n", addr)
	return http.ListenAndServe(addr, nil)
}

func (proxy StorageProxy) handler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path[0] == '/' {
		path = path[1:]
	}

	switch r.Method {
	case "GET":
		proxy.downloadBlob(w, path)
	case "HEAD":
		proxy.checkBlobExists(w, path)
	case "POST":
		proxy.uploadBlob(w, r, path)
	case "PUT":
		proxy.uploadBlob(w, r, path)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (proxy StorageProxy) downloadBlob(w http.ResponseWriter, name string) {
	object := proxy.bucket.Object(proxy.objectName(name))
	if object == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Set content type header from object attrs
	attrs, err := object.Attrs(context.Background())
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h := w.Header()
	h.Add("Content-Type", attrs.ContentType)

	reader, err := object.NewReader(context.Background())
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer reader.Close()

	bufferedReader := bufio.NewReader(reader)
	_, err = bufferedReader.WriteTo(w)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (proxy StorageProxy) checkBlobExists(w http.ResponseWriter, name string) {
	object := proxy.bucket.Object(proxy.objectName(name))
	if object == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// lookup attributes to see if the object exists
	attrs, err := object.Attrs(context.Background())
	if err != nil || attrs == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (proxy StorageProxy) uploadBlob(w http.ResponseWriter, r *http.Request, name string) {
	object := proxy.bucket.Object(proxy.objectName(name))

	writer := object.NewWriter(context.Background())
	defer writer.Close()

	_, err := bufio.NewWriter(writer).ReadFrom(bufio.NewReader(r.Body))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		errorMsg := fmt.Sprintf("Failed read cache body! %s", err)
		w.Write([]byte(errorMsg))
		return
	}
	w.WriteHeader(http.StatusCreated)
}
