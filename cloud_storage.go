package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

type CloudStorage struct {
	client *storage.Client
	bucket *storage.BucketHandle

	filenameformat string
}

// WithFilenameFormat defines the filename format string with its only parameter being the object key.
// Defaults to `%s.json`
type WithFilenameFormat string

// NewCloudStorage
func NewCloudStorage(bucket string, opts ...Option) (*CloudStorage, error) {
	client, err := storage.NewClient(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("cloud_storage client: %w", err)
	}

	// safety check that bucket exists and we're allowed to do a basic op on it
	_, err = client.Bucket(bucket).Object("nonexistant123").Attrs(context.TODO())
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return nil, fmt.Errorf("init check: %w", err)
	}

	cs := &CloudStorage{client, client.Bucket(bucket), "%s.json"}
	for _, opt := range opts {
		opt.apply(cs)
	}
	return cs, nil
}

func (cs *CloudStorage) Filename(key string) string {
	return fmt.Sprintf(cs.filenameformat, key)
}

func (cs *CloudStorage) WriteFile(ctx context.Context, key string, reader io.Reader) error {
	o := cs.bucket.Object(cs.Filename(key)).
		If(storage.Conditions{DoesNotExist: true})

	writer := o.NewWriter(ctx)
	writer.ContentType = "application/json"

	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		// NOTE (Axel): Close()ing will commit any data written, so only do it in the happy path
		return err
	}
	return nil
}

// Options configures the CloudStorage.
//	WithFilenameFormat
type Option interface {
	apply(*CloudStorage)
}

func (f WithFilenameFormat) apply(cs *CloudStorage) { cs.filenameformat = string(f) }
