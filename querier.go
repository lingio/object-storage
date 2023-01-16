package objectstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

var ErrObjectNotFound = errors.New("object not found")

// CRUDStore defines a rudimentary typesafe Create, Get, Put, Delete datastore
// over a CloudStorage.
// ErrObjectNotFound is returned if an operation is called on a non-existant object.
type CRUDStore[T any] interface {
	Create(context.Context, string, T) error
	Get(context.Context, string) (*T, error)
	Put(context.Context, string, T) error
	Delete(context.Context, string) error
	List(context.Context, string) *storage.ObjectIterator
}

// querier implements the CRUDStore interface.
type querier[T any] struct {
	cs *CloudStorage
}

func NewCRUDStore[T any](cs *CloudStorage) CRUDStore[T] {
	return &querier[T]{cs}
}

// Create
func (q *querier[T]) Create(ctx context.Context, key string, obj T) error {
	data, err := json.Marshal(&obj)
	if err != nil {
		return err
	}
	return q.cs.WriteFile(ctx, key, bytes.NewReader(data))
}

// Get
func (q *querier[T]) Get(ctx context.Context, key string) (*T, error) {
	data, err := q.cs.GetFile(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("Get %s: readall: %w", key, err)
	}

	var obj T
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("Get %s: %w", key, err)
	}

	return &obj, nil
}

// List
func (q *querier[T]) List(ctx context.Context, prefix string) *storage.ObjectIterator {
	return q.cs.bucket.Objects(ctx, &storage.Query{
		Prefix:     prefix,
		Projection: storage.ProjectionNoACL, // skip some metadata to speed up
	})
}

// Put
func (q *querier[T]) Put(ctx context.Context, key string, obj T) error {
	o := q.cs.bucket.Object(q.cs.Filename(key))

	// add compare-and-swap style updating so we don't overwrite with stale read
	attrs, err := o.Attrs(ctx)
	if err == nil {
		o = o.If(storage.Conditions{GenerationMatch: attrs.Generation})
	} else if !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("Put %s: Attrs: %w", key, err)
	}

	writer := o.NewWriter(ctx)
	writer.ContentType = "application/json"

	if data, err := json.Marshal(&obj); err != nil {
		return fmt.Errorf("Put %s: %w", key, err)
	} else if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("Put %s: copy: %w", key, err)
	}
	if err := writer.Close(); err != nil {
		// NOTE (Axel): Close()ing will commit any data written, so only do it in the happy path
		return fmt.Errorf("Put %s: Close: %w", key, err)
	}

	return nil
}

// Delete
func (q *querier[T]) Delete(ctx context.Context, key string) error {
	err := q.cs.bucket.Object(q.cs.Filename(key)).Delete(ctx)
	if err2 := wrapStorageError(err); err2 != nil {
		return fmt.Errorf("Delete %s: %w", key, err2)
	} else if err != nil {
		return fmt.Errorf("Delete %s: %w", key, err)
	}
	return nil
}

func wrapStorageError(err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return &storageError{cause: err, mask: ErrObjectNotFound}
	}
	return err
}

type storageError struct {
	cause error
	mask  error
}

func (s *storageError) Unwrap() error {
	return s.mask
}
func (s *storageError) Is(e error) bool {
	return s.mask == e || s.cause == e
}
func (s *storageError) Error() string {
	return fmt.Sprintf("%s: %s", s.mask.Error(), s.cause.Error())
}
