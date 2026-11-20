package backends

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSConfig holds GCS-specific configuration for the GCS backend.
type GCSConfig struct {
	// Timeout bounds each GCS request, including reading a GET body. Zero means no limit.
	Timeout time.Duration
}

// GCS implements Backend using Google Cloud Storage.
// This backend only handles GCS operations; local disk caching is handled by server.go.
type GCS struct {
	client  *storage.Client
	bucket  *storage.BucketHandle
	prefix  string
	ctx     context.Context
	timeout time.Duration
}

// NewGCS creates a new GCS-based cache backend.
// bucket is the GCS bucket name where cache files will be stored.
// prefix is an optional prefix for all GCS object names (e.g., "cache/" or "").
func NewGCS(bucket, prefix string, cfg GCSConfig) (*GCS, error) {
	ctx := context.Background()

	// Build client options. WithJSONReads forces the JSON API for downloads
	// (default is XML), required for GCS Anywhere Cache compatibility.
	opts := []option.ClientOption{storage.WithJSONReads()}

	// Support explicit access token via GOBUILDCACHE_GCS_ACCESS_TOKEN.
	// Useful when the GCE metadata server is unreachable (e.g., docker run
	// containers on bridge networks in Cloud Build).
	if t := os.Getenv("GOBUILDCACHE_GCS_ACCESS_TOKEN"); t != "" {
		opts = append(opts, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: t})))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bucketHandle := client.Bucket(bucket)

	backend := &GCS{
		client:  client,
		bucket:  bucketHandle,
		prefix:  prefix,
		ctx:     ctx,
		timeout: cfg.Timeout,
	}

	// Test bucket access by checking if bucket exists
	attrsCtx, cancel := requestContext(ctx, cfg.Timeout)
	defer cancel()
	_, err = bucketHandle.Attrs(attrsCtx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to access GCS bucket %s: %w", bucket, err)
	}

	return backend, nil
}

// Put stores an object in GCS.
func (g *GCS) Put(actionID, outputID []byte, body io.Reader, bodySize int64) error {
	key := g.actionIDToKey(actionID)
	obj := g.bucket.Object(key)

	ctx, cancel := requestContext(g.ctx, g.timeout)
	defer cancel()

	// Create a writer for the object
	writer := obj.NewWriter(ctx)
	defer writer.Close()

	// Set metadata
	writer.Metadata = map[string]string{
		"outputid": hex.EncodeToString(outputID),
		"size":     strconv.FormatInt(bodySize, 10),
		"time":     strconv.FormatInt(time.Now().Unix(), 10),
	}

	// Copy the body to the writer
	if bodySize > 0 && body != nil {
		written, err := io.CopyN(writer, body, bodySize)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to write body to GCS: %w", err)
		}
		if written != bodySize {
			return fmt.Errorf("size mismatch: expected %d, wrote %d", bodySize, written)
		}
	}

	// Close the writer to finalize the upload
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

// Get retrieves an object from GCS.
// Returns the object data as an io.ReadCloser that must be closed by the caller.
func (g *GCS) Get(actionID []byte) ([]byte, io.ReadCloser, int64, *time.Time, bool, error) {
	key := g.actionIDToKey(actionID)
	obj := g.bucket.Object(key)

	ctx, cancel := requestContext(g.ctx, g.timeout)

	// Get object attributes first to check if it exists and get metadata
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		cancel()
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil, 0, nil, true, nil
		}
		return nil, nil, 0, nil, true, fmt.Errorf("failed to get GCS object attrs: %w", err)
	}

	// Parse metadata
	outputIDHex := attrs.Metadata["outputid"]
	sizeStr := attrs.Metadata["size"]
	timeStr := attrs.Metadata["time"]

	outputID, err := hex.DecodeString(outputIDHex)
	if err != nil {
		cancel()
		return nil, nil, 0, nil, true, nil
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		// Fallback to actual object size if metadata is missing
		size = attrs.Size
	}

	var putTime *time.Time
	if timeStr != "" {
		putTimeUnix, err := strconv.ParseInt(timeStr, 10, 64)
		if err == nil {
			t := time.Unix(putTimeUnix, 0)
			putTime = &t
		}
	}
	// Fallback to object creation time if metadata is missing
	if putTime == nil {
		putTime = &attrs.Created
	}

	// Get a reader for the object
	reader, err := obj.NewReader(ctx)
	if err != nil {
		cancel()
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil, 0, nil, true, nil
		}
		return nil, nil, 0, nil, true, fmt.Errorf("failed to get GCS object reader: %w", err)
	}

	// Return the GCS object body as a ReadCloser
	// The caller is responsible for closing it, which also releases the request context
	return outputID, wrapBodyWithCancel(reader, cancel, g.timeout > 0), size, putTime, false, nil
}

// Close performs cleanup operations.
func (g *GCS) Close() error {
	if g.client != nil {
		return g.client.Close()
	}
	return nil
}

// Clear removes all entries from the cache in GCS.
func (g *GCS) Clear() error {
	// List all objects with the prefix
	query := &storage.Query{
		Prefix: g.prefix,
	}

	it := g.bucket.Objects(g.ctx, query)

	// Collect objects to delete (GCS allows up to 100 objects per batch delete)
	var objectsToDelete []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list GCS objects: %w", err)
		}
		objectsToDelete = append(objectsToDelete, attrs.Name)
	}

	if len(objectsToDelete) == 0 {
		return nil
	}

	// Delete objects in batches (GCS allows up to 100 objects per batch)
	batchSize := 100
	for i := 0; i < len(objectsToDelete); i += batchSize {
		end := i + batchSize
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}
		batch := objectsToDelete[i:end]

		// Delete each object in the batch
		for _, objName := range batch {
			obj := g.bucket.Object(objName)
			if err := obj.Delete(g.ctx); err != nil {
				// Continue deleting other objects even if one fails
				// Log error but don't fail the entire operation
				_ = err
			}
		}
	}

	return nil
}

// actionIDToKey converts an actionID to a GCS object name.
func (g *GCS) actionIDToKey(actionID []byte) string {
	hexID := hex.EncodeToString(actionID)
	if g.prefix != "" {
		return g.prefix + hexID
	}
	return hexID
}
