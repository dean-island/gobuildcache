package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/richardartoul/gobuildcache/pkg/backends"
)

func TestCreateBackendStartupFallback(t *testing.T) {
	// An S3 endpoint that rejects every request, so the bucket check in NewS3 fails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	t.Setenv("AWS_ENDPOINT_URL_S3", srv.URL)
	t.Setenv("GOBUILDCACHE_AWS_REGION", "us-east-1")
	t.Setenv("GOBUILDCACHE_AWS_ACCESS_KEY_ID", "test")
	t.Setenv("GOBUILDCACHE_AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("GOBUILDCACHE_AWS_S3_USE_PATH_STYLE", "true")

	oldType, oldBucket, oldAsync, oldFallback, oldTimeout, oldDebug :=
		backendType, s3Bucket, asyncBackend, backendFallback, backendTimeout, debug
	t.Cleanup(func() {
		backendType, s3Bucket, asyncBackend, backendFallback, backendTimeout, debug =
			oldType, oldBucket, oldAsync, oldFallback, oldTimeout, oldDebug
	})
	backendType, s3Bucket, asyncBackend, backendTimeout, debug = "s3", "missing-bucket", true, 0, false

	t.Run("fails without fallback", func(t *testing.T) {
		backendFallback = false
		if _, err := createBackend(); err == nil {
			t.Fatal("createBackend err = nil, want the bucket check error")
		}
	})

	t.Run("falls back directly to noop without async wrapper", func(t *testing.T) {
		backendFallback = true
		backend, err := createBackend()
		if err != nil {
			t.Fatalf("createBackend err = %v, want nil", err)
		}
		defer backend.Close()

		if _, ok := backend.(*backends.Noop); !ok {
			t.Fatalf("backend is %T, want *backends.Noop directly (no async/circuit-breaker overhead)", backend)
		}
		if _, _, _, _, miss, err := backend.Get([]byte("action")); err != nil || !miss {
			t.Errorf("Get = (miss %v, err %v), want a miss without error", miss, err)
		}
	})

	t.Run("rejects negative backend timeout", func(t *testing.T) {
		backendFallback = true
		backendTimeout = -time.Second
		defer func() { backendTimeout = 0 }()

		if _, err := createBackend(); err == nil {
			t.Fatal("createBackend err = nil for negative timeout, want error")
		}
	})
}

func TestCreateBackendStartupTimeoutFallback(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock)

	// An S3 endpoint that accepts TCP connections and hangs until the client cancels.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	defer srv.Close()

	t.Setenv("AWS_ENDPOINT_URL_S3", srv.URL)
	t.Setenv("GOBUILDCACHE_AWS_REGION", "us-east-1")
	t.Setenv("GOBUILDCACHE_AWS_ACCESS_KEY_ID", "test")
	t.Setenv("GOBUILDCACHE_AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("GOBUILDCACHE_AWS_S3_USE_PATH_STYLE", "true")

	oldType, oldBucket, oldAsync, oldFallback, oldTimeout, oldDebug :=
		backendType, s3Bucket, asyncBackend, backendFallback, backendTimeout, debug
	t.Cleanup(func() {
		backendType, s3Bucket, asyncBackend, backendFallback, backendTimeout, debug =
			oldType, oldBucket, oldAsync, oldFallback, oldTimeout, oldDebug
	})
	backendType, s3Bucket, asyncBackend, backendFallback, backendTimeout, debug =
		"s3", "slow-bucket", true, true, 150*time.Millisecond, false

	start := time.Now()
	backend, err := createBackend()
	if err != nil {
		t.Fatalf("createBackend err = %v, want fallback nil", err)
	}
	defer backend.Close()

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("createBackend took %v, expected timeout around 150ms", elapsed)
	}
	if _, ok := backend.(*backends.Noop); !ok {
		t.Fatalf("backend is %T, want *backends.Noop", backend)
	}
}
