package backends

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRequestContextAndWrapBody(t *testing.T) {
	t.Run("zero timeout returns parent context and leaves body unwrapped", func(t *testing.T) {
		parent := context.Background()
		ctx, cancel := requestContext(parent, 0)
		defer cancel()

		if ctx != parent {
			t.Errorf("expected parent context directly when timeout is 0")
		}
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			t.Errorf("expected no deadline when timeout is 0")
		}

		orig := io.NopCloser(strings.NewReader("hello"))
		wrapped := wrapBodyWithCancel(orig, cancel, false)
		if wrapped != orig {
			t.Errorf("expected body to be returned unchanged when active is false")
		}
	})

	t.Run("positive timeout cancels on body close", func(t *testing.T) {
		ctx, cancel := requestContext(context.Background(), time.Minute)
		wrapped := wrapBodyWithCancel(io.NopCloser(strings.NewReader("hello")), cancel, true)

		if ctx.Err() != nil {
			t.Fatalf("context cancelled before Close: %v", ctx.Err())
		}
		if err := wrapped.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		if ctx.Err() != context.Canceled {
			t.Errorf("ctx.Err() = %v after Close, want context.Canceled", ctx.Err())
		}
	})

	t.Run("nil body with active timeout cancels immediately", func(t *testing.T) {
		ctx, cancel := requestContext(context.Background(), time.Minute)
		if got := wrapBodyWithCancel(nil, cancel, true); got != nil {
			t.Errorf("expected nil body, got %v", got)
		}
		if ctx.Err() != context.Canceled {
			t.Errorf("ctx.Err() = %v, want context.Canceled", ctx.Err())
		}
	})
}

func TestS3GetBodyTimeout(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock)

	outputID := []byte("0123456789abcdef")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("x-amz-meta-outputid", hex.EncodeToString(outputID))
		w.Header().Set("x-amz-meta-size", strconv.Itoa(1024))
		w.Header().Set("x-amz-meta-time", strconv.FormatInt(time.Now().Unix(), 10))
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	defer srv.Close()

	t.Setenv("AWS_ENDPOINT_URL_S3", srv.URL)

	backend, err := NewS3("test-bucket", "", S3Config{
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		UsePathStyle:    true,
		Timeout:         150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewS3 failed: %v", err)
	}
	defer backend.Close()

	_, body, _, _, miss, err := backend.Get([]byte("action"))
	if err != nil || miss || body == nil {
		t.Fatalf("Get = (body %v, miss %v, err %v), want hit with streaming body", body, miss, err)
	}
	defer body.Close()

	start := time.Now()
	_, readErr := io.ReadAll(body)
	if readErr == nil {
		t.Fatalf("expected body read to time out, got nil error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("body read took %v, expected timeout around 150ms", elapsed)
	}
}
