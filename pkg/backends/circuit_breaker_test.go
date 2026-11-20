package backends

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scriptedBackend struct {
	fail    atomic.Bool
	miss    atomic.Bool
	bodyErr atomic.Value // stores error
	gets    atomic.Int64
	puts    atomic.Int64
	closes  atomic.Int64
	clears  atomic.Int64
}

type failingReadCloser struct {
	err error
}

func (f *failingReadCloser) Read([]byte) (int, error) { return 0, f.err }
func (f *failingReadCloser) Close() error             { return nil }

func (b *scriptedBackend) Put(actionID, outputID []byte, body io.Reader, bodySize int64) error {
	b.puts.Add(1)
	if b.fail.Load() {
		return errors.New("put failed")
	}
	return nil
}

func (b *scriptedBackend) Get(actionID []byte) ([]byte, io.ReadCloser, int64, *time.Time, bool, error) {
	b.gets.Add(1)
	if b.fail.Load() {
		return nil, nil, 0, nil, true, errors.New("get failed")
	}
	if b.miss.Load() {
		return nil, nil, 0, nil, true, nil
	}
	now := time.Now()
	if v := b.bodyErr.Load(); v != nil {
		if err, ok := v.(error); ok && err != nil {
			return []byte("out"), &failingReadCloser{err: err}, 1, &now, false, nil
		}
	}
	return []byte("out"), io.NopCloser(strings.NewReader("x")), 1, &now, false, nil
}

func (b *scriptedBackend) Close() error {
	b.closes.Add(1)
	return nil
}

func (b *scriptedBackend) Clear() error {
	b.clears.Add(1)
	if b.fail.Load() {
		return errors.New("clear failed")
	}
	return nil
}

func TestCircuitBreaker(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("preserves real errors before tripping", func(t *testing.T) {
		inner := &scriptedBackend{}
		inner.fail.Store(true)
		cb := NewCircuitBreaker(inner, 5, logger)

		_, body, _, _, _, err := cb.Get([]byte("a"))
		if err == nil || errors.Is(err, ErrCircuitOpen) || body != nil {
			t.Fatalf("Get = (body %v, err %v), want underlying error", body, err)
		}
		if err := cb.Put([]byte("a"), []byte("o"), strings.NewReader("x"), 1); err == nil || errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("Put err = %v, want underlying error", err)
		}
		if cb.Tripped() {
			t.Errorf("Tripped() = true after 2 errors, want false")
		}
	})

	t.Run("trips after consecutive errors and returns ErrCircuitOpen", func(t *testing.T) {
		inner := &scriptedBackend{}
		inner.fail.Store(true)
		cb := NewCircuitBreaker(inner, 3, logger)

		for range 3 {
			_, _, _, _, _, err := cb.Get([]byte("a"))
			if err == nil || errors.Is(err, ErrCircuitOpen) {
				t.Fatalf("pre-trip Get err = %v, want underlying error", err)
			}
		}
		if !cb.Tripped() {
			t.Fatalf("Tripped() = false after 3 consecutive errors, want true")
		}

		inner.fail.Store(false)
		for range 10 {
			_, _, _, _, miss, err := cb.Get([]byte("a"))
			if !errors.Is(err, ErrCircuitOpen) || !miss {
				t.Fatalf("post-trip Get = (miss %v, err %v), want (true, ErrCircuitOpen)", miss, err)
			}
			if err := cb.Put([]byte("a"), []byte("o"), strings.NewReader("x"), 1); !errors.Is(err, ErrCircuitOpen) {
				t.Fatalf("post-trip Put err = %v, want ErrCircuitOpen", err)
			}
		}
		if inner.gets.Load() != 3 || inner.puts.Load() != 0 {
			t.Errorf("inner saw %d gets and %d puts after tripping, want 3 and 0", inner.gets.Load(), inner.puts.Load())
		}
	})

	t.Run("body read errors count as failures and trip the breaker", func(t *testing.T) {
		inner := &scriptedBackend{}
		inner.bodyErr.Store(errors.New("stream timed out"))
		cb := NewCircuitBreaker(inner, 3, logger)

		for i := range 3 {
			_, body, _, _, miss, err := cb.Get([]byte("a"))
			if err != nil || miss || body == nil {
				t.Fatalf("call %d: Get = (body %v, miss %v, err %v), want hit with body", i, body, miss, err)
			}
			if _, readErr := io.ReadAll(body); readErr == nil {
				t.Fatalf("call %d: expected body read error, got nil", i)
			}
			_ = body.Close()
		}

		if !cb.Tripped() {
			t.Fatalf("Tripped() = false after 3 body read errors, want true")
		}
		_, _, _, _, _, err := cb.Get([]byte("a"))
		if !errors.Is(err, ErrCircuitOpen) {
			t.Errorf("post-trip Get err = %v, want ErrCircuitOpen", err)
		}
	})

	t.Run("successes reset consecutive error count", func(t *testing.T) {
		inner := &scriptedBackend{}
		cb := NewCircuitBreaker(inner, 3, logger)

		for range 5 {
			inner.fail.Store(true)
			_, _, _, _, _, _ = cb.Get([]byte("a"))
			_, _, _, _, _, _ = cb.Get([]byte("a"))

			inner.fail.Store(false)
			_, body, _, _, miss, err := cb.Get([]byte("a"))
			if err != nil || miss {
				t.Fatalf("expected hit, got miss=%v err=%v", miss, err)
			}
			if _, err := io.ReadAll(body); err != nil {
				t.Fatalf("reading body: %v", err)
			}
			_ = body.Close()
		}

		if cb.Tripped() {
			t.Errorf("Tripped() = true, want false")
		}
		if inner.gets.Load() != 15 {
			t.Errorf("inner.gets = %d, want 15", inner.gets.Load())
		}
	})

	t.Run("cache miss resets consecutive error count", func(t *testing.T) {
		inner := &scriptedBackend{}
		cb := NewCircuitBreaker(inner, 3, logger)

		inner.fail.Store(true)
		_, _, _, _, _, _ = cb.Get([]byte("a"))
		_, _, _, _, _, _ = cb.Get([]byte("a"))

		inner.fail.Store(false)
		inner.miss.Store(true)
		_, _, _, _, miss, err := cb.Get([]byte("a"))
		if err != nil || !miss {
			t.Fatalf("expected clean miss, got miss=%v err=%v", miss, err)
		}

		inner.miss.Store(false)
		inner.fail.Store(true)
		_, _, _, _, _, _ = cb.Get([]byte("a"))
		_, _, _, _, _, _ = cb.Get([]byte("a"))
		if cb.Tripped() {
			t.Errorf("Tripped() = true, want false because miss should reset consecutive errors")
		}
	})

	t.Run("close is idempotent and logs summary once", func(t *testing.T) {
		var logBuf bytes.Buffer
		bufLogger := slog.New(slog.NewTextHandler(&logBuf, nil))
		inner := &scriptedBackend{}
		inner.fail.Store(true)
		cb := NewCircuitBreaker(inner, 5, bufLogger)

		_, _, _, _, _, _ = cb.Get([]byte("a"))

		if err := cb.Close(); err != nil {
			t.Fatalf("first Close err = %v", err)
		}
		if err := cb.Close(); err != nil {
			t.Fatalf("second Close err = %v", err)
		}
		if inner.closes.Load() != 1 {
			t.Errorf("inner.closes = %d, want 1", inner.closes.Load())
		}
		if count := strings.Count(logBuf.String(), "backend errors occurred during this build"); count != 1 {
			t.Errorf("summary logged %d times, want 1:\n%s", count, logBuf.String())
		}
	})

	t.Run("clear passes through even after tripping", func(t *testing.T) {
		inner := &scriptedBackend{}
		inner.fail.Store(true)
		cb := NewCircuitBreaker(inner, 1, logger)

		_, _, _, _, _, _ = cb.Get([]byte("a"))
		if !cb.Tripped() {
			t.Fatalf("expected breaker to trip")
		}
		if err := cb.Clear(); err == nil || errors.Is(err, ErrCircuitOpen) {
			t.Errorf("Clear err = %v, want underlying error", err)
		}
		if inner.clears.Load() != 1 {
			t.Errorf("inner.clears = %d, want 1", inner.clears.Load())
		}
	})

	t.Run("concurrent requests are race-free", func(t *testing.T) {
		inner := &scriptedBackend{}
		inner.fail.Store(true)
		cb := NewCircuitBreaker(inner, 5, logger)

		var wg sync.WaitGroup
		for range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, body, _, _, _, _ := cb.Get([]byte("a"))
				if body != nil {
					_, _ = io.ReadAll(body)
					_ = body.Close()
				}
				_ = cb.Put([]byte("a"), []byte("o"), strings.NewReader("x"), 1)
			}()
		}
		wg.Wait()
		if !cb.Tripped() {
			t.Errorf("expected breaker to trip under concurrent failures")
		}
	})
}
