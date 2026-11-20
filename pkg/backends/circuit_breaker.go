package backends

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxConsecutiveErrors is the default number of consecutive backend
// failures before CircuitBreaker opens for the remainder of the process.
const DefaultMaxConsecutiveErrors = 5

// ErrCircuitOpen is returned by CircuitBreaker once the underlying backend has
// failed repeatedly and been bypassed for the rest of the process.
var ErrCircuitOpen = errors.New("backend circuit breaker open")

// CircuitBreaker wraps a Backend and stops calling it after
// maxConsecutiveErrors failures in a row, returning ErrCircuitOpen for
// subsequent Get and Put calls. A single gobuildcache process lives for one go
// command, so the backend is tried again on the next invocation.
//
// Unlike an error-swallowing wrapper, CircuitBreaker preserves real errors so
// callers (such as AsyncBackendWriter and CacheProg) keep accurate failure and
// byte-transfer statistics. It also wraps returned Get bodies so errors while
// streaming a response body (such as a read timeout) count as backend failures.
type CircuitBreaker struct {
	backend              Backend
	logger               *slog.Logger
	maxConsecutiveErrors int64

	consecutiveErrors atomic.Int64
	totalErrors       atomic.Int64
	tripped           atomic.Bool

	closeOnce sync.Once
	closeErr  error
}

// NewCircuitBreaker wraps backend with a circuit breaker that opens after
// maxConsecutiveErrors consecutive failures. Non-positive thresholds default
// to DefaultMaxConsecutiveErrors.
func NewCircuitBreaker(backend Backend, maxConsecutiveErrors int64, logger *slog.Logger) *CircuitBreaker {
	if maxConsecutiveErrors <= 0 {
		maxConsecutiveErrors = DefaultMaxConsecutiveErrors
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &CircuitBreaker{
		backend:              backend,
		logger:               logger,
		maxConsecutiveErrors: maxConsecutiveErrors,
	}
}

// Tripped reports whether the circuit breaker has opened.
func (cb *CircuitBreaker) Tripped() bool {
	return cb.tripped.Load()
}

// Put forwards to the underlying backend unless the circuit is open.
func (cb *CircuitBreaker) Put(actionID, outputID []byte, body io.Reader, bodySize int64) error {
	if cb.tripped.Load() {
		return ErrCircuitOpen
	}
	err := cb.backend.Put(actionID, outputID, body, bodySize)
	if err != nil {
		cb.recordFailure(err)
		return err
	}
	cb.recordSuccess()
	return nil
}

// Get forwards to the underlying backend unless the circuit is open.
// On a hit, the returned body is wrapped so mid-stream read errors also count
// as backend failures.
func (cb *CircuitBreaker) Get(actionID []byte) ([]byte, io.ReadCloser, int64, *time.Time, bool, error) {
	if cb.tripped.Load() {
		return nil, nil, 0, nil, true, ErrCircuitOpen
	}
	outputID, body, size, putTime, miss, err := cb.backend.Get(actionID)
	if err != nil {
		cb.recordFailure(err)
		return nil, nil, 0, nil, true, err
	}
	if miss || body == nil {
		cb.recordSuccess()
		return outputID, body, size, putTime, miss, nil
	}
	return outputID, &circuitBreakerBody{ReadCloser: body, cb: cb}, size, putTime, false, nil
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.consecutiveErrors.Store(0)
}

func (cb *CircuitBreaker) recordFailure(err error) {
	cb.totalErrors.Add(1)
	if cb.consecutiveErrors.Add(1) >= cb.maxConsecutiveErrors && cb.tripped.CompareAndSwap(false, true) {
		cb.logger.Warn("backend failed repeatedly, using only the local cache for the rest of this build",
			"consecutiveErrors", cb.maxConsecutiveErrors,
			"lastError", err)
	}
}

// Close closes the underlying backend and logs a summary once if any backend
// errors occurred. Close is idempotent so calling it from both CmdClose and a
// deferred cleanup does not double-log.
func (cb *CircuitBreaker) Close() error {
	cb.closeOnce.Do(func() {
		if n := cb.totalErrors.Load(); n > 0 {
			cb.logger.Warn("backend errors occurred during this build",
				"errors", n,
				"localOnly", cb.tripped.Load())
		}
		cb.closeErr = cb.backend.Close()
	})
	return cb.closeErr
}

// Clear passes through to the underlying backend so clear commands never bypass
// or silently skip remote storage.
func (cb *CircuitBreaker) Clear() error {
	return cb.backend.Clear()
}

type circuitBreakerBody struct {
	io.ReadCloser
	cb   *CircuitBreaker
	done atomic.Bool
}

func (b *circuitBreakerBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		if err == io.EOF {
			if b.done.CompareAndSwap(false, true) {
				b.cb.recordSuccess()
			}
		} else if b.done.CompareAndSwap(false, true) {
			b.cb.recordFailure(err)
		}
	}
	return n, err
}

func (b *circuitBreakerBody) Close() error {
	err := b.ReadCloser.Close()
	if b.done.CompareAndSwap(false, true) {
		b.cb.recordSuccess()
	}
	return err
}
