package backends

import (
	"context"
	"io"
	"time"
)

// DefaultBackendTimeout is the default per-request timeout for remote backend
// operations when -backend-timeout / BACKEND_TIMEOUT is not configured.
const DefaultBackendTimeout = 30 * time.Second

func noopCancel() {}

// requestContext returns a context bounded by timeout. When timeout <= 0, it
// returns parent directly with a no-op cancel function to avoid unnecessary
// allocations.
func requestContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return parent, noopCancel
}

// wrapBodyWithCancel wraps rc so closing it also releases cancel.
// When active is false, rc is returned unchanged. If active is true and rc is
// nil, cancel is invoked immediately so the timer is not leaked.
func wrapBodyWithCancel(rc io.ReadCloser, cancel context.CancelFunc, active bool) io.ReadCloser {
	if !active {
		return rc
	}
	if rc == nil {
		cancel()
		return nil
	}
	return &cancelOnClose{ReadCloser: rc, cancel: cancel}
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
