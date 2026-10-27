package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLocalCacheConcurrentWritesOfOneActionID writes the same entry from many goroutines at once,
// as separate gobuildcache processes do when nothing serializes them (LOCK_TYPE=memory, or a
// writer that gave up waiting for the file lock). Every write must succeed, the entry must hold
// the complete body, and no temporary file may be left behind.
func TestLocalCacheConcurrentWritesOfOneActionID(t *testing.T) {
	lc, cacheErr := newLocalCache(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if cacheErr != nil {
		t.Fatalf("newLocalCache: %v", cacheErr)
	}

	var (
		actionID = bytes.Repeat([]byte{0xab}, 32)
		body     = bytes.Repeat([]byte("payload "), 1<<16)
		meta     = localCacheMetadata{
			OutputID: bytes.Repeat([]byte{0xcd}, 32),
			Size:     int64(len(body)),
			PutTime:  time.Unix(1700000000, 0),
		}
		writers = 32
		wg      sync.WaitGroup
		errs    = make(chan error, writers)
	)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, writeErr := lc.writeWithMetadata(actionID, bytes.NewReader(body), meta); writeErr != nil {
				errs <- writeErr
			}
		}()
	}
	wg.Wait()
	close(errs)
	for writeErr := range errs {
		t.Errorf("concurrent write failed: %v", writeErr)
	}

	got, readErr := os.ReadFile(lc.getPath(meta.OutputID))
	if readErr != nil {
		t.Fatalf("reading the entry: %v", readErr)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("entry holds %d bytes, want the complete %d-byte body", len(got), len(body))
	}
	if gotMeta := lc.check(actionID); gotMeta == nil || !bytes.Equal(gotMeta.OutputID, meta.OutputID) || gotMeta.Size != meta.Size {
		t.Errorf("metadata = %+v, want %+v", gotMeta, meta)
	}

	for _, dir := range []string{filepath.Dir(lc.getPath(meta.OutputID)), filepath.Dir(lc.metadataPath(actionID))} {
		entries, readDirErr := os.ReadDir(dir)
		if readDirErr != nil {
			t.Fatalf("reading the cache subdirectory: %v", readDirErr)
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".tmp") {
				t.Errorf("temporary file left behind: %s", e.Name())
			}
		}
	}
}

// TestLocalCacheConcurrentWritesOfOneActionIDWithDifferentOutputs writes one action ID from
// many goroutines at once, each with a different output of the same size, as happens when
// unserialized processes build a non-reproducible action. Same-size outputs are the case
// Go's size check cannot catch. Readers running alongside, and the final entry, must
// always see metadata that matches the bytes in the file it points to.
func TestLocalCacheConcurrentWritesOfOneActionIDWithDifferentOutputs(t *testing.T) {
	lc, cacheErr := newLocalCache(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if cacheErr != nil {
		t.Fatalf("newLocalCache: %v", cacheErr)
	}

	var (
		actionID      = bytes.Repeat([]byte{0xab}, 32)
		writers       = 16
		bodyByOutput  = make(map[string][]byte, writers)
		metas         = make([]localCacheMetadata, writers)
		writersDone   = make(chan struct{})
		wg, readersWG sync.WaitGroup
		errs          = make(chan error, 2*writers)
	)
	for i := range writers {
		body := bytes.Repeat([]byte{byte(i)}, 1<<16)
		outputID := sha256.Sum256(body)
		bodyByOutput[string(outputID[:])] = body
		metas[i] = localCacheMetadata{OutputID: outputID[:], Size: int64(len(body)), PutTime: time.Unix(1700000000, 0)}
	}

	verify := func() error {
		gotMeta := lc.check(actionID)
		if gotMeta == nil {
			return nil
		}
		got, readErr := os.ReadFile(lc.getPath(gotMeta.OutputID))
		if readErr != nil {
			return fmt.Errorf("reading the output of a cache hit: %w", readErr)
		}
		if want := bodyByOutput[string(gotMeta.OutputID)]; !bytes.Equal(got, want) {
			return fmt.Errorf("metadata references output %x, but its file holds different bytes", gotMeta.OutputID)
		}
		return nil
	}

	for range 4 {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-writersDone:
					return
				default:
				}
				if verifyErr := verify(); verifyErr != nil {
					errs <- verifyErr
					return
				}
			}
		}()
	}
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, writeErr := lc.writeWithMetadata(actionID, bytes.NewReader(bodyByOutput[string(metas[i].OutputID)]), metas[i]); writeErr != nil {
				errs <- writeErr
			}
		}()
	}
	wg.Wait()
	close(writersDone)
	readersWG.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if lc.check(actionID) == nil {
		t.Fatal("expected a cache hit after the writes")
	}
	if verifyErr := verify(); verifyErr != nil {
		t.Error(verifyErr)
	}
}

// TestLocalCacheIgnoresEntriesWithoutOutputFile checks that metadata whose output file is
// missing, as for entries written before outputs were stored by output ID, is a miss.
func TestLocalCacheIgnoresEntriesWithoutOutputFile(t *testing.T) {
	lc, cacheErr := newLocalCache(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if cacheErr != nil {
		t.Fatalf("newLocalCache: %v", cacheErr)
	}

	var (
		actionID = bytes.Repeat([]byte{0xab}, 32)
		body     = []byte("payload")
		meta     = localCacheMetadata{
			OutputID: bytes.Repeat([]byte{0xcd}, 32),
			Size:     int64(len(body)),
			PutTime:  time.Unix(1700000000, 0),
		}
	)
	// The layout of earlier versions: the data file sits at the action ID's path.
	if writeErr := os.WriteFile(lc.actionIDToPath(actionID), body, 0644); writeErr != nil {
		t.Fatalf("writing the old-layout data file: %v", writeErr)
	}
	if writeErr := lc.writeMetadata(actionID, meta); writeErr != nil {
		t.Fatalf("writeMetadata: %v", writeErr)
	}
	if gotMeta := lc.check(actionID); gotMeta != nil {
		t.Fatalf("check = %+v, want a miss for an entry without an output file", gotMeta)
	}

	if _, writeErr := lc.writeWithMetadata(actionID, bytes.NewReader(body), meta); writeErr != nil {
		t.Fatalf("writeWithMetadata: %v", writeErr)
	}
	if gotMeta := lc.check(actionID); gotMeta == nil {
		t.Fatal("check = nil, want a hit once the entry is rewritten")
	}
}
