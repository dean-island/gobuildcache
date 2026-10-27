package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// localCache manages the local disk cache where Go build tools access cached files.
// It handles writing, reading, and metadata management for cached entries.
type localCache struct {
	cacheDir string // Absolute path to cache directory
	logger   *slog.Logger
}

// localCacheMetadata holds metadata for a cached entry.
type localCacheMetadata struct {
	OutputID []byte
	Size     int64
	PutTime  time.Time
}

// newLocalCache creates a new local cache instance.
// cacheDir is the directory where cached files will be stored.
func newLocalCache(cacheDir string, logger *slog.Logger) (*localCache, error) {
	// Ensure cache directory exists
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Convert to absolute path once at initialization
	// This avoids repeated filepath.Abs() calls later
	absCacheDir, err := filepath.Abs(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Precreate all 256 subdirectories (00-ff) in parallel to avoid syscalls during writes.
	var (
		wg      sync.WaitGroup
		errChan = make(chan error, 256)
	)
	for i := range 256 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var (
				subdir     = fmt.Sprintf("%02x", i)
				subdirPath = filepath.Join(absCacheDir, subdir)
			)
			if err := os.MkdirAll(subdirPath, 0755); err != nil {
				errChan <- fmt.Errorf("failed to create subdirectory %s: %w", subdir, err)
			}
		}()
	}

	wg.Wait()
	close(errChan)

	// Check if any errors occurred.
	if err := <-errChan; err != nil {
		return nil, err
	}

	return &localCache{
		cacheDir: absCacheDir,
		logger:   logger,
	}, nil
}

// writeMetadata writes metadata for a cache entry.
func (lc *localCache) writeMetadata(actionID []byte, meta localCacheMetadata) error {
	metaPath := lc.metadataPath(actionID)

	// Format: outputID:hex\nsize:num\ntime:unix\n
	content := fmt.Sprintf("outputID:%s\nsize:%d\ntime:%d\n",
		hex.EncodeToString(meta.OutputID),
		meta.Size,
		meta.PutTime.Unix())

	if err := writeFileAtomically(metaPath, strings.NewReader(content)); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// writeFileAtomically writes body to path through a temporary file in the same directory
// and renames it into place, so readers never observe a partial file.
//
// The temporary file has a unique name: several gobuildcache processes can write the
// same action ID at once (LOCK_TYPE=memory locks only within one process), and a shared
// name would let one writer truncate, rename away, or delete another writer's file.
func writeFileAtomically(path string, body io.Reader) error {
	tmpFile, createErr := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if createErr != nil {
		return fmt.Errorf("failed to create temp file: %w", createErr)
	}
	defer os.Remove(tmpFile.Name()) // No-op once the rename has succeeded.

	// os.CreateTemp creates the file with mode 0600; keep the mode cached files had before.
	_, copyErr := io.Copy(tmpFile, body)
	if writeErr := errors.Join(copyErr, tmpFile.Chmod(0644), tmpFile.Close()); writeErr != nil {
		return fmt.Errorf("failed to write temp file: %w", writeErr)
	}

	if renameErr := os.Rename(tmpFile.Name(), path); renameErr != nil {
		return fmt.Errorf("failed to rename temp file: %w", renameErr)
	}
	return nil
}

// readMetadata reads metadata for a cache entry.
// Returns an error if metadata doesn't exist or is corrupted.
func (lc *localCache) readMetadata(actionID []byte) (*localCacheMetadata, error) {
	metaPath := lc.metadataPath(actionID)

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var outputIDHex string
	var size int64
	var putTimeUnix int64

	// Parse each line
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "outputID:") {
			fmt.Sscanf(line, "outputID:%s", &outputIDHex)
		} else if strings.HasPrefix(line, "size:") {
			fmt.Sscanf(line, "size:%d", &size)
		} else if strings.HasPrefix(line, "time:") {
			fmt.Sscanf(line, "time:%d", &putTimeUnix)
		}
	}

	if outputIDHex == "" {
		return nil, fmt.Errorf("metadata missing outputID field")
	}

	outputID, err := hex.DecodeString(outputIDHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode outputID: %w", err)
	}

	return &localCacheMetadata{
		OutputID: outputID,
		Size:     size,
		PutTime:  time.Unix(putTimeUnix, 0),
	}, nil
}

// Write atomically writes the output identified by outputID from a reader to the local cache.
// Returns the absolute path to the cached file.
func (lc *localCache) write(outputID []byte, size int64, body io.Reader) (string, error) {
	diskPath := lc.outputIDToPath(outputID)

	// Outputs are content-addressed and only ever renamed into place whole, so an
	// existing file of the right size already holds these bytes.
	if info, statErr := os.Stat(diskPath); statErr == nil && info.Size() == size {
		return diskPath, nil
	}

	if err := writeFileAtomically(diskPath, body); err != nil {
		return "", fmt.Errorf("failed to write cache file: %w", err)
	}

	// diskPath is already absolute (cacheDir is absolute)
	return diskPath, nil
}

// WriteWithMetadata writes data and metadata to the local cache.
// Returns the absolute path to the cached file.
//
// The data is stored under meta.OutputID and the metadata under actionID, and the data
// is written first. Concurrent writers of one action ID with different outputs therefore
// never leave metadata pointing at another writer's data: whichever metadata wins
// references an output file that was complete before the metadata was written.
func (lc *localCache) writeWithMetadata(actionID []byte, body io.Reader, meta localCacheMetadata) (string, error) {
	if len(meta.OutputID) == 0 {
		return "", fmt.Errorf("missing output ID for action ID %s", hex.EncodeToString(actionID))
	}

	// Write data
	diskPath, err := lc.write(meta.OutputID, meta.Size, body)
	if err != nil {
		return "", err
	}

	// Write metadata
	if err := lc.writeMetadata(actionID, meta); err != nil {
		lc.logger.Warn("failed to write local cache metadata",
			"actionID", hex.EncodeToString(actionID),
			"error", err)
		// Continue - data is cached, just missing metadata
	}

	return diskPath, nil
}

// Check checks if an entry exists in the local cache and returns its metadata.
// Returns nil if not found, and logs a warning if metadata is corrupted or the
// output file it references is missing or has the wrong size.
func (lc *localCache) check(actionID []byte) *localCacheMetadata {
	meta, err := lc.readMetadata(actionID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No metadata exists - this is a cache miss
			return nil
		}

		lc.logger.Warn(
			"failed to read local cache metadata",
			"actionID", hex.EncodeToString(actionID),
			"error", err,
		)
		return nil
	}

	// The output file must be checked too: metadata written before outputs were stored
	// by output ID references a file that doesn't exist, and Go rejects a hit whose file
	// is missing. Treating it as a miss lets the entry be refetched and rewritten.
	info, statErr := os.Stat(lc.outputIDToPath(meta.OutputID))
	if statErr != nil || info.Size() != meta.Size {
		if statErr == nil || !errors.Is(statErr, os.ErrNotExist) {
			lc.logger.Warn(
				"local cache output file does not match metadata",
				"actionID", hex.EncodeToString(actionID),
				"outputID", hex.EncodeToString(meta.OutputID),
				"error", statErr,
			)
		}
		return nil
	}

	return meta
}

// actionIDToPath converts an actionID to the base path of its local cache entry.
// Files are organized into 256 subdirectories (00-ff) based on the first byte
// of the action ID, similar to Go's build cache structure.
func (lc *localCache) actionIDToPath(actionID []byte) string {
	hexActionID := hex.EncodeToString(actionID)
	// Use first two hex characters (first byte) of action ID as subdirectory name
	subdir := hexActionID[:2]
	hexID := fileFormatVersion + hexActionID
	return filepath.Join(lc.cacheDir, subdir, hexID)
}

// outputIDToPath converts an outputID to the path of its output file, using the
// same subdirectory layout as actionIDToPath. The "-d" suffix keeps output files
// apart from metadata files and from data files written under action IDs by
// earlier versions.
func (lc *localCache) outputIDToPath(outputID []byte) string {
	return lc.actionIDToPath(outputID) + "-d"
}

// metadataPath returns the path to the metadata file for an actionID.
func (lc *localCache) metadataPath(actionID []byte) string {
	return lc.actionIDToPath(actionID) + ".meta"
}

// GetPath returns the absolute path for an outputID in the local cache.
// Does not check if the file actually exists.
func (lc *localCache) getPath(outputID []byte) string {
	// Since cacheDir is already absolute, the path is already absolute
	return lc.outputIDToPath(outputID)
}
