// /home/nicky/src/go/go-file-dedupe/src/iphash/iphash.go
package iphash

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex" // Import the hash interface
	"fmt"
	"hash"
	"io" // Import the io package for io.Copy
	"os"
	"sync"
)

// bufferPool is used to reuse buffers for file hashing to reduce memory allocations.
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate a 64KB buffer.
		buffer := make([]byte, 65536)
		return &buffer
	},
}

// HashBytes remains the same type alias for the MD5 fixed-size array
type HashBytes []byte

// GetFileHashBytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashMD5bytes(path string) (HashBytes, error) {
	return getFileHash(path, md5.New())
}

// GetFileHashBytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashSHA256bytes(path string) (HashBytes, error) {
	return getFileHash(path, sha256.New())
}

// getFileHash is an internal helper that computes the hash of a file using a provided hash.Hash implementation.
// It uses a custom buffer for potentially faster I/O.
func getFileHash(path string, hasher hash.Hash) (HashBytes, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close()

	// Get a buffer from the pool and ensure it's put back.
	bufferPtr := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(bufferPtr)
	buffer := *bufferPtr

	// Use the buffer from the pool for the I/O operation.
	if _, err := io.CopyBuffer(hasher, file, buffer); err != nil {
		return nil, fmt.Errorf("failed to hash file %s: %w", path, err)
	}

	return hasher.Sum(nil), nil
}

// HashToString remains the same. Note: Original didn't return error, keeping it that way.
func HashToString(code HashBytes) string {
	if code == nil {
		return "" // Or return an error if a nil hash is unexpected
	}
	return hex.EncodeToString(code) // Slice the array to pass to EncodeToString
}
