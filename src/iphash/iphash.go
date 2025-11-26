// Package iphash provides functions to compute file hashes using different algorithms.
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

	"github.com/zeebo/blake3"
)

// bufferPool is used to reuse buffers for file hashing to reduce memory allocations.
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate a 64KB buffer.
		buffer := make([]byte, 65536)
		return &buffer
	},
}

// Hasher pools for different algorithms to reduce allocations.
var (
	md5Pool = sync.Pool{
		New: func() interface{} { return md5.New() },
	}
	sha256Pool = sync.Pool{
		New: func() interface{} { return sha256.New() },
	}
	blake3Pool = sync.Pool{
		New: func() interface{} { return blake3.New() },
	}
)

// HashBytes remains the same type alias for the MD5 fixed-size array
type HashBytes []byte

// GetFileHashMD5bytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashMD5bytes(path string) (HashBytes, error) {
	h := md5Pool.Get().(hash.Hash)
	defer md5Pool.Put(h)
	return getFileHash(path, h)
}

// GetFileHashSHA256bytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashSHA256bytes(path string) (HashBytes, error) {
	h := sha256Pool.Get().(hash.Hash)
	defer sha256Pool.Put(h)
	return getFileHash(path, h)
}

// GetFileHashBLAKE3bytes calculates the BLAKE3 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashBLAKE3bytes(path string) (HashBytes, error) {
	h := blake3Pool.Get().(hash.Hash)
	defer blake3Pool.Put(h)
	return getFileHash(path, h)
}

// GetReaderHashMD5bytes calculates the MD5 hash from an io.Reader.
func GetReaderHashMD5bytes(r io.Reader) (HashBytes, error) {
	h := md5Pool.Get().(hash.Hash)
	defer md5Pool.Put(h)
	return getReaderHash(r, h)
}

// getFileHash is an internal helper that computes the hash of a file using a provided hash.Hash implementation.
// It uses a custom buffer for potentially faster I/O.
func getFileHash(path string, hasher hash.Hash) (HashBytes, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close()

	return getReaderHash(file, hasher)
}

// getReaderHash is an internal helper that computes the hash from a reader.
func getReaderHash(r io.Reader, hasher hash.Hash) (HashBytes, error) {
	hasher.Reset() // Ensure the hasher from the pool is in a clean state.

	// Get a buffer from the pool and ensure it's put back.
	bufferPtr := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(bufferPtr)
	buffer := *bufferPtr

	// Use the buffer from the pool for the I/O operation.
	if _, err := io.CopyBuffer(hasher, r, buffer); err != nil {
		return nil, fmt.Errorf("failed to read from reader for hashing: %w", err)
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

// --- Factory Pattern ---

// Algorithm defines a supported hash algorithm.
type Algorithm string

// Constants for the supported hashing algorithms.
const (
	BLAKE3 Algorithm = "blake3"
	SHA256 Algorithm = "sha256"
	MD5    Algorithm = "md5"
)

// NewHasher is a factory function that returns a hashing function based on the specified algorithm.
// This encapsulates the logic of choosing a hash implementation.
func NewHasher(algo Algorithm) (func(string) (HashBytes, error), error) {
	switch algo {
	case BLAKE3:
		return GetFileHashBLAKE3bytes, nil
	case SHA256:
		return GetFileHashSHA256bytes, nil
	case MD5:
		return GetFileHashMD5bytes, nil
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s", algo)
	}
}
