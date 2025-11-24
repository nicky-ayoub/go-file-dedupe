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

	"github.com/zeebo/blake3"
)

// HashBytes remains the same type alias for the MD5 fixed-size array
type HashBytes []byte

// GetFileHashBytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashMD5bytes(path string) (HashBytes, error) {
	return getFileHash(path, md5.New())
}

// GetFileHashSHA256bytes calculates the SHA256 hash of a file efficiently using streaming I/O.
func GetFileHashSHA256bytes(path string) (HashBytes, error) {
	return getFileHash(path, sha256.New())
}

// GetFileHashBLAKE3bytes calculates the BLAKE3 hash of a file efficiently using streaming I/O.
func GetFileHashBLAKE3bytes(path string) (HashBytes, error) {
	return getFileHash(path, blake3.New())
}

// getFileHash is a generic helper that computes the hash of a file using any provided hash.Hash implementation.
func getFileHash(path string, hasher hash.Hash) (HashBytes, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close() // Ensure file is closed

	// io.Copy efficiently copies data from the file (Reader) to the hasher (Writer)
	if _, err := io.Copy(hasher, file); err != nil {
		return nil, fmt.Errorf("failed to hash file %s: %w", path, err)
	}

	// Get the final hash sum. hasher.Sum(nil) appends the hash to a nil slice.
	return hasher.Sum(nil), nil
}

// GetFileHash remains the same, it now benefits from the efficient GetFileHashBytes
func GetFileHash(path string) (string, error) {
	data, err := GetFileHashMD5bytes(path) // Calls the new efficient version
	if err != nil {
		// Wrap the error from GetFileHashBytes
		return "", fmt.Errorf("could not execute digest for %s: %w", path, err)
	}
	// Pass the array directly, HashToString handles the slicing
	return HashToString(data), nil
}
func GetFileHashSHA256(path string) (string, error) {
	data, err := GetFileHashSHA256bytes(path) // Calls the new efficient version
	if err != nil {
		// Wrap the error from GetFileHashBytes
		return "", fmt.Errorf("could not execute digest for %s: %w", path, err)
	}
	// Pass the array directly, HashToString handles the slicing
	return HashToString(data), nil
}

// HashToString remains the same. Note: Original didn't return error, keeping it that way.
func HashToString(code HashBytes) string {
	if code == nil {
		return "" // Or return an error if a nil hash is unexpected
	}
	return hex.EncodeToString(code) // Slice the array to pass to EncodeToString
}
