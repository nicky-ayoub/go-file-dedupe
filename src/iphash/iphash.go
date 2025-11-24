// /home/nicky/src/go/go-file-dedupe/src/iphash/iphash.go
package iphash

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt" // Import the hash interface
	"io"  // Import the io package for io.Copy
	"os"
)

// HashBytes remains the same type alias for the MD5 fixed-size array
type HashBytes []byte

// GetFileHashBytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashMD5bytes(path string) (HashBytes, error) {
	var result HashBytes // Pre-allocate the result array

	file, err := os.Open(path)
	if err != nil {
		// Add context to the error, wrap original error
		return result, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close() // Ensure file is closed

	// Create a new MD5 hasher. md5.New() returns a hash.Hash interface.
	hasher := md5.New()

	// io.Copy efficiently copies data from the file (Reader) to the hasher (Writer)
	// using an internal buffer. This avoids loading the whole file.
	if _, err := io.Copy(hasher, file); err != nil {
		return result, fmt.Errorf("failed to hash file %s: %w", path, err)
	}

	// Get the final hash sum. hasher.Sum(nil) appends the hash to a nil slice.
	hashSlice := hasher.Sum(nil)

	return hashSlice, nil
}

// GetFileHashBytes calculates the MD5 hash of a file efficiently using streaming I/O.
// This version avoids loading the entire file into memory.
func GetFileHashSHA256bytes(path string) (HashBytes, error) {
	var result HashBytes // Pre-allocate the result array

	file, err := os.Open(path)
	if err != nil {
		// Add context to the error, wrap original error
		return result, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close() // Ensure file is closed

	hasher := sha256.New()

	// io.Copy efficiently copies data from the file (Reader) to the hasher (Writer)
	// using an internal buffer. This avoids loading the whole file.
	if _, err := io.Copy(hasher, file); err != nil {
		return result, fmt.Errorf("failed to hash file %s: %w", path, err)
	}

	// Get the final hash sum. hasher.Sum(nil) appends the hash to a nil slice.
	hashSlice := hasher.Sum(nil)

	return hashSlice, nil
}

// GetFileHashBytesOriginal - Keeping the old implementation for reference if needed
// func GetFileHashBytesOriginal(path string) (HashBytes, error) {
// 	data, err := os.ReadFile(path) // Inefficient part
// 	if err != nil {
// 		return HashBytes{}, fmt.Errorf("could not read digest: %w", err) // Use %w for wrapping
// 	}
// 	return md5.Sum(data), nil
// }

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
