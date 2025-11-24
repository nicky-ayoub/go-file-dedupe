package iphash

import (
	"os"
	"path/filepath"
	"testing"
)

// TestGetFileHashMD5bytes checks if the MD5 hashing function works correctly.
func TestGetFileHashMD5bytes(t *testing.T) {
	// Create a temporary file with known content.
	content := []byte("hello world")
	// The known MD5 hash for "hello world"
	expectedHash := "5eb63bbbe01eeed093cb22bb8f5acdc3"

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testfile.txt")
	if err := os.WriteFile(tmpFile, content, 0666); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Test the hashing function
	hashBytes, err := GetFileHashMD5bytes(tmpFile)
	if err != nil {
		t.Fatalf("GetFileHashMD5bytes returned an unexpected error: %v", err)
	}

	hashString := HashToString(hashBytes)
	if hashString != expectedHash {
		t.Errorf("Hash mismatch. Got: %s, Want: %s", hashString, expectedHash)
	}
}

// TestGetFileHashSHA256bytes checks if the SHA256 hashing function works correctly.
func TestGetFileHashSHA256bytes(t *testing.T) {
	// Create a temporary file with known content.
	content := []byte("hello world")
	// The known SHA256 hash for "hello world"
	expectedHash := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testfile.txt")
	if err := os.WriteFile(tmpFile, content, 0666); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Test the hashing function
	hashBytes, err := GetFileHashSHA256bytes(tmpFile)
	if err != nil {
		t.Fatalf("GetFileHashSHA256bytes returned an unexpected error: %v", err)
	}

	hashString := HashToString(hashBytes)
	if hashString != expectedHash {
		t.Errorf("Hash mismatch. Got: %s, Want: %s", hashString, expectedHash)
	}
}

// TestGetFileHashBLAKE3bytes checks if the BLAKE3 hashing function works correctly.
func TestGetFileHashBLAKE3bytes(t *testing.T) {
	// Create a temporary file with known content.
	content := []byte("hello world")
	// The known BLAKE3 hash for "hello world"
	expectedHash := "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24"

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testfile.txt")
	if err := os.WriteFile(tmpFile, content, 0666); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Test the hashing function
	hashBytes, err := GetFileHashBLAKE3bytes(tmpFile)
	if err != nil {
		t.Fatalf("GetFileHashBLAKE3bytes returned an unexpected error: %v", err)
	}

	hashString := HashToString(hashBytes)
	if hashString != expectedHash {
		t.Errorf("Hash mismatch. Got: %s, Want: %s", hashString, expectedHash)
	}
}

// TestGetFileHash_NonExistentFile checks that an error is returned for a file that doesn't exist.
func TestGetFileHash_NonExistentFile(t *testing.T) {
	_, err := GetFileHashMD5bytes("non-existent-file.txt")
	if err == nil {
		t.Fatal("Expected an error for a non-existent file, but got nil")
	}
}

// TestHashToString tests the utility function for converting hash bytes to a string.
func TestHashToString(t *testing.T) {
	// Test with nil
	if str := HashToString(nil); str != "" {
		t.Errorf("Expected empty string for nil input, got %s", str)
	}

	// Test with a known value
	hashBytes := []byte{0x5e, 0xb6, 0x3b, 0xbb}
	expectedStr := "5eb63bbb"
	if str := HashToString(hashBytes); str != expectedStr {
		t.Errorf("Expected %s for known bytes, got %s", expectedStr, str)
	}
}
