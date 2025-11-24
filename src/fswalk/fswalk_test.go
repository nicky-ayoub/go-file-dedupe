package fswalk

import (
	"context"
	"crypto/md5"
	"me/go-file-dedupe/iphash"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// mockHashFunc is a simple, fast hashing function for testing purposes.
func mockHashFunc(filePath string) (iphash.HashBytes, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	hash := md5.Sum(content)
	return hash[:], nil
}

// setupTestDir creates a temporary directory structure for testing.
// root/
//   - file1.txt (content: "alpha")
//   - file2.txt (content: "beta")
//   - subdir/
//   - file3.txt (content: "alpha") -> duplicate of file1
func setupTestDir(t *testing.T) string {
	t.Helper()
	rootDir := t.TempDir()

	// Create files
	os.WriteFile(filepath.Join(rootDir, "file1.txt"), []byte("alpha"), 0666)
	os.WriteFile(filepath.Join(rootDir, "file2.txt"), []byte("beta"), 0666)

	// Create subdirectory and another file
	subDir := filepath.Join(rootDir, "subdir")
	os.Mkdir(subDir, 0755)
	os.WriteFile(filepath.Join(subDir, "file3.txt"), []byte("alpha"), 0666)

	return rootDir
}

// TestDigestAll_HappyPath tests the normal operation of DigestAll.
func TestDigestAll_HappyPath(t *testing.T) {
	rootDir := setupTestDir(t)
	var filesFound, filesHashed atomic.Uint64

	fileMap, dirs, err := DigestAll(context.Background(), rootDir, mockHashFunc, 2, &filesFound, &filesHashed)

	if err != nil {
		t.Fatalf("DigestAll returned an unexpected error: %v", err)
	}

	// --- Assertions ---
	if filesFound.Load() != 3 {
		t.Errorf("Expected 3 files found, got %d", filesFound.Load())
	}
	if filesHashed.Load() != 3 {
		t.Errorf("Expected 3 files hashed, got %d", filesHashed.Load())
	}
	if len(fileMap) != 3 {
		t.Errorf("Expected fileMap to have 3 entries, got %d", len(fileMap))
	}
	if len(dirs) != 1 {
		t.Errorf("Expected 1 subdirectory discovered, got %d", len(dirs))
	}

	// Check for duplicate hashes
	hash1 := fileMap[filepath.Join(rootDir, "file1.txt")]
	hash3 := fileMap[filepath.Join(rootDir, "subdir", "file3.txt")]
	if iphash.HashToString(hash1) != iphash.HashToString(hash3) {
		t.Error("Expected hashes for file1.txt and file3.txt to be identical")
	}
}

// TestDigestAll_ContextCancellation tests that the function respects context cancellation.
func TestDigestAll_ContextCancellation(t *testing.T) {
	rootDir := setupTestDir(t)
	var filesFound, filesHashed atomic.Uint64

	// Create a context that will be canceled shortly
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// A slow hash function to ensure cancellation happens mid-process
	slowHashFunc := func(filePath string) (iphash.HashBytes, error) {
		time.Sleep(100 * time.Millisecond) // This is longer than the context timeout
		return mockHashFunc(filePath)
	}

	_, _, err := DigestAll(ctx, rootDir, slowHashFunc, 2, &filesFound, &filesHashed)

	if err == nil {
		t.Fatal("Expected a context cancellation error, but got nil")
	}

	if err != context.DeadlineExceeded {
		t.Errorf("Expected error to be context.DeadlineExceeded, got %v", err)
	}
}

// TestDigestAll_EmptyDir tests behavior with an empty directory.
func TestDigestAll_EmptyDir(t *testing.T) {
	rootDir := t.TempDir()
	var filesFound, filesHashed atomic.Uint64

	fileMap, dirs, err := DigestAll(context.Background(), rootDir, mockHashFunc, 2, &filesFound, &filesHashed)

	if err != nil {
		t.Fatalf("DigestAll returned an unexpected error for an empty directory: %v", err)
	}
	if filesFound.Load() != 0 || filesHashed.Load() != 0 || len(fileMap) != 0 || len(dirs) != 0 {
		t.Error("Expected zero results for an empty directory")
	}
}
