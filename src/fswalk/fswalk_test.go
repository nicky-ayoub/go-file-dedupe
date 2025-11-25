package fswalk

import (
	"context"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"me/go-file-dedupe/iphash"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
)

// mockHasher is a test implementation of HashFunc that computes the MD5 hash of a file's content.
func mockHasher(filePath string) (iphash.HashBytes, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	sum := md5.Sum(data)
	return sum[:], nil
}

func TestDigestAll_Success(t *testing.T) {
	// 1. Setup a temporary directory structure for testing.
	tmpDir, err := ioutil.TempDir("", "test-digestall-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create subdirectories
	subDir1 := filepath.Join(tmpDir, "subdir1")
	subDir2 := filepath.Join(tmpDir, "subdir2")
	if err := os.Mkdir(subDir1, 0755); err != nil {
		t.Fatalf("Failed to create subdir1: %v", err)
	}
	if err := os.Mkdir(subDir2, 0755); err != nil {
		t.Fatalf("Failed to create subdir2: %v", err)
	}

	// Create test files with content
	files := map[string]string{
		filepath.Join(tmpDir, "file1.txt"):    "hello",
		filepath.Join(subDir1, "file2.txt"):   "world",
		filepath.Join(subDir2, "file3.txt"):   "go test",
		filepath.Join(subDir2, "another.log"): "log data",
	}

	expectedHashes := make(map[string]iphash.HashBytes)
	for path, content := range files {
		if err := ioutil.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write test file %s: %v", path, err)
		}
		sum := md5.Sum([]byte(content))
		expectedHashes[path] = sum[:]
	}

	expectedDirs := []string{subDir1, subDir2}
	sort.Strings(expectedDirs)

	// 2. Execute DigestAll
	var filesFound, filesHashed atomic.Uint64
	ctx := context.Background()
	numWorkers := 2

	hashes, dirs, err := DigestAll(ctx, tmpDir, mockHasher, numWorkers, &filesFound, &filesHashed)

	// 3. Assert the results
	if err != nil {
		t.Errorf("DigestAll() returned an unexpected error: %v", err)
	}

	// Check file counts
	if filesFound.Load() != uint64(len(files)) {
		t.Errorf("Expected %d files found, but got %d", len(files), filesFound.Load())
	}
	if filesHashed.Load() != uint64(len(files)) {
		t.Errorf("Expected %d files hashed, but got %d", len(files), filesHashed.Load())
	}

	// Check directory list
	sort.Strings(dirs)
	if !reflect.DeepEqual(dirs, expectedDirs) {
		t.Errorf("Discovered directories do not match expected.\nGot: %v\nWant: %v", dirs, expectedDirs)
	}

	// Check file hashes
	if len(hashes) != len(expectedHashes) {
		t.Errorf("Expected %d hashes, but got %d", len(expectedHashes), len(hashes))
	}

	// To handle potential map inequality in tests due to different key orders,
	// we convert the returned map to a slice of strings for comparison.
	var actualHashesStr []string
	for p, h := range hashes {
		actualHashesStr = append(actualHashesStr, fmt.Sprintf("%s:%x", p, h))
	}
	sort.Strings(actualHashesStr)

	var expectedHashesStr []string
	for p, h := range expectedHashes {
		expectedHashesStr = append(expectedHashesStr, fmt.Sprintf("%s:%x", p, h))
	}
	sort.Strings(expectedHashesStr)

	if !reflect.DeepEqual(actualHashesStr, expectedHashesStr) {
		t.Errorf("File hashes do not match expected.\nGot: %v\nWant: %v", actualHashesStr, expectedHashesStr)
	}

	// Test cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	_, _, err = DigestAll(ctx, tmpDir, mockHasher, numWorkers, &filesFound, &filesHashed)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, but got: %v", err)
	}
}

func TestDigestAll_HashingError(t *testing.T) {
	// 1. Setup a temporary directory with one file that will cause a hashing error.
	tmpDir, err := ioutil.TempDir("", "test-digestall-error-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	goodFilePath := filepath.Join(tmpDir, "goodfile.txt")
	badFilePath := filepath.Join(tmpDir, "badfile.txt")

	if err := ioutil.WriteFile(goodFilePath, []byte("this is fine"), 0644); err != nil {
		t.Fatalf("Failed to write good file: %v", err)
	}
	if err := ioutil.WriteFile(badFilePath, []byte("this will fail"), 0644); err != nil {
		t.Fatalf("Failed to write bad file: %v", err)
	}

	// This hasher will return an error for `badFilePath`.
	mockHasherWithError := func(filePath string) (iphash.HashBytes, error) {
		if filePath == badFilePath {
			return nil, fmt.Errorf("forced hashing error")
		}
		return mockHasher(filePath)
	}

	// 2. Execute DigestAll
	var filesFound, filesHashed atomic.Uint64
	ctx := context.Background()
	numWorkers := 1

	hashes, _, err := DigestAll(ctx, tmpDir, mockHasherWithError, numWorkers, &filesFound, &filesHashed)

	// 3. Assert the results
	// The function itself should not return an error, as it handles hashing errors internally.
	if err != nil {
		t.Errorf("DigestAll() returned an unexpected error: %v", err)
	}

	// Check file counts.
	if filesFound.Load() != 2 {
		t.Errorf("Expected 2 files found, but got %d", filesFound.Load())
	}
	if filesHashed.Load() != 1 {
		t.Errorf("Expected 1 file to be hashed, but got %d", filesHashed.Load())
	}

	// Check the hashes map.
	if len(hashes) != 1 {
		t.Errorf("Expected 1 hash in the map, but got %d", len(hashes))
	}

	// Ensure the bad file is not in the map.
	if _, exists := hashes[badFilePath]; exists {
		t.Errorf("The file that failed to hash should not be in the results map")
	}

	// Ensure the good file IS in the map with the correct hash.
	if _, exists := hashes[goodFilePath]; !exists {
		t.Errorf("The successfully hashed file is missing from the results map")
	}
}
