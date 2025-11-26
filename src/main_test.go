package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"me/go-file-dedupe/iphash"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestDeduplicator_FindAndReportDuplicates tests the core logic of finding duplicates
// and verifying the report output.
func TestDeduplicator_FindAndReportDuplicates(t *testing.T) { // 1. Setup: Create a buffer to capture output and a Deduplicator instance.
	// 1. Setup: Create a buffer to capture output and a Deduplicator instance.
	var out bytes.Buffer
	// The hashFunc and rootDir are not critical for this specific test.
	deduper := NewDeduplicator("/test/root", nil, &out)

	// Mock os.Stat to prevent file system access in this unit test
	originalOsStat := osStat
	osStat = func(name string) (os.FileInfo, error) {
		// Return a mock FileInfo object. The size is arbitrary.
		// We use reflect.TypeOf to get a concrete type that implements os.FileInfo
		return &mockFileInfo{name: name, size: 123}, nil
	}
	// Restore the original function at the end of the test
	defer func() { osStat = originalOsStat }()

	// 2. Manually populate the fileMap to simulate the result of a file scan.
	// Hash for "alpha" is 99c7a8d0b733ea40463b47934042799f
	// Hash for "beta" is 5d41402abc4b2a76b9719d911017c592
	hashAlpha := iphash.HashBytes{0x99, 0xc7, 0xa8, 0xd0, 0xb7, 0x33, 0xea, 0x40, 0x46, 0x3b, 0x47, 0x93, 0x40, 0x42, 0x79, 0x9f}
	hashBeta := iphash.HashBytes{0x5d, 0x41, 0x40, 0x2a, 0xbc, 0x4b, 0x2a, 0x76, 0xb9, 0x71, 0x9d, 0x91, 0x10, 0x17, 0xc5, 0x92}

	deduper.fileMap = map[string]iphash.HashBytes{
		"/test/root/file1.txt":     hashAlpha, // Original
		"/test/root/unique.txt":    hashBeta,  // Unique file
		"/test/root/sub/file2.txt": hashAlpha, // Duplicate of file1
	}
	deduper.discoveredPaths = []string{"/test/root/sub"} // Ensure this is set for reportSummary

	// 3. Run the methods to be tested.
	deduper.findDuplicates()
	deduper.reportDuplicates()
	deduper.reportSummary()

	// 4. Assertions: Check the internal state and the output.

	// Check internal state: fileByteMapDups should contain one entry.
	if len(deduper.fileByteMapDups) != 1 {
		t.Errorf("Expected 1 entry in fileByteMapDups, but got %d", len(deduper.fileByteMapDups))
	}

	// Check that the correct duplicate was found.
	hashAlphaString := "99c7a8d0b733ea40463b47934042799f"
	dups, ok := deduper.fileByteMapDups[hashAlphaString]
	if !ok {
		t.Fatalf("Expected to find duplicates for hash %s, but none were found", hashAlphaString)
	}
	if len(dups) != 2 {
		t.Fatalf("Expected 2 file paths for the duplicate hash, got %d", len(dups))
	}

	// Sort the slice to make the test deterministic, regardless of map iteration order.
	sort.Strings(dups)

	expectedDups := []string{"/test/root/file1.txt", "/test/root/sub/file2.txt"} // Sort this as well for comparison
	sort.Strings(expectedDups)
	if !reflect.DeepEqual(dups, expectedDups) {
		t.Errorf("Duplicate list is incorrect. Got: %v, Want: %v", dups, expectedDups)
	}

	// Check the output written to the buffer.
	output := out.String()

	// Check for the new duplicates report header.
	if !strings.Contains(output, "Duplicate File Report") {
		t.Error("Output is missing the duplicates report header.")
	}

	// Check for the new, formatted output components.
	if !strings.Contains(output, "Hash: 99c7a8d0b733ea40463b47934042799f") ||
		!strings.Contains(output, "Original File: /test/root/file1.txt (Size: 123 bytes)") ||
		!strings.Contains(output, "1. /test/root/sub/file2.txt") {
		t.Errorf("Output is missing the correct new duplicate line components.\nGot: %s", output)
	}

	// Check for the summary report.
	if !strings.Contains(output, "3 Files scanned and hashed.") {
		t.Error("Output is missing the correct file count in the summary.")
	}
	if !strings.Contains(output, "2 unique file content hashes found.") {
		t.Error("Output is missing the correct unique hash count in the summary.")
	}
}

// mockFileInfo is a simple struct to mock os.FileInfo for testing.
type mockFileInfo struct {
	name string
	size int64
}

func (m *mockFileInfo) Name() string       { return m.name }
func (m *mockFileInfo) Size() int64        { return m.size }
func (m *mockFileInfo) Mode() os.FileMode  { return 0 }
func (m *mockFileInfo) ModTime() time.Time { return time.Time{} }
func (m *mockFileInfo) IsDir() bool        { return false }
func (m *mockFileInfo) Sys() interface{}   { return nil }

// setupTestDir creates a temporary directory structure for integration testing.
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

// mockHashFunc is a simple, fast hashing function for testing purposes.
func mockHashFunc(filePath string) (iphash.HashBytes, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	hash := md5.Sum(content)
	return hash[:], nil
}

// TestDeduplicator_Run_Integration tests the full Run method against a temporary filesystem.
func TestDeduplicator_Run_Integration(t *testing.T) {
	// 1. Setup
	rootDir := setupTestDir(t)
	var out bytes.Buffer
	deduper := NewDeduplicator(rootDir, mockHashFunc, &out)

	// 2. Execute the Run method
	err := deduper.Run(context.Background(), 2)
	if err != nil {
		t.Fatalf("Deduplicator.Run() returned an unexpected error: %v", err)
	}

	// 3. Assert the final state of the Deduplicator
	if found := deduper.filesFoundCount.Load(); found != 3 {
		t.Errorf("Expected filesFoundCount to be 3, got %d", found)
	}
	if hashed := deduper.filesHashedCount.Load(); hashed != 3 {
		t.Errorf("Expected filesHashedCount to be 3, got %d", hashed)
	}

	// The number of duplicate *hashes* is the length of the fileByteMapDups map.
	// The test setup has one set of duplicate files.
	if dupes := len(deduper.fileByteMapDups); dupes != 1 {
		t.Errorf("Expected 1 entry in fileByteMapDups, got %d", dupes)
	}

	if len(deduper.fileMap) != 3 {
		t.Errorf("Expected fileMap to contain 3 entries, got %d", len(deduper.fileMap))
	}
	if len(deduper.fileByteMap) != 2 {
		t.Errorf("Expected fileByteMap (unique hashes) to contain 2 entries, got %d", len(deduper.fileByteMap))
	}
}

// TestAreFilesHardLinked tests the functionality of the areFilesHardLinked helper function.
func TestAreFilesHardLinked(t *testing.T) {
	// Setup a temporary directory for our test files
	tempDir := t.TempDir()

	// --- Test Case 1: Two files that ARE hard links of each other ---
	t.Run("AreHardLinked", func(t *testing.T) {
		originalPath := filepath.Join(tempDir, "original.txt")
		hardlinkPath := filepath.Join(tempDir, "hardlink.txt")

		// Create the original file
		if err := os.WriteFile(originalPath, []byte("hello world"), 0666); err != nil {
			t.Fatalf("Failed to create original file: %v", err)
		}

		// Create a hard link to the original file
		if err := os.Link(originalPath, hardlinkPath); err != nil {
			t.Fatalf("Failed to create hard link: %v", err)
		}

		// Check if they are correctly identified as hard links
		linked, err := areFilesHardLinked(originalPath, hardlinkPath)
		if err != nil {
			t.Errorf("areFilesHardLinked returned an unexpected error: %v", err)
		}
		if !linked {
			t.Error("areFilesHardLinked returned false for files that are hard links, expected true")
		}
	})

	// --- Test Case 2: Two separate files with identical content (NOT hard links) ---
	t.Run("AreNotHardLinked", func(t *testing.T) {
		file1Path := filepath.Join(tempDir, "file1.txt")
		file2Path := filepath.Join(tempDir, "file2.txt")

		// Create two separate files
		if err := os.WriteFile(file1Path, []byte("same content"), 0666); err != nil {
			t.Fatalf("Failed to create file1: %v", err)
		}
		if err := os.WriteFile(file2Path, []byte("same content"), 0666); err != nil {
			t.Fatalf("Failed to create file2: %v", err)
		}

		linked, err := areFilesHardLinked(file1Path, file2Path)
		if err != nil {
			t.Errorf("areFilesHardLinked returned an unexpected error: %v", err)
		}
		if linked {
			t.Error("areFilesHardLinked returned true for separate files, expected false")
		}
	})

	// --- Test Case 3: One of the files does not exist ---
	t.Run("FileDoesNotExist", func(t *testing.T) {
		existingPath := filepath.Join(tempDir, "existing.txt")
		if err := os.WriteFile(existingPath, []byte("exists"), 0666); err != nil {
			t.Fatalf("Failed to create existing file: %v", err)
		}

		_, err := areFilesHardLinked(existingPath, "nonexistent.txt")
		if err == nil {
			t.Error("areFilesHardLinked did not return an error for a nonexistent file, but one was expected")
		}
	})
}
