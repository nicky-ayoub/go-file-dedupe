package fswalk

import (
	"context"
	"crypto/md5"
	"me/go-file-dedupe/iphash"
	"os"
	"path/filepath"
	"sort"
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

	duplicates, dirs, err := DigestAll(context.Background(), rootDir, mockHashFunc, 2, &filesFound, &filesHashed)

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
	if len(dirs) != 1 {
		t.Errorf("Expected 1 subdirectory discovered, got %d", len(dirs))
	}

	// Check that the duplicates map is correct.
	if len(duplicates) != 1 {
		t.Fatalf("Expected duplicates map to have 1 entry, got %d", len(duplicates))
	}

	// The MD5 hash for "alpha" is "900150983cd24fb0d6963f7d28e17f72"
	expectedHash := "900150983cd24fb0d6963f7d28e17f72"
	dupPaths, ok := duplicates[expectedHash]
	if !ok {
		t.Fatalf("Expected to find duplicates for hash %s, but none were found", expectedHash)
	}

	sort.Strings(dupPaths) // Sort for deterministic checking.
	if len(dupPaths) != 2 || dupPaths[0] != filepath.Join(rootDir, "file1.txt") || dupPaths[1] != filepath.Join(rootDir, "subdir", "file3.txt") {
		t.Errorf("Incorrect duplicate paths found. Got: %v", dupPaths)
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

	duplicates, dirs, err := DigestAll(context.Background(), rootDir, mockHashFunc, 2, &filesFound, &filesHashed)

	if err != nil {
		t.Fatalf("DigestAll returned an unexpected error for an empty directory: %v", err)
	}
	if filesFound.Load() != 0 || filesHashed.Load() != 0 || len(duplicates) != 0 || len(dirs) != 0 {
		t.Error("Expected zero results for an empty directory")
	}
}

// TestDigestAll_UnreadableDir checks that the walker can gracefully handle
// directories it does not have permission to read.
func TestDigestAll_UnreadableDir(t *testing.T) {
	// 1. Setup a directory structure with one unreadable subdirectory.
	rootDir := t.TempDir()

	// Create a readable file in the root.
	readableFilePath := filepath.Join(rootDir, "readable_file.txt")
	if err := os.WriteFile(readableFilePath, []byte("readable"), 0666); err != nil {
		t.Fatalf("Failed to create readable file: %v", err)
	}

	// Create the directory that will be made unreadable.
	unreadableDir := filepath.Join(rootDir, "unreadable")
	if err := os.Mkdir(unreadableDir, 0755); err != nil {
		t.Fatalf("Failed to create unreadable dir: %v", err)
	}

	// Create a file inside it that should NOT be found.
	secretFilePath := filepath.Join(unreadableDir, "secret.txt")
	if err := os.WriteFile(secretFilePath, []byte("secret"), 0666); err != nil {
		t.Fatalf("Failed to create secret file: %v", err)
	}

	// 2. Make the directory unreadable.
	if err := os.Chmod(unreadableDir, 0000); err != nil {
		t.Skipf("Skipping test: could not make directory unreadable with chmod: %v", err)
	}
	// Defer restoring permissions so the TempDir can be cleaned up successfully.
	defer os.Chmod(unreadableDir, 0755)

	// 3. Run DigestAll and expect it to succeed without error.
	var filesFound, filesHashed atomic.Uint64
	duplicates, _, err := DigestAll(context.Background(), rootDir, mockHashFunc, 2, &filesFound, &filesHashed)

	if err != nil {
		t.Fatalf("DigestAll returned an unexpected error when encountering an unreadable directory: %v", err)
	}

	// 4. Assert that only the readable file was found and hashed.
	if filesFound.Load() != 1 || filesHashed.Load() != 1 {
		t.Errorf("Expected 1 file found and hashed, but got %d found and %d hashed", filesFound.Load(), filesHashed.Load())
	}

	if len(duplicates) != 0 {
		t.Error("The file inside the unreadable directory was incorrectly found.")
	}
}

// TestDigestAll_DeeplyNestedDir verifies that the parallel walker can handle
// deeply nested directory structures.
func TestDigestAll_DeeplyNestedDir(t *testing.T) {
	// 1. Setup a directory structure 4 levels deep.
	// root/
	//   - file_root.txt
	//   - level1/
	//     - file_l1.txt
	//     - level2/
	//       - dup1.txt (content: "duplicate")
	//       - level3/
	//         - file_l3.txt
	//         - level4/
	//           - file_l4.txt
	//           - dup2.txt (content: "duplicate")
	rootDir := t.TempDir()

	// Create files and directories
	if err := os.WriteFile(filepath.Join(rootDir, "file_root.txt"), []byte("root"), 0666); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	level1 := filepath.Join(rootDir, "level1")
	os.Mkdir(level1, 0755)
	os.WriteFile(filepath.Join(level1, "file_l1.txt"), []byte("l1"), 0666)

	level2 := filepath.Join(level1, "level2")
	os.Mkdir(level2, 0755)
	os.WriteFile(filepath.Join(level2, "dup1.txt"), []byte("duplicate"), 0666)

	level3 := filepath.Join(level2, "level3")
	os.Mkdir(level3, 0755)
	os.WriteFile(filepath.Join(level3, "file_l3.txt"), []byte("l3"), 0666)

	level4 := filepath.Join(level3, "level4")
	os.Mkdir(level4, 0755)
	os.WriteFile(filepath.Join(level4, "file_l4.txt"), []byte("l4"), 0666)
	os.WriteFile(filepath.Join(level4, "dup2.txt"), []byte("duplicate"), 0666)

	totalFiles := 6
	totalDirs := 4

	// 2. Run DigestAll. Use a higher number of workers to exercise the parallel logic.
	var filesFound, filesHashed atomic.Uint64
	duplicates, dirs, err := DigestAll(context.Background(), rootDir, mockHashFunc, 4, &filesFound, &filesHashed)

	if err != nil {
		t.Fatalf("DigestAll returned an unexpected error: %v", err)
	}

	// 3. Assertions
	if filesFound.Load() != uint64(totalFiles) {
		t.Errorf("Expected %d files found, got %d", totalFiles, filesFound.Load())
	}
	if filesHashed.Load() != uint64(totalFiles) {
		t.Errorf("Expected %d files hashed, got %d", totalFiles, filesHashed.Load())
	}
	if len(dirs) != totalDirs {
		t.Errorf("Expected %d directories discovered, got %d", totalDirs, len(dirs))
	}

	// Check for the duplicate hashes.
	if len(duplicates) != 1 {
		t.Fatalf("Expected duplicates map to have 1 entry, got %d", len(duplicates))
	}

	// The MD5 hash for "duplicate" is "552822c9c61b582c52387a9ab904b4f2"
	expectedHash := "552822c9c61b582c52387a9ab904b4f2"
	dupPaths, ok := duplicates[expectedHash]
	if !ok {
		t.Fatalf("Expected to find duplicates for hash %s, but none were found", expectedHash)
	}

	sort.Strings(dupPaths) // Sort for deterministic checking.
	if len(dupPaths) != 2 || dupPaths[0] != filepath.Join(level2, "dup1.txt") || dupPaths[1] != filepath.Join(level4, "dup2.txt") {
		t.Errorf("Incorrect duplicate paths found. Got: %v", dupPaths)
	}
}
