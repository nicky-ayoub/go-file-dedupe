// /home/nicky/src/go/go-file-dedupe/src/fswalk/fswalk.go
package fswalk

import (
	"context"
	"fmt"
	"me/go-file-dedupe/iphash" // Make sure this import path is correct
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// HashFunc defines the signature for functions that can hash a file.
// It matches the signatures of GetFileHashMD5bytes and GetFileHashSHA256bytes.
// Exported so it can be used by the caller (main.go).
type HashFunc func(filePath string) (iphash.HashBytes, error)

// A result is the product of reading and summing a file using MD5.
type result struct {
	path string
	sum  iphash.HashBytes // Use the specific type from iphash
	err  error
}

// digester reads path names from filePaths and sends digests of the corresponding
// files on c until either filePaths or done is closed.
func digester(ctx context.Context, filePaths <-chan string, c chan<- result, hashFile HashFunc) {
	for path := range filePaths {
		//fmt.Println("DEBUG: Digester received path:", path)
		data, err := hashFile(path)
		select {
		case c <- result{path, data, err}:
		case <-ctx.Done():
			return
		}
	}
}

// DigestAll reads all the files in the file tree rooted at root, calculates their MD5 sums in parallel,
// and returns a map from file path to MD5 sum, a slice of discovered directory paths, and any error encountered during the walk.
func DigestAll(
	ctx context.Context,
	root string,
	hasher HashFunc,
	numWorkers int,
	filesFound *atomic.Uint64, // Pointer to counter
	filesHashed *atomic.Uint64, // Pointer to counter
) (map[string]iphash.HashBytes, []string, error) {
	// --- Parallel Directory Traversal ---
	var walkWg sync.WaitGroup
	dirsToWalk := make(chan string, numWorkers) // Buffered channel for directories to walk
	filePaths := make(chan string, numWorkers)  // Channel for discovered file paths
	dirPaths := make(chan string, numWorkers)   // Channel for discovered directory paths

	// Start a pool of directory walkers
	walkWg.Add(1) // Start with 1 for the root directory
	go func() {   // This single goroutine will spawn the workers
		for i := 0; i < numWorkers; i++ {
			go func() {
				for dir := range dirsToWalk {
					entries, err := os.ReadDir(dir)
					if err != nil {
						fmt.Printf("Warning: Error reading directory %s: %v\n", dir, err)
						walkWg.Done() // Decrement counter on error
						continue
					}

					for _, entry := range entries {
						fullPath := filepath.Join(dir, entry.Name())

						if entry.IsDir() {
							select {
							case dirPaths <- fullPath:
							case <-ctx.Done():
								return
							}
							walkWg.Add(1) // Add to the waitgroup before sending to the channel
							select {
							case dirsToWalk <- fullPath:
							case <-ctx.Done():
								walkWg.Done() // Must decrement if we fail to send
								return
							}
						} else if entry.Type().IsRegular() {
							filesFound.Add(1)
							select {
							case filePaths <- fullPath:
							case <-ctx.Done():
								return
							}
						}
					}
					walkWg.Done() // Done with this directory
				}
			}()
		}
	}()

	// Seed the process with the root directory
	dirsToWalk <- root

	// --- Hashing Worker Pool (Digesters) ---
	c := make(chan result)
	var wg sync.WaitGroup

	// Start digesters
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			digester(ctx, filePaths, c, hasher)
		}()
	}

	// Closer goroutine: Waits for all directory walking to complete,
	// then closes the filePaths channel to signal digesters to stop.
	go func() {
		walkWg.Wait()
		close(dirsToWalk)
		close(filePaths)
		wg.Wait()
		close(dirPaths)
		close(c) // Close results channel after digesters are done.
	}()

	// Consume results: Collect hashes into the map and handle errors.
	// Also consume directory paths concurrently.
	m := make(map[string]iphash.HashBytes)
	discoveredDirs := []string{}
	var finalWalkErr error // To store the error from filepath.Walk

	// Use a loop and select to consume from multiple channels until all are closed
	dirPathsClosed := false
	resultsClosed := false

	for !dirPathsClosed || !resultsClosed {
		select {
		case dirPath, ok := <-dirPaths:
			if !ok {
				dirPathsClosed = true
			} else {
				discoveredDirs = append(discoveredDirs, dirPath)
			}
		case r, ok := <-c:
			if !ok {
				resultsClosed = true
			} else {
				if r.err != nil {
					fmt.Printf("Error hashing file %s: %v\n", r.path, r.err)
				}
				// Only add successfully hashed files
				if r.err == nil {
					filesHashed.Add(1)
					m[r.path] = r.sum
				}
			}
		// --- Add check for context cancellation in the main loop ---
		case <-ctx.Done():
			// If context is cancelled while waiting for results,
			// return immediately with the context error.
			// We might have partial results in 'm' and 'discoveredDirs'.
			// Depending on requirements, you might choose to return them or nil.
			// Returning the context error signals cancellation clearly.
			return m, discoveredDirs, ctx.Err() // Return partial results and context error
		}
	}

	if finalWalkErr != nil {
		return m, discoveredDirs, finalWalkErr
	}

	return m, discoveredDirs, nil
}
