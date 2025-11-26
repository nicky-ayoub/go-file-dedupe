// Package fswalk handles file scanning and hashing in a directory tree.
package fswalk

import (
	"context"
	"errors"
	"fmt"
	"me/go-file-dedupe/iphash" // Make sure this import path is correct
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// HashFunc defines the signature for functions that can hash a file.
// It matches the signatures of GetFileHashMD5bytes and GetFileHashSHA256bytes.
// Exported so it can be used by the caller (main.go).
type HashFunc func(filePath string) (iphash.HashBytes, error)

// A result is the product of reading and hashing a file.
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

// DigestAll reads all the files in the file tree rooted at root, calculates their hashes in parallel,
// and returns a map from file path to MD5 sum, a slice of discovered directory paths, and any error encountered during the walk.
func DigestAll(
	ctx context.Context,
	root string,
	hasher HashFunc,
	numWorkers int,
	filesFound *atomic.Uint64, // Pointer to counter
	filesHashed *atomic.Uint64, // Pointer to counter
) (map[string]iphash.HashBytes, []string, error) {
	if hasher == nil {
		return nil, nil, errors.New("fswalk.DigestAll: provided hash function cannot be nil")
	}
	if numWorkers < 1 {
		return nil, nil, fmt.Errorf("fswalk.DigestAll: numWorkers must be at least 1, got %d", numWorkers)
	}
	if filesFound == nil || filesHashed == nil {
		return nil, nil, errors.New("fswalk.DigestAll: provided atomic counters cannot be nil")
	}

	// Create an errgroup that is tied to the parent context.
	// gctx will be canceled if any goroutine in the group returns an error.
	g, gctx := errgroup.WithContext(ctx)

	filePaths := make(chan string)
	dirPaths := make(chan string)

	// Stage 1: Walk the filesystem in a goroutine managed by the errgroup.
	g.Go(func() error {
		defer close(filePaths)
		defer close(dirPaths)

		return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err // Propagate errors from walking the path.
			}

			// Check for cancellation from the errgroup's context at each step.
			select {
			case <-gctx.Done():
				return gctx.Err() // Stop walking if context is cancelled.
			default:
			}

			if !info.IsDir() && info.Mode().IsRegular() {
				filesFound.Add(1)
				filePaths <- path
			} else if info.IsDir() && path != root { // Exclude the root dir itself
				dirPaths <- path
			}
			return nil
		})
	})

	// Stage 2: Start a fixed number of digester goroutines.
	results := make(chan result)
	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			digester(gctx, filePaths, results, hasher)
			return nil
		})
	}

	// Stage 3: Start a goroutine to close the results channel once all digesters
	// and the file walker have finished.
	go func() {
		g.Wait() // Wait for all goroutines in the group to complete.
		close(results)
	}()

	// Stage 4: Consume all the results from the pipeline.
	m := make(map[string]iphash.HashBytes)
	discoveredDirs := []string{}

	// Loop to consume from channels until they are both closed.
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
		case r, ok := <-results: // Consume results from digesters
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
		}
	}

	// Wait for all goroutines in the group to finish and return the first error encountered.
	if err := g.Wait(); err != nil {
		return m, discoveredDirs, err
	}

	return m, discoveredDirs, nil
}
