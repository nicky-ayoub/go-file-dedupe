// Package fswalk handles file scanning and hashing in a directory tree.
package fswalk

import (
	"context"
	"errors"
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

// walkFiles starts a goroutine to walk the directory tree from root and sends
// the path of each regular file and directory onto their respective channels.
// It returns an error on a third channel if the walk fails.
func walkFiles(ctx context.Context, root string, filesFound *atomic.Uint64) (<-chan string, <-chan string, <-chan error) {
	filePaths := make(chan string)
	dirPaths := make(chan string)
	errc := make(chan error, 1) // Buffered channel for the final error.

	go func() {
		defer close(filePaths) // Close channels when walk is done
		defer close(dirPaths)

		walker := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err // Propagate errors from walking the path.
			}
			// Check for cancellation at each step.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !info.IsDir() && info.Mode().IsRegular() {
				filesFound.Add(1)
				filePaths <- path
			} else if info.IsDir() && path != root { // Exclude the root dir itself
				dirPaths <- path
			}
			return nil
		}

		// filepath.Walk is synchronous and will block until complete.
		// The final error is sent to errc.
		errc <- filepath.Walk(root, walker)
	}()

	return filePaths, dirPaths, errc
}

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

	// Stage 1: Walk the filesystem to find files and directories.
	filePaths, dirPaths, errc := walkFiles(ctx, root, filesFound)

	// Stage 2: Start a fixed number of goroutines to hash files.
	c := make(chan result)
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			digester(ctx, filePaths, c, hasher)
		}()
	}

	// Stage 3: Close the results channel once all the digesters are done.
	go func() {
		wg.Wait()
		close(c)
	}()

	// Stage 4: Consume all the results from the pipeline.
	m := make(map[string]iphash.HashBytes)
	discoveredDirs := []string{}
	var finalWalkErr error // To store the error from filepath.Walk

	// Use a loop and select to consume from multiple channels until all are closed
	dirPathsClosed := false
	resultsClosed := false
	errcClosed := false

	for !dirPathsClosed || !resultsClosed || !errcClosed {
		select {
		case dirPath, ok := <-dirPaths:
			if !ok {
				dirPathsClosed = true
			} else {
				discoveredDirs = append(discoveredDirs, dirPath)
			}
		case r, ok := <-c: // Consume results from digesters
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
		case walkErr, ok := <-errc:
			if !ok {
				errcClosed = true
			} else {
				finalWalkErr = walkErr // Store the final walk error
				errcClosed = true
			}
		case <-ctx.Done():
			// If context is cancelled, drain the error channel to prevent a goroutine leak
			<-errc
			return m, discoveredDirs, ctx.Err() // Return partial results and context error
		}
	}

	if finalWalkErr != nil {
		return m, discoveredDirs, finalWalkErr
	}

	return m, discoveredDirs, nil
}
