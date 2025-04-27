// /home/nicky/src/go/go-file-dedupe/src/fswalk/fswalk.go
package fswalk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"me/go-file-dedupe/iphash" // Make sure this import path is correct
	"path/filepath"
	"sync"
	"sync/atomic"
)

// HashFunc defines the signature for functions that can hash a file.
// It matches the signatures of GetFileHashMD5bytes and GetFileHashSHA256bytes.
// Exported so it can be used by the caller (main.go).
type HashFunc func(filePath string) (iphash.HashBytes, error)

// walkFiles starts a goroutine to walk the directory tree at root.
// It sends the path of each regular file to the filePaths channel,
// each directory to the dirPaths channel, and the result of the
// walk on the error channel. If done is closed, walkFiles abandons its work.
func walkFiles(ctx context.Context, root string, filesFound *atomic.Uint64) (<-chan string, <-chan string, <-chan error) {
	filePaths := make(chan string)
	dirPaths := make(chan string)
	errc := make(chan error, 1)

	go func() {
		defer close(filePaths) // Close channels when walk is done
		defer close(dirPaths)

		// The actual filepath.WalkFunc logic
		walker := func(path string, info fs.FileInfo, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err() // Return context error (e.g., context.Canceled)
			default:
			}

			if err != nil {
				fmt.Printf("Warning: Error accessing %s: %v\n", path, err)
				return nil // Continue walking if possible
			}
			if info.IsDir() {
				// Don't send the root directory itself unless needed
				if path == root {
					return nil
				}
				select {
				case dirPaths <- path:
				case <-ctx.Done(): // Check context here too
					return ctx.Err()
				}
			} else if info.Mode().IsRegular() { // Only process regular files
				//fmt.Println("DEBUG: Walker found regular file:", path)
				filesFound.Add(1)
				select {
				case filePaths <- path:
				case <-ctx.Done(): // Check context here too
					return ctx.Err()
				}
			}
			return nil
		}
		errc <- filepath.Walk(root, walker)
	}()

	return filePaths, dirPaths, errc
}

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
	if hasher == nil {
		return nil, nil, errors.New("fswalk.DigestAll: provided hash function cannot be nil")
	}
	if numWorkers < 1 {
		return nil, nil, fmt.Errorf("fswalk.DigestAll: numWorkers must be at least 1, got %d", numWorkers)
	}
	if filesFound == nil || filesHashed == nil {
		// Add check for nil counters
		return nil, nil, errors.New("fswalk.DigestAll: provided atomic counters cannot be nil")
	}

	filePaths, dirPaths, errc := walkFiles(ctx, root, filesFound)

	c := make(chan result)
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			digester(ctx, filePaths, c, hasher)
		}()
	}

	// Closer goroutine: Waits for all digesters to finish, then closes the results channel.
	go func() {
		wg.Wait()
		close(c)
	}()

	// Consume results: Collect hashes into the map and handle errors.
	// Also consume directory paths concurrently.
	m := make(map[string]iphash.HashBytes)
	discoveredDirs := []string{}
	var finalWalkErr error // To store the error from filepath.Walk

	// Use a loop and select to consume from multiple channels until all are closed
	dirPathsClosed := false
	resultsClosed := false
	errcClosed := false // errc is buffered, will receive one value

	for !errcClosed || !dirPathsClosed || !resultsClosed {
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
