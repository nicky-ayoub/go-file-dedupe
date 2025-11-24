// /home/nicky/src/go/go-file-dedupe/src/fswalk/fswalk.go
package fswalk

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/fs"
	"me/go-file-dedupe/iphash"
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
// walk on the error channel.
func walkFiles(ctx context.Context, root string, filesFound *atomic.Uint64) (<-chan string, <-chan string, <-chan error) {
	filePaths := make(chan string)
	dirPaths := make(chan string)
	errc := make(chan error, 1)

	go func() {
		defer close(filePaths)
		defer close(dirPaths)

		walker := func(path string, d fs.DirEntry, err error) error {
			select {
			case <-ctx.Done():
				return ctx.Err() // Cancellation support
			default:
			}

			if err != nil {
				fmt.Printf("Warning: Error accessing %s: %v\n", path, err)
				return nil // Continue walking if possible
			}

			if d.IsDir() {
				if path != root { // Don't report the root directory itself
					dirPaths <- path
				}
			} else if d.Type().IsRegular() {
				filesFound.Add(1)
				filePaths <- path
			}
			return nil
		}

		// Use the more modern WalkDir, which is generally more efficient.
		errc <- filepath.WalkDir(root, walker)
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

		// After a potentially long operation (hashing), we must check for
		// cancellation *before* attempting to send the result. This prevents
		// a deadlock where the receiver has already shut down.
		select {
		case <-ctx.Done():
			return // Context was cancelled while hashing, exit immediately.
		case c <- result{path, data, err}:
			// Result sent successfully.
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
	filesFound *atomic.Uint64,
	filesHashed *atomic.Uint64,
) (map[string][]string, []string, error) {
	// Start the sequential directory walker.
	filePaths, dirPaths, errc := walkFiles(ctx, root, filesFound)

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

	// Closer goroutine: Waits for all digesters to finish, then closes the results channel.
	go func() {
		wg.Wait()
		close(c)
	}()

	hashesToPaths := make(map[string][]string)
	discoveredDirs := []string{}
	var finalWalkErr error

	// This is the main processing loop. It must drain all channels concurrently
	// until all of them are closed.
	for {
		var r result
		var dirPath string
		var walkErr error
		var ok bool

		select {
		case r, ok = <-c:
			if !ok {
				c = nil // Set channel to nil to block it from being selected again.
			} else {
				if r.err != nil {
					fmt.Printf("Error hashing file %s: %v\n", r.path, r.err)
				}
				if r.err == nil {
					filesHashed.Add(1)
					hashString := hex.EncodeToString(r.sum)
					hashesToPaths[hashString] = append(hashesToPaths[hashString], r.path)
				}
			}
		case dirPath, ok = <-dirPaths:
			if !ok {
				dirPaths = nil // Set channel to nil.
			} else {
				discoveredDirs = append(discoveredDirs, dirPath)
			}
		case walkErr, ok = <-errc:
			if !ok {
				errc = nil // Set channel to nil.
			} else {
				finalWalkErr = walkErr
			}
		}

		// Exit condition for the loop
		if c == nil && dirPaths == nil && errc == nil {
			// All channels are closed and drained, so all goroutines have exited.
			break
		}
	}

	// After the loop, check if the context was cancelled.
	// This is the only safe place to check, after all goroutines are guaranteed to be done.
	if ctx.Err() != nil {
		return nil, discoveredDirs, ctx.Err()
	}

	// Only if the operation was not cancelled do we proceed to filter and return duplicates.
	if finalWalkErr == nil {
		duplicates := make(map[string][]string)
		for hash, paths := range hashesToPaths {
			if len(paths) > 1 {
				duplicates[hash] = paths
			}
		}
		return duplicates, discoveredDirs, nil
	}

	if finalWalkErr != nil {
		return nil, discoveredDirs, finalWalkErr
	}

	return nil, discoveredDirs, nil
}
