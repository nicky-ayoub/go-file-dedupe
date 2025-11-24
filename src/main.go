// /home/nicky/src/go/go-file-dedupe/src/main.go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"me/go-file-dedupe/fswalk"
	"me/go-file-dedupe/iphash"
)

// --- Application Struct ---
type Deduplicator struct {
	// Configuration
	rootDir  string
	hashFunc fswalk.HashFunc
	out      io.Writer
	logger   *log.Logger

	// Results / State
	fileByteMapDups map[string][]string // hash(string) -> duplicate_paths
	discoveredPaths []string            // All discovered sub-directories

	// Progress Counters (Atomic)
	filesFoundCount  atomic.Uint64 // Use atomic types
	filesHashedCount atomic.Uint64
}

// --- Constructor ---
func NewDeduplicator(rootDir string, hashFunc fswalk.HashFunc, out io.Writer) *Deduplicator {
	return &Deduplicator{
		rootDir:         rootDir,
		hashFunc:        hashFunc,
		out:             out,
		logger:          log.New(out, "INFO: ", log.LstdFlags),
		fileByteMapDups: make(map[string][]string), // Initialize map
		discoveredPaths: []string{},                // Initialize slice
	}
}

// Run executes the main deduplication process.
func (d *Deduplicator) Run(ctx context.Context, numWorkers int) error {
	d.logger.Println("Starting parallel file scan and hash calculation...")

	// Call DigestAll, passing the context and the hash function from the struct
	returnedDuplicates, returnedDiscoveredPaths, err := fswalk.DigestAll(
		ctx,
		d.rootDir,
		d.hashFunc,
		numWorkers,
		&d.filesFoundCount,  // Pass pointer
		&d.filesHashedCount, // Pass pointer
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			d.logger.Println("Operation cancelled.")
			return err
		}
		d.logger.Printf("Error during file scanning/hashing: %v", err)
		return fmt.Errorf("file scanning/hashing failed: %w", err)
	}

	// Store results in the struct fields
	d.fileByteMapDups = returnedDuplicates
	d.discoveredPaths = returnedDiscoveredPaths

	return nil // Success
}

// startProgressReporter runs in a goroutine to periodically display progress.
func (d *Deduplicator) startProgressReporter(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done() // Signal that this goroutine has finished when it returns.

	ticker := time.NewTicker(1 * time.Second) // Update every second
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			// Read atomic counters
			found := d.filesFoundCount.Load()
			hashed := d.filesHashedCount.Load()
			elapsed := time.Since(startTime).Round(time.Second)

			// Print progress, overwriting previous line
			fmt.Print("\033[u\033[K") // Restore cursor, clear line
			fmt.Printf("Progress: Found %d files, Hashed %d files [%s]...", found, hashed, elapsed)

		case <-ctx.Done():
			// Context cancelled (operation finished or interrupted)
			// Print final status
			found := d.filesFoundCount.Load()
			hashed := d.filesHashedCount.Load()
			elapsed := time.Since(startTime).Round(time.Second)
			fmt.Print("\033[u\033[K") // Restore cursor, clear line
			fmt.Printf("Progress: Found %d files, Hashed %d files [%s]... Done\n", found, hashed, elapsed)
			return // Exit goroutine
		}
	}
}

// reportDuplicates prints the content of the fileByteMapDups (hash -> paths).
func (d *Deduplicator) reportDuplicates() {
	fmt.Fprintln(d.out, "\nDump FileMapDups (Hash -> Duplicate Paths)\n-------------------------")
	if len(d.fileByteMapDups) == 0 {
		fmt.Fprintln(d.out, "No duplicates found.")
	} else {
		for hashString, element := range d.fileByteMapDups {
			fmt.Fprintf(d.out, "Hash |%s|: %q\n", hashString, element)
		}
	}
	fmt.Fprintln(d.out, "-------------------------")
}

// reportSummary prints the final statistics.
func (d *Deduplicator) reportSummary() {
	totalFilesHashed := d.filesHashedCount.Load()
	totalDuplicates := 0
	for _, paths := range d.fileByteMapDups {
		totalDuplicates += len(paths) - 1
	}
	uniqueFiles := totalFilesHashed - uint64(totalDuplicates)

	fmt.Fprintf(d.out, "%d Files scanned and hashed.\n", totalFilesHashed)
	fmt.Fprintf(d.out, "%d unique file content hashes found.\n", uniqueFiles)
	fmt.Fprintf(d.out, "%d directories discovered (excluding root).\n", len(d.discoveredPaths))
}

// --- Define command-line flag ---
var (
	hashAlgorithm = flag.String("algo", "blake3", "Hashing algorithm to use (blake3, sha256, or md5)")
	workers       = flag.Int("workers", runtime.NumCPU(), "Number of concurrent hashing workers")
	dryRun        = flag.Bool("dry-run", false, "Perform a dry run without actual deduplication actions")
)

func main() {
	flag.Parse()  // Parse command-line flags
	var err error // Declare err at the top of the function scope.

	// Configure the default logger to not have prefixes, as our app logger will.
	log.SetFlags(0)

	// --- Validate number of workers ---
	if *workers < 1 {
		log.Fatalf("Error: Number of workers must be at least 1, got %d", *workers)
	}
	log.Printf("Using %d hashing workers.", *workers)

	// --- Select the hashing function based on the flag ---
	var selectedHashFunc fswalk.HashFunc // Use the exported type from fswalk
	switch strings.ToLower(*hashAlgorithm) {
	case "blake3":
		selectedHashFunc = iphash.GetFileHashBLAKE3bytes
		log.Println("Using BLAKE3 hashing algorithm.")
	case "md5":
		selectedHashFunc = iphash.GetFileHashMD5bytes
		log.Println("Using MD5 hashing algorithm.")
	case "sha256":
		selectedHashFunc = iphash.GetFileHashSHA256bytes
		log.Println("Using SHA256 hashing algorithm.")
	default:
		log.Fatalf("Error: Invalid hashing algorithm '%s'. Please use 'blake3', 'sha256', or 'md5'.", *hashAlgorithm)
	}

	// --- Determine the root directory to scan ---
	scanDir := flag.Arg(0) // Get the first non-flag argument
	if scanDir == "" {
		// If no directory is provided, default to the current working directory.
		scanDir, err = os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get current working directory: %v", err)
		}
		log.Printf("No directory specified, using current directory: %s", scanDir)
	}

	// --- Create Application Instance ---
	app := NewDeduplicator(scanDir, selectedHashFunc, os.Stdout)

	// --- Setup Context for Cancellation (e.g., on Ctrl+C) ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Important: call stop to release resources when main exits

	// --- Start Progress Reporter ---
	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Print("\033[s") // Save cursor position
	go app.startProgressReporter(ctx, &wg)

	// --- Run the Application ---
	err = app.Run(ctx, *workers)

	// Wait for the progress reporter to finish printing its final line and exit.
	wg.Wait()

	if err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(130) // Standard exit code for Ctrl+C
		}
		log.Printf("Application failed: %v", err)
		os.Exit(1)
	}

	// --- Report the results ---
	if *dryRun {
		app.logger.Println("Dry run mode: No files would be modified.")
	}

	app.reportDuplicates()
	app.reportSummary()

	app.logger.Println("Application finished successfully.")
}
