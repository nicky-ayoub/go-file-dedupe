// /home/nicky/src/go/go-file-dedupe/src/main.go
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"me/go-file-dedupe/fswalk"
	"me/go-file-dedupe/iphash"
)

// --- Application Struct ---

// Deduplicator is the main application struct that holds configuration and state.
type Deduplicator struct {
	// Configuration
	rootDir  string
	hashFunc fswalk.HashFunc
	out      io.Writer
	logger   *log.Logger

	// Results / State
	fileMap         map[string]iphash.HashBytes // path -> hash
	fileByteMap     map[string]string           // hash(string) -> first_path
	fileByteMapDups map[string][]string         // hash(string) -> duplicate_paths
	discoveredPaths []string

	// Progress Counters (Atomic)
	filesFoundCount   atomic.Uint64 // Use atomic types
	filesHashedCount  atomic.Uint64
	linksCreatedCount atomic.Uint64
}

// NewDeduplicator creates and initializes a Deduplicator instance.
func NewDeduplicator(rootDir string, hashFunc fswalk.HashFunc, out io.Writer) *Deduplicator {
	return &Deduplicator{
		rootDir:         rootDir,
		hashFunc:        hashFunc,
		out:             out,
		logger:          log.New(out, "INFO: ", log.LstdFlags),
		fileMap:         make(map[string]iphash.HashBytes),
		fileByteMap:     make(map[string]string),
		fileByteMapDups: make(map[string][]string),
		discoveredPaths: []string{}, // Initialize slice
	}
}

// Run executes the main deduplication process.
func (d *Deduplicator) Run(ctx context.Context, numWorkers int) error {
	d.logger.Println("Starting parallel file scan and hash calculation...")

	// Call DigestAll, passing the context and the hash function from the struct
	returnedFileMap, returnedDiscoveredPaths, err := fswalk.DigestAll(
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
	d.fileMap = returnedFileMap
	d.discoveredPaths = returnedDiscoveredPaths

	d.logger.Println("Hash calculation complete. Processing results for duplicates...")
	d.findDuplicates()

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

// findDuplicates processes the fileMap to populate duplicate information.
func (d *Deduplicator) findDuplicates() {
	d.logger.Println("Starting findDuplicates...")

	// Iterate through all hashed files to identify originals and duplicates.
	for path, hashBytes := range d.fileMap {
		hashString := hex.EncodeToString(hashBytes)

		// Check if we have already seen this hash.
		originalPath, ok := d.fileByteMap[hashString]
		if !ok {
			// First time seeing this hash. Record it as the original.
			d.fileByteMap[hashString] = path
		} else {
			// This hash has been seen before. This is a duplicate.
			// If this is the first duplicate for this hash, add the original file first.
			if _, exists := d.fileByteMapDups[hashString]; !exists {
				d.fileByteMapDups[hashString] = []string{originalPath}
			}
			// Append the new duplicate path.
			d.fileByteMapDups[hashString] = append(d.fileByteMapDups[hashString], path)
		}
	}
	d.logger.Println("Finished findDuplicates.")
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
	fmt.Fprintf(d.out, "%d Files scanned and hashed.\n", len(d.fileMap))
	fmt.Fprintf(d.out, "%d unique file content hashes found.\n", len(d.fileByteMap))
	fmt.Fprintf(d.out, "%d directories discovered (excluding root).\n", len(d.discoveredPaths))
	if d.linksCreatedCount.Load() > 0 {
		fmt.Fprintf(d.out, "%d duplicate files replaced with hard links.\n", d.linksCreatedCount.Load())
	}
}

// hardlinkDuplicates iterates through the found duplicates and replaces them with hard links.
func (d *Deduplicator) hardlinkDuplicates() {
	d.logger.Println("Starting hard linking process...")

	// Iterate over the map of duplicates. The key is the hash, the value is a slice of paths.
	for _, paths := range d.fileByteMapDups {
		// The first path in the slice is considered the original.
		originalPath := paths[0]
		duplicatePaths := paths[1:]

		for _, duplicatePath := range duplicatePaths {
			// --- Check if files are already hard-linked ---
			alreadyLinked, err := areFilesHardLinked(originalPath, duplicatePath)
			if err != nil {
				d.logger.Printf("Could not check hard link status for %s: %v", duplicatePath, err)
				continue
			}
			if alreadyLinked {
				d.logger.Printf("Skipping already linked file: %s", duplicatePath)
				continue
			}

			// To create a hard link, we must first remove the old file.
			if err := os.Remove(duplicatePath); err != nil {
				d.logger.Printf("Failed to remove duplicate file %s: %v", duplicatePath, err)
				continue // Skip to the next file
			}
			// Create a hard link from the original file to the path of the duplicate.
			if err := os.Link(originalPath, duplicatePath); err != nil {
				d.logger.Printf("Failed to create hard link from %s to %s: %v", originalPath, duplicatePath, err)
				continue
			}
			d.linksCreatedCount.Add(1)
			d.logger.Printf("Successfully linked %s -> %s", duplicatePath, originalPath)
		}
	}
}

// areFilesHardLinked checks if two file paths point to the same file on disk (i.e., are hard links).
// This is done by comparing their device and inode numbers.
func areFilesHardLinked(path1, path2 string) (bool, error) {
	info1, err := os.Stat(path1)
	if err != nil {
		return false, err
	}

	info2, err := os.Stat(path2)
	if err != nil {
		return false, err
	}

	// os.SameFile is the canonical way to check if two FileInfo objects refer to the same file.
	// It handles the underlying system-specific details (like comparing device and inode numbers on Unix).
	return os.SameFile(info1, info2), nil
}

// --- Define command-line flag ---
var (
	hashAlgorithm = flag.String("algo", "blake3", "Hashing algorithm to use (blake3, sha256, or md5)")
	workers       = flag.Int("workers", runtime.NumCPU(), "Number of concurrent hashing workers")
	dryRun        = flag.Bool("dry-run", false, "Perform a dry run without actual deduplication actions")
	hardlink      = flag.Bool("hardlink", false, "Replace duplicate files with hard links to the original file")
	cpuprofile    = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile    = flag.String("memprofile", "", "write memory profile to `file`")
)

func main() {
	flag.Parse()  // Parse command-line flags
	var err error // Declare err at the top of the function scope.

	// Configure the default logger to not have prefixes, as our app logger will.
	log.SetFlags(0)

	// --- Setup Profiling ---
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatalf("could not create CPU profile: %v", err)
		}
		defer f.Close() //
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("could not start CPU profile: %v", err)
		}
		// pprof.StopCPUProfile() will be called at the end of main.
		defer pprof.StopCPUProfile()
	}

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
	// Create a separate context for the progress reporter so we can cancel it
	// independently when the main work is done.
	reporterCtx, cancelReporter := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Print("\033[s") // Save cursor position
	go app.startProgressReporter(reporterCtx, &wg)

	// --- Run the Application ---
	err = app.Run(ctx, *workers)

	// Signal the progress reporter to stop and print its final status.
	cancelReporter()
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

	// --- Perform Hard Linking if requested ---
	if *hardlink && !*dryRun {
		app.hardlinkDuplicates()
	}

	// --- Write Memory Profile if requested ---
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatalf("could not create memory profile: %v", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("could not write memory profile: %v", err)
		}
		app.logger.Printf("Memory profile written to %s", *memprofile)
	}

	app.reportDuplicates()
	app.reportSummary()

	app.logger.Println("Application finished successfully.")
}
