// /home/nicky/src/go/go-file-dedupe/src/main.go
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
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

	// Results / State
	fileMap         map[string]iphash.HashBytes // path -> hash
	fileByteMap     map[string]string           // hash(string) -> first_path
	fileByteMapDups map[string][]string         // hash(string) -> duplicate_paths
	discoveredPaths []string

	// Progress Counters (Atomic)
	filesFoundCount  atomic.Uint64 // Use atomic types
	filesHashedCount atomic.Uint64
}

// --- Constructor ---
func NewDeduplicator(rootDir string, hashFunc fswalk.HashFunc) *Deduplicator {
	return &Deduplicator{
		rootDir:         rootDir,
		hashFunc:        hashFunc,
		fileMap:         make(map[string]iphash.HashBytes), // Initialize maps
		fileByteMap:     make(map[string]string),
		fileByteMapDups: make(map[string][]string),
		discoveredPaths: []string{}, // Initialize slice
	}
}

// Run executes the main deduplication process.
func (d *Deduplicator) Run(ctx context.Context, numWorkers int) error {
	log.Println("Starting parallel file scan and hash calculation...")

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
			log.Println("Operation cancelled.")
			return err
		}
		log.Printf("Error during file scanning/hashing: %v", err)
		return fmt.Errorf("file scanning/hashing failed: %w", err)
	}

	// Store results in the struct fields
	d.fileMap = returnedFileMap
	d.discoveredPaths = returnedDiscoveredPaths

	log.Println("Hash calculation complete. Processing results for duplicates...")
	d.findDuplicates()

	// Reporting
	d.reportFileMap()
	d.reportDuplicates()
	d.reportSummary()

	return nil // Success
}

// startProgressReporter runs in a goroutine to periodically display progress.
func (d *Deduplicator) startProgressReporter(ctx context.Context) {
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
	for path, hashBytes := range d.fileMap {
		hashString := hex.EncodeToString(hashBytes)

		orig, ok := d.fileByteMap[hashString]
		if !ok {
			d.fileByteMap[hashString] = path
		} else {
			fmt.Printf("\rDUPLICATE [%s] == [%s]\n", path, orig)
			if _, exists := d.fileByteMapDups[hashString]; !exists {
				d.fileByteMapDups[hashString] = []string{orig}
			}
			d.fileByteMapDups[hashString] = append(d.fileByteMapDups[hashString], path)
		}
	}
}

// reportFileMap prints the content of the fileMap (path -> hash).
func (d *Deduplicator) reportFileMap() {
	fmt.Println("\nDump FileMap (Path -> Hash)\n-------------------------")
	count := 0
	limit := 50 // Example limit

	// Access struct field directly
	fmt.Printf("FileMap contains %d entries\n", len(d.fileMap))

	for key, element := range d.fileMap {
		str := hex.EncodeToString(element)
		fmt.Println("Hash:", str, ":", key)
		count++
		if count >= limit {
			fmt.Println("... (output limited to", limit, "entries)")
			break
		}
	}
	fmt.Println("-------------------------")
}

// reportDuplicates prints the content of the fileByteMapDups (hash -> paths).
func (d *Deduplicator) reportDuplicates() {
	fmt.Println("\nDump FileMapDups (Hash -> Duplicate Paths)\n-------------------------")
	if len(d.fileByteMapDups) == 0 {
		fmt.Println("No duplicates found.")
	} else {
		for hashString, element := range d.fileByteMapDups {
			fmt.Printf("Hash |%s|: %q\n", hashString, element)
		}
	}
	fmt.Println("-------------------------")
}

// reportSummary prints the final statistics.
func (d *Deduplicator) reportSummary() {
	fmt.Println(len(d.fileMap), " Files scanned and hashed.")
	fmt.Println(len(d.fileByteMap), " unique file content hashes found.")
	fmt.Println(len(d.discoveredPaths), " directories discovered (excluding root).")
}

// // Keep global maps as they are used for processing and reporting
// var FileByteMap map[string]string
// var FileByteMapDups map[string][]string
// var FileMap map[string]iphash.HashBytes
// var discoveredPaths []string

// --- Define command-line flag ---
var (
	hashAlgorithm = flag.String("algo", "blake3", "Hashing algorithm to use (blake3, sha256, or md5)")
	workers       = flag.Int("workers", runtime.NumCPU(), "Number of concurrent hashing workers")
)

func main() {
	flag.Parse() // Parse command-line flags

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

	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}

	// --- Create Application Instance ---
	app := NewDeduplicator(workingDir, selectedHashFunc)

	// --- Setup Context for Cancellation (e.g., on Ctrl+C) ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Important: call stop to release resources when main exits

	// --- Start Progress Reporter ---
	fmt.Print("\033[s") // Save cursor position
	go app.startProgressReporter(ctx)

	// --- Run the Application ---
	app.Run(ctx, *workers)

	// --- Ensure newline after progress reporter finishes ---
	// A small delay might be needed if Run finishes extremely quickly,
	// otherwise the final reporter print might overwrite logs.
	time.Sleep(100 * time.Millisecond) // Optional small delay
	// Or rely on the final print within startProgressReporter having a newline.

	if err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(130) // Standard exit code for Ctrl+C
		}
		log.Printf("Application failed: %v", err)
		os.Exit(1)
	}

	log.Println("Application finished successfully.")
}
