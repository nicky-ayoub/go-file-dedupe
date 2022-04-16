package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"me/go-file-dedupe/hashmap"
	"os"
	"path/filepath"
)

func main() {
	// Create hashmap to check against for duped files
	hashmap.FileHashMap = make(map[string]int)
	hashmap.DeletedFiles = 0
	workingDir := getWorkingDir()
	filepath.WalkDir(workingDir, dedupe)

	fmt.Printf("Finished de-duplicating. Removed %d duplicate files.\nPress Enter to exit.\n", hashmap.DeletedFiles)
	fmt.Scanf("%s")
}

func getFileHash(file string) string {
	hasher := sha256.New()
	s, err := ioutil.ReadFile(file)
	hasher.Write(s)
	if err != nil {
		log.Fatal(err)
	}

	hash := hex.EncodeToString(hasher.Sum(nil))

	return hash
}

func getWorkingDir() string {
	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return pwd
}

func dedupe(s string, d os.DirEntry, err error) error {
	if err != nil {
		return err
	}

	if !d.IsDir() {
		currFileHash := getFileHash(s)

		// Do map key lookup
		_, hashExists := hashmap.FileHashMap[currFileHash]

		if hashExists == false {
			// Add new file hash to hashmap
			hashmap.FileHashMap[currFileHash] = 1
		} else {
			hashmap.DeletedFiles += 1
			// Only print status every 50 files.
			if hashmap.DeletedFiles%50 == 0 {
				fmt.Printf("De-duped %d files so far.\n", hashmap.DeletedFiles)
			}
			os.Remove(s)
		}
	}
	return nil
}
