package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"me/go-file-dedupe/fswalk"
	"me/go-file-dedupe/iphash"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
)

var FileByteMap map[iphash.HashBytes]string
var FileByteMapDups map[iphash.HashBytes][]string

var FileMap map[string]iphash.HashBytes

var discoveredPaths []string
var fs billy.Filesystem
var mem billy.Filesystem

func main() {
	// Create hashmap to check against for duped files
	FileByteMap = make(map[iphash.HashBytes]string)
	FileByteMapDups = make(map[iphash.HashBytes][]string)
	FileMap = make(map[string]iphash.HashBytes)
	discoveredPaths = []string{}

	// Setup the File Systems to Scan and create
	workingDir := getWorkingDir()
	fs = osfs.New(workingDir)
	mem = memfs.New()

	// FS is the data file system
	// MEM  is the metadata file system - will be the real cache at some point

	//readdir(fs)
	log.Println("Scanning for duplicates...")
	fmt.Print("\033[s")
	go StartSpider()
	fswalk.Walk(fs, ".", billy_dedupe)
	//dumpFileMap()
	//dumpFileMapDups()

	//readdir(mem)

	log.Println("Printing duplicates...")
	fswalk.Walk(mem, ".", billy_print_dups) // just because we can...

	fmt.Println(len(FileMap), " Files")
	fmt.Println(len(FileByteMap), " unique hashes")
	fmt.Println(len(discoveredPaths), " unique discoveredPaths")

	//fmt.Println("Press Enter to exit")
	//fmt.Scanf("%s")
}

func getWorkingDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalln(err)
	}
	return dir
}

func billy_print_dups(path string, info os.FileInfo, err error) error {
	if err != nil {
		fmt.Println(err)
		return err
	}
	if !info.IsDir() {
		file, _ := mem.Open(path)
		defer file.Close()
		lines, body, err := lineCounter(file)
		if err != nil {
			fmt.Println(err)
			return err
		}

		if lines > 1 {
			fmt.Printf("name: %s: lines %d\n", path, lines)
			fileContent := string(body)
			fmt.Println(fileContent)
		}
	}
	return nil
}

func billy_dedupe(path string, info os.FileInfo, err error) error {

	if err != nil {
		return err
	}
	if !info.IsDir() {
		code := iphash.GetFileHashBytes(path)
		//fmt.Printf("%s\n", hex.EncodeToString(code[:]))
		FileMap[path] = code

		// Do map key lookup
		_, hashExists := FileByteMap[code]

		if !hashExists {
			// Add new file hash to hashmap
			FileByteMap[code] = path
		} else {
			fmt.Printf("DupFile '%s' %s %+q\n", path, iphash.GetFileHash(path), FileByteMap[code])
			FileByteMapDups[code] = append(FileByteMapDups[code], path)
		}
		AppendLineToFile(mem, path, iphash.GetHashToPath(code))
	} else {
		discoveredPaths = append(discoveredPaths, path)
	}
	return nil
}

func AppendLineToFile(fs billy.Filesystem, line string, file string) error {
	if _, err := fs.Stat(filepath.Dir(file)); os.IsNotExist(err) {
		fs.MkdirAll(filepath.Dir(file), 0700) // Create your file
	}
	f, err := fs.OpenFile(file,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte(line + "\n")); err != nil {
		return err
	}
	return nil
}

func lineCounter(r io.Reader) (int, []byte, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, buf, nil

		case err != nil:
			return count, buf, err
		}
	}
}
func readdir(fs billy.Filesystem) {
	fmt.Println("\nDump Directory", fs.Root(), "\n-------------------------")
	x, err := fs.ReadDir("/")
	if err != nil {
		log.Fatalln("Can't read dir root file system", err)
	}
	for i, e := range x {
		if !e.IsDir() {
			fmt.Printf("%08d : %s  <%s>\n", i, e.Name(), iphash.GetFileHash(e.Name()))
		} else {
			fmt.Printf("%08d : %s\n", i, e.Name())
		}
	}
	fmt.Println("-------------------------")
}
func dumpFileMap() {
	fmt.Println("\nDump FileMap\n-------------------------")
	for key, element := range FileMap {
		fmt.Println("Hash:", iphash.GetHashToPath(element), ":", key)
	}
	fmt.Println("-------------------------")
}
func dumpFileMapDups() {
	fmt.Println("\nDump FileMapDups\n-------------------------")
	for key, element := range FileByteMapDups {
		fmt.Println("Hash:", iphash.GetHashToPath(key), ":", strings.Join(element[:], ","))
	}
	fmt.Println("-------------------------")
}

// StartSpider starts the spider. Call this function in a goroutine.
func StartSpider() {
	nextTime := time.Now().Truncate(time.Minute)
	nextTime = nextTime.Add(time.Minute)
	time.Sleep(time.Until(nextTime))
	Spider()
	go StartSpider()
}

// Spider scans website's market.
func Spider() {
	fmt.Print("\033[u\033[K")
	fmt.Printf("Progress : %d file(s) scanned", len(FileMap))
}
