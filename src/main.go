package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
)

type sha256bytes [sha256.Size]byte

var FileByteMap map[sha256bytes][]string

var FileMap map[string]sha256bytes

var discoveredPaths []string
var fs billy.Filesystem
var mem billy.Filesystem

func main() {
	// Create hashmap to check against for duped files
	FileByteMap = make(map[sha256bytes][]string)
	FileMap = make(map[string]sha256bytes)
	workingDir := getWorkingDir()

	discoveredPaths = []string{}

	fs = osfs.New(workingDir)
	//mem = memfs.New()
	//fs.Chroot(fs.Root())

	fmt.Println(fs.Root())
	//fmt.Println(mem.Root())

	x, err := fs.ReadDir("/")
	if err != nil {
		log.Fatalln("Can't read dir root file system", err)
	}
	for i, e := range x {
		fmt.Printf("%08d : %s\n", i, e.Name())
	}

	// filepath.WalkDir(workingDir, dedupe)

	Walk(fs, ".", billy_dedupe)

	for key, element := range FileMap {
		fmt.Println("Hash:", getHashToPath(element), ":", key)
	}
	fmt.Println(len(FileMap), " Files")
	fmt.Println(len(FileByteMap), " unique hashes")
	fmt.Println(len(discoveredPaths), " unique discoveredPaths")

	//fmt.Println("Press Enter to exit")
	//fmt.Scanf("%s")
}

func getFileHashBytes(file string) sha256bytes {
	hasher := sha256.New()
	s, err := ioutil.ReadFile(file)
	hasher.Write(s)
	if err != nil {
		log.Fatal(err)
	}
	var arr sha256bytes
	hash := hasher.Sum(nil)
	copy(arr[:], hash[:sha256.Size])

	return arr
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

func getFileHashParts(file string) []string {
	curFileHash := getFileHash(file)
	parts := []string{curFileHash[0:2], curFileHash[2:4], curFileHash[4:]}
	return parts
}

func getHashToPath(code sha256bytes) string {
	curFileHash := hex.EncodeToString(code[:])
	parts := []string{curFileHash[0:2], curFileHash[2:4], curFileHash[4:]}
	return filepath.Join(parts...)
}

func getFileHashPath(file string) string {
	parts := getFileHashParts(file)
	return filepath.Join(parts...)
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

	if d.Type().IsRegular() {
		bytes := getFileHashBytes(s)
		//fmt.Printf("%s\n", hex.EncodeToString(bytes[:]))
		FileMap[s] = bytes

		// Do map key lookup
		_, hashExists := FileByteMap[bytes]

		if !hashExists {
			// Add new file hash to hashmap
			FileByteMap[bytes] = make([]string, 0)
		} else {
			fmt.Printf("DupFile %s %+q\n", getFileHash(s), FileByteMap[bytes])
		}
		FileByteMap[bytes] = append(FileByteMap[bytes], s)
	} else {
		discoveredPaths = append(discoveredPaths, s)
	}
	return nil
}

func billy_dedupe(path string, info os.FileInfo, err error) error {

	if err != nil {
		return err
	}
	if !info.IsDir() {
		bytes := getFileHashBytes(path)
		//fmt.Printf("%s\n", hex.EncodeToString(bytes[:]))
		FileMap[path] = bytes

		// Do map key lookup
		_, hashExists := FileByteMap[bytes]

		if !hashExists {
			// Add new file hash to hashmap
			FileByteMap[bytes] = make([]string, 0)
		} else {
			fmt.Printf("DupFile %s %+q\n", getFileHash(path), FileByteMap[bytes])
		}
		FileByteMap[bytes] = append(FileByteMap[bytes], path)
	} else {
		discoveredPaths = append(discoveredPaths, path)
	}
	return nil
}

// walk recursively descends path, calling walkFn
// adapted from https://golang.org/src/path/filepath/path.go
func walk(fs billy.Filesystem, path string, info os.FileInfo, walkFn filepath.WalkFunc) error {
	if !info.IsDir() {
		return walkFn(path, info, nil)
	}

	names, err := readdirnames(fs, path)
	err1 := walkFn(path, info, err)
	// If err != nil, walk can't walk into this directory.
	// err1 != nil means walkFn want walk to skip this directory or stop walking.
	// Therefore, if one of err and err1 isn't nil, walk will return.
	if err != nil || err1 != nil {
		// The caller's behavior is controlled by the return value, which is decided
		// by walkFn. walkFn may ignore err and return nil.
		// If walkFn returns SkipDir, it will be handled by the caller.
		// So walk should return whatever walkFn returns.
		return err1
	}

	for _, name := range names {
		filename := filepath.Join(path, name)
		fileInfo, err := fs.Lstat(filename)
		if err != nil {
			if err := walkFn(filename, fileInfo, err); err != nil && err != filepath.SkipDir {
				return err
			}
		} else {
			err = walk(fs, filename, fileInfo, walkFn)
			if err != nil {
				if !fileInfo.IsDir() || err != filepath.SkipDir {
					return err
				}
			}
		}
	}
	return nil
}

// Stolen from the billy master

// Walk walks the file tree rooted at root, calling fn for each file or
// directory in the tree, including root. All errors that arise visiting files
// and directories are filtered by fn: see the WalkFunc documentation for
// details.
//
// The files are walked in lexical order, which makes the output deterministic
// but requires Walk to read an entire directory into memory before proceeding
// to walk that directory. Walk does not follow symbolic links.
//
// Function adapted from https://github.com/golang/go/blob/3b770f2ccb1fa6fecc22ea822a19447b10b70c5c/src/path/filepath/path.go#L500
func Walk(fs billy.Filesystem, root string, walkFn filepath.WalkFunc) error {
	info, err := fs.Lstat(root)
	if err != nil {
		err = walkFn(root, nil, err)
	} else {
		err = walk(fs, root, info, walkFn)
	}

	if err == filepath.SkipDir {
		return nil
	}

	return err
}

func readdirnames(fs billy.Filesystem, dir string) ([]string, error) {
	files, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, file := range files {
		names = append(names, file.Name())
	}

	return names, nil
}
