package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const defaultRootDirName = "hoardNetworkFiles"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key)) //[20]byte => []byte => hash[:]
	hashString := hex.EncodeToString(hash[:])

	blockSize := 5
	sliceLen := len(hashString) / blockSize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashString[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		FileName: hashString,
	}
}

type PathTransformFunc func(string) PathKey

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.FileName)
}

type PathKey struct {
	PathName string
	FileName string
}

func (p PathKey) firstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return "ss"
	}
	return paths[0]
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		FileName: key,
	}
}

type StorageOpts struct {
	// Root is the folder name of the root,
	// containing, all the files/dirs of the system
	Root          string
	TransFormFunc PathTransformFunc
}

type Storage struct {
	StorageOpts
}

func NewStorage(opts StorageOpts) *Storage {
	if opts.TransFormFunc == nil {
		opts.TransFormFunc = DefaultPathTransformFunc
	}
	if opts.Root == "" {
		opts.Root = defaultRootDirName
	}
	return &Storage{
		StorageOpts: opts,
	}
}

func (s *Storage) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *Storage) writeStream(key string, r io.Reader) (int64, error) {
	pathKey := s.TransFormFunc(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return 0, err
	}

	fullPathRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	f, err := os.Create(fullPathRoot)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(f, r)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (s *Storage) Read(key string) (int64, io.Reader, error) {
	return s.readStream(key)
}

func (s *Storage) readStream(key string) (int64, io.ReadCloser, error) {
	pathKey := s.TransFormFunc(key)
	fullPathRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	file, err := os.Open(fullPathRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}

func (s *Storage) Has(key string) bool {
	pathKey := s.TransFormFunc(key)
	fullPathRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	_, err := os.Stat(fullPathRoot)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Storage) Delete(key string) error {
	pathKey := s.TransFormFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.FileName)
	}()
	firstpathNameRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.firstPathName())

	return os.RemoveAll(firstpathNameRoot)
}

func (s *Storage) Clear() error {
	return os.RemoveAll(s.Root)
}
