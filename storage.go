package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
)

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

var DefaultPathTransformFunc = func(key string) string {
	return key
}

type StorageOpts struct {
	TransFormFunc PathTransformFunc
}

type Storage struct {
	StorageOpts
}

func NewStorage(opts StorageOpts) *Storage {
	return &Storage{
		StorageOpts: opts,
	}
}

func (s *Storage) writeStream(key string, r io.Reader) error {
	pathKey := s.TransFormFunc(key)
	if err := os.MkdirAll(pathKey.PathName, os.ModePerm); err != nil {
		return err
	}

	fullPath := pathKey.FullPath()

	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	log.Printf("written (%d) bytes to disk: %s", n, fullPath)

	return nil
}

func (s *Storage) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)

	return buf, err
}

func (s *Storage) readStream(key string) (io.ReadCloser, error) {
	PathKey := s.TransFormFunc(key)

	return os.Open(PathKey.FullPath())
}

func (s *Storage) Has(key string) bool {
	pathKey := s.TransFormFunc(key)

	_, err := os.Stat(pathKey.FullPath())
	if err == fs.ErrNotExist {
		return false
	}

	return true
}

func (s *Storage) Delete(key string) error {
	pathKey := s.TransFormFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.FileName)

	}()

	return os.RemoveAll(pathKey.firstPathName())
}
