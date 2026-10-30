package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func newStorage() *Storage {
	options := StorageOpts{
		TransFormFunc: CASPathTransformFunc,
	}
	return NewStorage(options)
}

func tearDown(t *testing.T, s *Storage) {
	if err := s.Clear(); err != nil {
		t.Error(err)
	}
}
func TestTransFormFunc(t *testing.T) {
	key := "momsbestpictures"
	pathKey := CASPathTransformFunc(key)
	expectedFileName := "52439f5e49ba33b4b9b2373dcd6bfc9e97cafb7f"
	expectedPathName := "52439/f5e49/ba33b/4b9b2/373dc/d6bfc/9e97c/afb7f"
	if pathKey.PathName != expectedPathName {
		t.Errorf("have %s wants %s", pathKey.PathName, expectedPathName)
	}
	if pathKey.FileName != expectedFileName {
		t.Errorf("have %s wants %s", pathKey.FileName, expectedFileName)
	}
}

func TestStorage(t *testing.T) {
	s := newStorage()
	defer tearDown(t, s)

	for i := 0; i < 15; i++ {

		key := fmt.Sprintf("foo_%d", i)

		data := []byte("some image bytes")
		if _, err := s.writeStream(key, bytes.NewReader(data)); err != nil {
			t.Error(err)
		}

		if ok := s.Has(key); !ok {
			t.Errorf("expected to have key %s", key)
		}

		_, r, err := s.Read(key)
		if err != nil {
			t.Error(err)
		}

		b, _ := io.ReadAll(r)

		fmt.Println(string(b))

		if string(b) != string(data) {
			t.Errorf("want %s have %s", data, b)
		}
		if err := s.Delete(key); err != nil {
			t.Error(err)
		}

		if ok := s.Has(key); ok {
			t.Errorf("Expected to NOT have key %s", key)
		}

	}

}

// func TestStorageDelete(t *testing.T) {
// 	opts := StorageOpts{
// 		TransFormFunc: CASPathTransformFunc,
// 	}

// 	s := NewStorage(opts)

// 	key := "momsspecials"
// 	data := []byte("some image bytes")

// 	if err := s.writeStream(key, bytes.NewReader(data)); err != nil {
// 		t.Error(err)
// 	}

// 	if err := s.Delete(key); err != nil {
// 		t.Error(err)
// 	}

// }
