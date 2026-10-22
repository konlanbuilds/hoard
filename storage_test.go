package main

import (
	"bytes"
	"testing"
)

func TestStorage(t *testing.T) {
	opts := StorageOpts{
		TransFormFunc: DefaultPathTransformFunc,
	}
	s := NewStorage(opts)
	data := bytes.NewReader([]byte("some image bytes"))
	if err := s.writeStream("myspecialimage", data); err != nil {
		t.Error(err)
	}

}
