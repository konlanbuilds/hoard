package main

import (
	"bytes"
	"fmt"
	"testing"
)

func TestNewEncryptionKey(t *testing.T) {
	key := newEncryptionKey()
	for i := 0; i < len(key); i++ {
		if key[i] == 0x0 {
			t.Errorf("0 bytes")
		}
	}
}
func TestCopyEncrypt(t *testing.T) {
	data := "Foo not Bar"
	src := bytes.NewReader([]byte(data))
	dst := new(bytes.Buffer)

	key := newEncryptionKey()

	_, err := copyEncrypt(key, src, dst)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(len(data))
	fmt.Println(len(dst.String()))

	output := new(bytes.Buffer)
	nw, err := copyDecrypt(key, dst, output)
	if err != nil {
		t.Error(err)
	}

	if nw != 16+len(data) {
		t.Fail()
	}

	if output.String() != data {
		t.Errorf("decryption failed")
	}
}
