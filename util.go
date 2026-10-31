package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
)

func hashKey(key string) string {
	hash := sha256.Sum256([]byte(key))

	return hex.EncodeToString(hash[:])
}

func generateID() string {
	buf := make([]byte, 32)
	io.ReadFull(rand.Reader, buf)
	return hex.EncodeToString(buf)
}
