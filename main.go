package main

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/konlanbuilds/hoard/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}

	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)
	fileServerOptions := FileServerOptions{
		EncKey:            newEncryptionKey(),
		StorageRoot:       "hoard_" + listenAddr + "_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	fs := NewFileServer(fileServerOptions)

	tcpTransport.OnPeer = fs.OnPeer
	return fs
}

func main() {
	fs1 := makeServer(":3000", "")
	fs2 := makeServer(":7000", "")
	fs3 := makeServer(":5000", ":3000", ":7000")

	go func() { log.Fatal(fs1.Start()) }()
	// time.Sleep(500 * time.Millisecond)
	go func() { log.Fatal(fs2.Start()) }()

	time.Sleep(5 * time.Millisecond)

	go fs3.Start()
	time.Sleep(2 * time.Second)

	const key = "test_file.mp4"

	file, err := os.Open(key)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	if err := fs3.StoreData(fs3.ID, key, file); err != nil {
		log.Fatal(err)
	}

	if err := fs3.storage.Delete(fs3.ID, key); err != nil {
		log.Fatal(err)
	}

	r, err := fs3.GetData(key)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closer, ok := r.(io.Closer); ok {
			closer.Close()
		}
	}()

	recoveredSize, err := io.Copy(io.Discard, r)
	if err != nil {
		log.Fatal(err)
	}
	if recoveredSize != info.Size() {
		log.Fatalf("recovered file size mismatch: got %d bytes, want %d", recoveredSize, info.Size())
	}

	log.Printf("successfully replicated and recovered %s (%d bytes)", key, recoveredSize)
}
