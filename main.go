package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
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

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("picture_%d.png", i)
		dat := fmt.Sprintf("Large data file %d\n", i)
		data := bytes.NewReader([]byte(dat))
		id := fs3.ID
		fs3.StoreData(id, key, data)

		if err := fs3.storage.Delete(fs3.ID, key); err != nil {
			log.Fatal(err)
		}

		r, err := fs3.GetData(key)
		if err != nil {
			log.Fatal(err)
		}

		b, err := io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(b))
	}
}
