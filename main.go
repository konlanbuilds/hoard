package main

import (
	"bytes"
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
	fs2 := makeServer(":4000", ":3000")

	go func() {
		log.Fatal(fs1.Start())
	}()

	time.Sleep(2 * time.Second)

	go fs2.Start()
	time.Sleep(2 * time.Second)

	data := bytes.NewReader([]byte("Large data file!\n"))
	fs2.StoreData("CoolPicture.jpg", data)

	// r, err := fs2.GetData("CoolPicture.jpg")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// b, err := io.ReadAll(r)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println(string(b))
}
