package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/konlanbuilds/hoard/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
		// TODO: onPeer func
	}

	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)
	fileServerOptions := FileServerOptions{
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

	for i := 0; i < 10; i++ {
		data := bytes.NewReader([]byte("Large data file!\n"))
		fs2.StoreData(fmt.Sprintf("mydata_%d", i), data)
		time.Sleep(time.Microsecond * 5)
	}

	// r, err := fs2.GetData("randomkey")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// b, err := io.ReadAll(r)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println(string(b))
	select {}
}
