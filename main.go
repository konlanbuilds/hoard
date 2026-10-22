package main

import (
	"log"
	"time"

	"github.com/konlanbuilds/hoard/p2p"
)

func main() {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    ":3000",
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
		// TODO: onPeer func
	}

	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)
	fileServerOptions := FileServerOptions{
		StorageRoot:       "hoard_3000_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
	}

	s := NewFileServer(fileServerOptions)

	go func() {
		time.Sleep(time.Second * 3)
		s.Quit()
	}()

	if err := s.Start(); err != nil {
		log.Fatal(err)
	}

}
