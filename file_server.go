package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/konlanbuilds/hoard/p2p"
)

type FileServerOptions struct {
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOptions

	peerLock  sync.Mutex
	peers     map[string]p2p.Peer
	storage   *Storage
	quickChan chan struct{}
}

func NewFileServer(opts FileServerOptions) *FileServer {
	storageOptions := StorageOpts{
		Root:          opts.StorageRoot,
		TransFormFunc: opts.PathTransformFunc,
	}
	return &FileServer{
		FileServerOptions: opts,
		storage:           NewStorage(storageOptions),
		quickChan:         make(chan struct{}),
		peers:             make(map[string]p2p.Peer),
	}
}

func (fs *FileServer) broadcast(payload *Payload) error {
	peers := []io.Writer{}
	for _, peer := range fs.peers {
		peers = append(peers, peer)
	}

	mulWriter := io.MultiWriter(peers...)
	return gob.NewEncoder(mulWriter).Encode(payload)
}

type Payload struct {
	Key  string
	Data []byte
}

func (fs *FileServer) StoreData(key string, r io.Reader) error {
	// 1. Store this file to disk
	// 2. Broadcast this file to all known peers on the network

	buf := new(bytes.Buffer)
	tee := io.TeeReader(r, buf)

	if err := fs.storage.Write(key, tee); err != nil {
		return err
	}

	payload := &Payload{
		Key:  key,
		Data: buf.Bytes(),
	}

	fmt.Println(buf.Bytes())

	return fs.broadcast(payload)
}

func (fs *FileServer) Quit() {
	close(fs.quickChan)
}

func (fs *FileServer) OnPeer(p p2p.Peer) error {
	fs.peerLock.Lock()
	defer fs.peerLock.Unlock()

	fs.peers[p.RemoteAddr().String()] = p
	log.Printf("Connected with remote %s\n", p.RemoteAddr())
	return nil
}

func (fs *FileServer) loop() {

	defer func() {
		log.Println("file server stopped by user.")
		fs.Transport.Close()
	}()
	for {
		select {
		case msg := <-fs.Transport.Consume():
			var payload Payload
			if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&payload); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%+v\n", payload)
		case <-fs.quickChan:
			return

		}
	}
}

func (fs *FileServer) bootstrapNetwork() error {
	for _, addr := range fs.BootstrapNodes {

		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			fmt.Println("Attempting to connect with remote: ", addr)
			if err := fs.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}

	return nil
}

func (fs *FileServer) Start() error {
	if err := fs.Transport.ListenAndAccept(); err != nil {
		return err
	}

	fs.bootstrapNetwork()

	fs.loop()
	return nil
}
