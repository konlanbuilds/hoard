package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

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

func (fs *FileServer) stream(msg *Message) error {
	peers := []io.Writer{}
	for _, peer := range fs.peers {
		peers = append(peers, peer)
	}

	mulWriter := io.MultiWriter(peers...)
	return gob.NewEncoder(mulWriter).Encode(msg)
}

func (fs *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range fs.peers {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	Key  string
	Size int64
}

type MessageGetFile struct {
	Key string
}

func (fs *FileServer) GetData(key string) (io.Reader, error) {
	if fs.storage.Has(key) {
		return fs.storage.Read(key)
	}

	fmt.Printf("File (%s) not found locally, fetching from network...\n", key)

	message := Message{
		Payload: MessageGetFile{
			Key: key,
		},
	}

	if err := fs.broadcast(&message); err != nil {
		return nil, err
	}

	time.Sleep(time.Second * 3)

	for _, peer := range fs.peers {
		fmt.Println("receive stream for peer: ", peer.RemoteAddr())
		fileBuffer := new(bytes.Buffer)
		n, err := io.CopyN(fileBuffer, peer, 17)
		if err != nil {
			return nil, err
		}
		fmt.Println("Received bytes over the network: ", n)
		fmt.Println(fileBuffer.String())
	}

	select {}
	return nil, nil
}

func (fs *FileServer) StoreData(key string, r io.Reader) error {
	// 1. Store  file to disk
	// 2.Broadcast file to all known peers on the network

	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)
	size, err := fs.storage.Write(key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			Key:  key,
			Size: size,
		},
	}

	if err := fs.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 2)

	// TODO: (@konlanbuilds) use a multiwriter here
	for _, peer := range fs.peers {
		peer.Send([]byte{p2p.IncomingStream})
		n, err := io.Copy(peer, fileBuffer)
		if err != nil {
			return err
		}

		fmt.Println("Received and written bytes to disk: ", n)
	}
	return nil
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
		log.Println("File server stopped due to error or user quit action")
		fs.Transport.Close()
	}()
	for {
		select {
		case rpc := <-fs.Transport.Consume():
			var message Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&message); err != nil {
				log.Println("decoding error: ", err)
			}
			if err := fs.handleMessage(rpc.From, &message); err != nil {
				log.Println("handle message error: ", err)
			}
		case <-fs.quickChan:
			return

		}
	}
}

func (fs *FileServer) handleMessage(from string, message *Message) error {
	switch v := message.Payload.(type) {
	case MessageStoreFile:
		return fs.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return fs.handleMessageGetFile(from, v)
	}

	return nil
}

func (fs *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !fs.storage.Has(msg.Key) {
		return fmt.Errorf("Need to serve file (%s) but it does not exist on disk\n", msg.Key)
	}

	fmt.Printf("Serving file (%s) over the network\n", msg.Key)
	r, err := fs.storage.Read(msg.Key)
	if err != nil {
		return err
	}

	peer, ok := fs.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}
	fmt.Printf("Written %d bytes over the network to %s\n", n, from)

	return nil
}

func (fs *FileServer) handleMessageStoreFile(from string, message MessageStoreFile) error {
	peer, ok := fs.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := fs.storage.Write(message.Key, io.LimitReader(peer, message.Size))
	if err != nil {
		return err
	}
	fmt.Printf("[%s] written %d bytes to disk\n", fs.Transport.Addr(), n)

	peer.CloseStream()

	return nil
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

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}
