package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/konlanbuilds/hoard/p2p"
)

type FileServerOptions struct {
	ID                string
	EncKey            []byte
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
	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}
	return &FileServer{
		FileServerOptions: opts,
		storage:           NewStorage(storageOptions),
		quickChan:         make(chan struct{}),
		peers:             make(map[string]p2p.Peer),
	}
}

func (fs *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range fs.peersSnapshot() {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func (fs *FileServer) peersSnapshot() []p2p.Peer {
	fs.peerLock.Lock()
	defer fs.peerLock.Unlock()

	peers := make([]p2p.Peer, 0, len(fs.peers))
	for _, peer := range fs.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (fs *FileServer) getPeer(from string) (p2p.Peer, bool) {
	fs.peerLock.Lock()
	defer fs.peerLock.Unlock()

	peer, ok := fs.peers[from]
	return peer, ok
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}

func (fs *FileServer) GetData(key string) (io.Reader, error) {
	if fs.storage.Has(fs.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n", fs.Transport.Addr(), key)
		_, r, err := fs.storage.Read(fs.ID, key)
		return r, err
	}

	fmt.Printf("[%s] does not have file (%s) locally, fetching from network...\n", fs.Transport.Addr(), key)

	message := Message{
		Payload: MessageGetFile{
			ID:  fs.ID,
			Key: key,
		},
	}

	if err := fs.broadcast(&message); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 300)

	var fetched bool
	for _, peer := range fs.peersSnapshot() {
		// First read the file size, so we can limit the amount of bytes we
		// read from the connection, to prevent it from hanging.
		// A negative size means the peer does not have the file.
		var fileSize int64
		if err := binary.Read(peer, binary.LittleEndian, &fileSize); err != nil {
			continue
		}

		if fileSize < 0 {
			peer.CloseStream()
			continue
		}

		// Drain every response so no peer is left waiting on an open stream,
		// even if we already fetched the file from another peer.
		limited := &io.LimitedReader{R: peer, N: fileSize}
		if !fetched {
			n, err := fs.storage.WriteDecrypt(fs.EncKey, fs.ID, key, limited)
			if err != nil {
				io.Copy(io.Discard, limited)
				peer.CloseStream()
				return nil, err
			}
			fmt.Printf("[%s] received (%d) bytes over the network from (%s)\n", fs.Transport.Addr(), n, peer.RemoteAddr())
			fetched = true
		}
		io.Copy(io.Discard, limited)
		peer.CloseStream()
	}

	if !fetched {
		return nil, fmt.Errorf("file (%s) was not found on any peer", key)
	}

	_, r, err := fs.storage.Read(fs.ID, key)
	return r, err
}

func (fs *FileServer) StoreData(id, key string, r io.Reader) error {
	// 1. Store  file to disk
	// 2.Broadcast file to all known peers on the network

	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	size, err := fs.storage.Write(id, key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			ID:   id,
			Key:  key,
			Size: size + 16,
		},
	}

	if err := fs.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 5)

	peers := []io.Writer{}
	for _, peer := range fs.peersSnapshot() {
		peers = append(peers, peer)
	}

	mulWriter := io.MultiWriter(peers...)
	mulWriter.Write([]byte{p2p.IncomingStream})
	n, err := copyEncrypt(fs.EncKey, fileBuffer, mulWriter)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] received and written (%d) bytes to disk: ", fs.Transport.Addr(), n)

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
	peer, ok := fs.getPeer(from)
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	// Always send the incoming stream marker first. The requester reads a size
	// from every peer it asked, so peers without the file must respond too.
	if err := peer.Send([]byte{p2p.IncomingStream}); err != nil {
		return err
	}

	if !fs.storage.Has(msg.ID, msg.Key) {
		// Negative size tells the requester this peer does not have the file.
		return binary.Write(peer, binary.LittleEndian, int64(-1))
	}

	fmt.Printf("[%s] serving file (%s) over the network\n", fs.Transport.Addr(), msg.Key)
	fileSize, r, err := fs.storage.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		defer rc.Close()
	}

	// Send the file size as an int64, then the file contents.
	if err := binary.Write(peer, binary.LittleEndian, fileSize); err != nil {
		return err
	}

	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}
	fmt.Printf("[%s] written %d bytes over the network to %s\n", fs.Transport.Addr(), n, from)

	return nil
}

func (fs *FileServer) handleMessageStoreFile(from string, message MessageStoreFile) error {
	peer, ok := fs.getPeer(from)
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := fs.storage.Write(message.ID, message.Key, io.LimitReader(peer, message.Size))
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
			fmt.Printf("[%s] attempting to connect with remote %s\n", fs.Transport.Addr(), addr)
			if err := fs.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}

	return nil
}

func (fs *FileServer) Start() error {
	fmt.Printf("[%s] starting file server...\n", fs.Transport.Addr())
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
