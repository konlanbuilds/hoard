package main

import (
	"fmt"
	"log"

	"github.com/konlanbuilds/hoard/p2p"
)

type FileServerOptions struct {
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
}

type FileServer struct {
	FileServerOptions
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
	}
}

func (fs *FileServer) Quit() {
	close(fs.quickChan)
}

func (fs *FileServer) loop() {

	defer func() {
		log.Println("file server stopped by user.")
		fs.Transport.Close()
	}()
	for {
		select {
		case msg := <-fs.Transport.Consume():
			fmt.Println(msg)
		case <-fs.quickChan:
			return

		}
	}
}

func (fs *FileServer) Start() error {
	if err := fs.Transport.ListenAndAccept(); err != nil {
		return err
	}

	fs.loop()
	return nil
}
