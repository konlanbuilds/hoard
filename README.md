# Hoard

Hoard is a small peer-to-peer file storage experiment written in Go. It stores
files locally using a content-addressed layout and can share them with other
Hoard nodes over TCP.

The project is intentionally simple at the moment. It is a place to explore
the pieces of a distributed file store—storage, peer connections, message
encoding, and replication—without hiding the mechanics behind a large
framework.

## What it does

- Stores data under a key on disk.
- Uses SHA-1-derived paths to spread files across nested directories.
- Connects nodes over TCP.
- Broadcasts a file announcement and streams the file to connected peers.
- Keeps the transport layer separate from the file-server logic.

## Requirements

- Go 1.27.2 or newer

The Go version above is the version declared in [`go.mod`](./go.mod).

## Getting started

Clone the repository and run the test suite:

```bash
go test ./... -v
```

Or use the Makefile:

```bash
make test
```

Build the example binary:

```bash
make build
```

This writes the binary to `bin/fs`. To start the current two-node example:

```bash
make run
```

The example starts one node on `:3000` and another on `:4000`. Each node writes
its files beneath a directory named after its listen address, such as
`hoard_:3000_network/`.

## How it is organized

```text
.
├── main.go              # Starts the example two-node network
├── file_server.go       # File server, replication, and message handling
├── storage.go           # Local content-addressed storage
├── p2p/                 # TCP transport and wire-level types
├── storage_test.go
└── p2p/*_test.go
```

`Storage` is responsible for writing, reading, checking, deleting, and clearing
files. `FileServer` combines storage with a transport and tracks connected
peers. The `p2p` package provides the transport interface and its TCP
implementation, so another transport can be added without changing the
storage API.

## Storage layout

By default, storage uses `hoardNetworkFiles` as its root directory. The example
configures a separate root for each node. A key is hashed with SHA-1, split
into five-character path segments, and written at the end of that path:

```text
<storage-root>/<hash-part>/<hash-part>/.../<full-hash>
```

For example, the key `momsbestpictures` is stored using the hash
`52439f5e49ba33b4b9b2373dcd6bfc9e97cafb7f`.

## Current status

Hoard is still a work in progress. Local storage and the basic TCP connection
flow are implemented, but the network read path is not complete yet:

- `StoreData` can announce and stream data to connected peers.
- `GetData` reads local data, but a missing file currently sends a request and
  waits indefinitely.
- The peer-side handler for a missing-file request is still a placeholder.
- The example in `main.go` currently requests `randomkey`, so `make run` is
  expected to remain running while that unfinished request waits.

These limitations are useful context if you are extending the project: the next
natural step is to implement the response path for `MessageGetFile`, then add
tests for fetching a file that exists on another node.

## License

No license has been specified yet.
