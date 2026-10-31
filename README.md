# Hoard

Hoard is a peer-to-peer file storage system written in Go. It combines
content-addressed local storage with a TCP transport layer so multiple nodes
can exchange and replicate files without requiring a centralized storage
service.

The project is intentionally focused on the fundamentals of distributed
systems: storage layout, streaming I/O, peer connections, message framing,
replication, and failure handling. The implementation is small enough to
inspect end to end while providing a foundation for exploring production
storage concerns.

## Features

- Content-addressed file layout using SHA-256-derived paths
- Local streaming reads and writes
- Peer-to-peer TCP connections
- Explicit peer bootstrapping
- File announcements and replication to connected peers
- On-demand retrieval of files from peers
- Streaming encrypted transfers between nodes
- Pluggable transport, decoder, handshake, and path-transformation
  abstractions
- Concurrent peer management with safe peer snapshots
- Tests for storage, cryptography, encoding, and TCP transport behavior

## Architecture

Hoard is organized into two primary layers:

```text
FileServer
  ├── Storage       Local file persistence and content-addressed paths
  └── Transport     Peer connections and framed network messages
```

### File server

[`file_server.go`](./file_server.go) coordinates storage and replication. It
tracks connected peers, broadcasts file operations, handles requests for
missing files, and streams file contents without loading entire files into
memory.

### Storage

[`storage.go`](./storage.go) owns local persistence. Each file is stored below
an explicit storage-owner ID and a transformed key path, allowing files from
different nodes or logical owners to coexist under the same storage root.

### Peer-to-peer transport

The [`p2p/`](./p2p/) package provides the transport interfaces and TCP
implementation. The transport performs connection acceptance, dialing,
handshake execution, message decoding, and stream coordination. The file
server depends on the transport interface rather than directly on TCP, making
alternative transports possible.

## Data flow

### Storing a file

1. The file server writes the file to local storage while streaming the input.
2. It broadcasts a metadata message to connected peers.
3. Each peer receives the encrypted stream and writes it to its own storage.

### Reading a file

1. The file server checks local storage first.
2. If the file is absent, it requests the file from connected peers.
3. Peers report whether they have the file and, when available, stream its
   size and contents.
4. The requesting node decrypts and stores the stream locally before returning
   it to the caller.

## Requirements

- Go 1.27.2 or newer

The required Go version is declared in [`go.mod`](./go.mod).

## Getting started

Clone the repository and run the test suite:

```bash
go test ./...
```

Run the verbose test target:

```bash
make test
```

Build the example binary:

```bash
make build
```

The binary is written to `bin/fs`. Run the example network with:

```bash
make run
```

The current example starts several local nodes and uses explicit bootstrap
addresses to establish connections. Each node writes to a separate directory,
such as `hoard_:3000_network/`.

## Repository layout

```text
.
├── file_server.go       # Replication, peer management, and message handling
├── storage.go           # Local content-addressed storage
├── crypto.go            # Streaming encryption and decryption
├── util.go              # Shared utilities, including ID generation
├── main.go              # Local multi-node example
├── p2p/
│   ├── transport.go     # Transport and peer interfaces
│   ├── tcp_transport.go # TCP transport implementation
│   ├── handshake.go     # Connection handshake abstraction
│   ├── encoding.go      # Network message decoding
│   └── message.go       # Transport message types
└── *_test.go            # Unit and transport tests
```

## Storage layout

By default, storage uses `hoardNetworkFiles` as its root. The example
configures a separate root for each node. A key is hashed with SHA-256, split
into five-character path segments, and written at the end of the path:

```text
<storage-root>/<owner-id>/<hash-part>/<hash-part>/.../<full-hash>
```

This spreads files across nested directories and avoids placing a large number
of entries in a single directory. The owner ID is supplied by the file server,
which keeps storage identity separate from the storage implementation.

## Security considerations

File contents transferred between peers are encrypted using the configured
encryption key. The current example uses a no-op handshake and is intended for
local development rather than untrusted networks. Production deployments
should provide authenticated peer handshakes, secure key distribution, access
control, and transport-level identity verification.

## Current limitations

Hoard is an active systems project and is not yet a production-ready
distributed filesystem. Current limitations include:

- Peer discovery requires configured bootstrap addresses.
- The default handshake does not authenticate peer identity.
- There is no durable metadata database or crash-recovery protocol.
- Replication does not yet provide quorum guarantees or conflict resolution.
- Peer liveness, reconnect backoff, and membership management are minimal.
- Configuration, metrics, tracing, and operational tooling are still limited.
- The example is a development program, not a deployment daemon.

## Roadmap

Planned work is organized around making the system more autonomous, reliable,
and operable:

1. **Automatic LAN peer discovery** using UDP multicast or broadcast, with
   announcements, peer expiry, duplicate suppression, and reconnect behavior.
2. **Authenticated peer identity** in the TCP handshake, including protocol
   version negotiation and connection deduplication.
3. **Reliable replication semantics** with acknowledgements, retry policy,
   conflict handling, and configurable replication targets.
4. **Durability and recovery** through write-ahead logging, metadata
   persistence, snapshots, and restart recovery.
5. **Operational visibility** with structured logging, metrics, tracing, health
   checks, and benchmark coverage.
6. **Production configuration and deployment** with explicit configuration
   files, graceful shutdown, resource limits, and documented deployment
   topologies.
7. **Broader networking support**, including rendezvous-based discovery for
   peers that are not on the same local network.

## Contributing

Keep changes focused and preserve the transport/storage separation. Before
opening a pull request, run:

```bash
gofmt -w *.go p2p/*.go
go test ./...
```

Changes that affect the wire protocol should include compatibility notes and
tests covering both sides of the connection.

## License

No license has been specified yet.
