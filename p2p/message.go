package p2p

import "net"

// holds any arbitrary data being sent over each
// transport between two nodes on the network
type RPC struct {
	From    net.Addr
	Payload []byte
}
