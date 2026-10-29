package p2p

// holds any arbitrary data being sent over each
// transport between two nodes on the network
type RPC struct {
	From    string
	Payload []byte
}
