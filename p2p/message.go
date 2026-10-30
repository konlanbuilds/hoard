package p2p

const (
	IncomingStream  = 0x2
	IncomingMessage = 0x1
)

// holds any arbitrary data being sent over each
// transport between two nodes on the network
type RPC struct {
	From    string
	Payload []byte
	Stream  bool
}
