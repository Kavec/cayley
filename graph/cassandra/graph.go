package cassandra

import (
	"github.com/dgryski/go-spooky" // NOT FOR CRYPTOGRAPHIC USE
	"github.com/gocql/gocql"
)

type linkID [4]nodeID

type Link struct {
	id    string
	birth time.Time
	dead  bool
}

type Node struct {
	id   string
	data string

	sub [][4]*Node
	obj [][4]*Node
	pre [][4]*Node
	lab [][4]*Node
}

// ValueOf generates a new Value token based on node--
// nb that this doesn't guarantee that Value has a proper
// link to the graph, since ValueOf eschews hitting the
// database by instead just hashing the string into a Value
func (cs *Cassandra) ValueOf(node string) graph.Value {
	return &Node{
		id:   hashNode(node),
		data: string,
	}
}

// NameOf returns the node string of a Value
func (cs *Cassandra) NameOf(ifce graph.Value) string {

}

// Quad converts an opaque value token into a quad.Quad
func (cs *Cassandra) Quad(ifce graph.Value) quad.Quad {

}

// QuadDirection takes in an opaque value token, a direction, and spits out the
// Value at that position iff ifce is a valid quad. Elsewise, it logs under infof
// and returns the original value passed in
func (cs *Cassandra) QuadDirection(ifce graph.Value, d quad.Direction) graph.Value {
	val := ifce.(isQuader)
	switch v := ifce.(type) {
	case Quad:
		// Extract the nodeID from the quadID
		var idStart, idEnd int = int((d - 1) * 24), int(d * 24)
		// And then spit out a constructed value from that ID
		return cs.ValueOf(val.(Quad).id[idStart:idEnd])

	case *Node:
		glog.V(2).Infof("QuadDirection received node instead of quad; node: %#v\n", val.(*Node))
		return val

	default:
		panic(fmt.Sprintf("Received bad value type in C*//QuadDirection: <%T> %#v", v, v))
	}
}

// ------------------------------------------ Internal ------------------------------------------ //

// hashNode generates new hashed node IDs from an input string using
// SpookyHash. SpookyHash was chosen for its high speed and fairly
// good randomness properties; like any hash function, there may
// be collisions-- hopefully paths and nodes will be disentangled
// enough that isn't a huge problem in practice. Being able to
// compute new IDs quickly on our side is a huge win for enabling
// async/concurrent queries.
//
// That said, future work may/will be needed to shore up this
// and handle collisions more gracefully than via hope and prayer.
//
// And remember: Is your hash function fast? Then it's
// NOT FOR CRYPTOGRAPHIC USE. Please don't use this to hash
// passwords or anything that should be kept secure. Instead,
// use scrypt or PBKDF (and then scare up an ID from here)
func hashNode(n string) string {
	if n == "" {
		return ""
	}
	// See http://www.burtleburtle.net/bob/c/spooky.h for more info
	var hash1, hash2 uint64

	b := []byte(n)
	// Spooky.h suggests Short() is best for inputs of < 192 bytes
	// long-- TODO: Benchmark to see if go-spooky is similar
	if len(b) < 192 {
		spooky.Short(b, &hash1, &hash2)
	} else {
		spooky.Hash128(b, &hash1, &hash2)
	}

	nBuf := make([]byte, 16)
	binary.BigEndian.PutUint64(nBuf[8:], hash1)
	binary.BigEndian.PutUint64(nBuf[:8], hash2)

	// base64 encoding the output will inflate 16 bytes to
	// 24 (oh no), but makes the ID easier to handle across
	// the board-- unless profiling shows otherwise, the extra
	// uint64 worth of memory probably isn't worth optimizing away
	return base64.URLEncoding.EncodeToString(nBuf)
}

// hashQuad takes a quad and smashes all the hashed nodes together to
// generate the proper ID
func hashQuad(q quad.Quad) string {
	if !q.Valid() {
		return ""
	}

	qBuf := []string{
		hashNode(q.Get(graph.Subject)),
		hashNode(q.Get(graph.Predicate)),
		hashNode(q.Get(graph.Object)),
		hashNode(q.Get(graph.Label)),
	}
	return strings.Join(qBuf, "")
}
