package node

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

func randomN(arr []string, n int) []string {
	if n >= len(arr) {
		return arr
	}

	shuffled := make([]string, len(arr))
	copy(shuffled, arr)

	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled[:n]
}

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

func (v Version) MoreOrEqual(other Version) bool {
	if v.Counter != other.Counter {
		return v.Counter > other.Counter
	}
	return v.NodeID >= other.NodeID
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu sync.Mutex

	allNodeIDs          []string
	allNodesWithoutSelf []string

	state MapState

	gossipVersion          uint64
	receivedGossipVersions map[string]uint64
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	n := &CRDTMapNode{
		BaseNode:               hive.NewBaseNode(id),
		allNodeIDs:             allNodeIDs,
		state:                  make(MapState),
		gossipVersion:          0,
		receivedGossipVersions: make(map[string]uint64),
	}

	n.allNodesWithoutSelf = make([]string, 0, len(n.allNodeIDs)-1)
	for _, id := range n.allNodeIDs {
		if id != n.ID() {
			n.allNodesWithoutSelf = append(n.allNodesWithoutSelf, id)
		}
	}
	return n
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	fmt.Printf("Node %s starting\n", n.ID())
	n.BaseNode.Start(ctx)
	go n.gossipCoroutine()
	return nil
}

func (n *CRDTMapNode) gossipCoroutine() {
	nodeN := 0
	for {
		time.Sleep(500 * time.Millisecond)
		n.mu.Lock()
		if len(n.state) == 0 {
			fmt.Printf("Node %s has empty state, skipping gossip\n", n.ID())
			time.Sleep(100 * time.Millisecond)
			n.mu.Unlock()
			continue
		}
		n.gossipVersion++
		n.receivedGossipVersions[n.ID()] = n.gossipVersion
		n.mu.Unlock()

		nodeN = rand.IntN(len(n.allNodeIDs)/2) + 1 // [1, len(n.allNodeIDs)/2]
		peer := randomN(n.allNodesWithoutSelf, nodeN)

		fmt.Printf("Node %s gossiping version %d len(%d) peer(%v) isRunning: %t\n", n.ID(), n.gossipVersion, len(n.state), peer, n.IsRunning())

		for _, id := range peer {
			if id == n.ID() {
				continue
			}
			m := hive.NewMessage(n.ID(), id, n.State())
			m.Metadata["from"] = n.ID()
			m.Metadata["version"] = n.gossipVersion
			m.Metadata["type"] = "gossip"
			err := n.SendMessage(m)
			if err != nil {
				fmt.Printf("Node %s failed to send gossip to %s: %v\n", n.ID(), id, err)
			}
		}
	}
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	cur, exists := n.state[k]
	if !exists {
		cur = StateEntry{
			Value:     "",
			Tombstone: false,
			Version: Version{
				Counter: 0,
				NodeID:  n.ID(),
			},
		}
	}

	cur.Value = v
	cur.Tombstone = false
	cur.Version.Counter++
	n.state[k] = cur
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	cur, exists := n.state[k]
	if !exists || cur.Tombstone {
		return "", false
	}
	return cur.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	cur, exists := n.state[k]
	if !exists {
		cur = StateEntry{
			Value:     "",
			Tombstone: false,
			Version: Version{
				Counter: 0,
				NodeID:  n.ID(),
			},
		}
	}

	cur.Value = ""
	cur.Tombstone = true
	cur.Version.Counter++
	n.state[k] = cur
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("Node %s merging remote state version len(%d)\n", n.ID(), len(remote))
	for k, remoteEntry := range remote {
		localEntry, exists := n.state[k]
		if !exists {
			fmt.Printf("Node %s merging new key %s version %d from remote node %s\n", n.ID(), k, remoteEntry.Version.Counter, remoteEntry.Version.NodeID)
			n.state[k] = remoteEntry
		} else {
			if remoteEntry.Version.MoreOrEqual(localEntry.Version) {
				fmt.Printf("Node %s merging key %s remote version %d local version %d from remote node %s\n", n.ID(), k, remoteEntry.Version.Counter, localEntry.Version.Counter, remoteEntry.Version.NodeID)
				n.state[k] = remoteEntry
			}
		}
	}
	fmt.Printf("Node %s merge complete, state len(%d) state %v\n", n.ID(), len(n.state), n.state)
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.Lock()
	defer n.mu.Unlock()

	stateCopy := make(MapState)
	for k, v := range n.state {
		stateCopy[k] = v
	}
	return stateCopy
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	result := make(map[string]string)
	for k, v := range n.state {
		if !v.Tombstone {
			result[k] = v.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	if msg.Metadata["type"] == "gossip" {
		return n.handleGossip(msg)
	}

	return nil
}

func (n *CRDTMapNode) handleGossip(msg *hive.Message) error {
	remoteVersion, ok := msg.Metadata["version"].(uint64)
	if !ok {
		return ErrInternal
	}
	from, ok := msg.Metadata["from"].(string)
	if !ok {
		return ErrInternal
	}
	m, ok := msg.Payload.(MapState)
	if !ok {
		return ErrInternal
	}

	fmt.Printf("Node %s received gossip from %s version %d len(%d)\n", n.ID(), from, remoteVersion, len(m))

	n.mu.Lock()
	if n.receivedGossipVersions[from] >= remoteVersion {
		n.mu.Unlock()
		return nil
	}
	n.receivedGossipVersions[from] = remoteVersion
	n.mu.Unlock()
	n.Merge(m)

	nodeN := rand.IntN(len(n.allNodeIDs)/2) + 1 // [1, len(n.allNodeIDs)/2]

	peer := randomN(n.allNodesWithoutSelf, nodeN)

	fmt.Printf("Node %s received gossip from %s version %d, forwarding to peer(%v)\n", n.ID(), from, remoteVersion, peer)

	for _, id := range peer {
		if id == n.ID() {
			continue
		}
		m := hive.NewMessage(n.ID(), id, n.State())
		m.Metadata["from"] = msg.Metadata["from"]
		m.Metadata["version"] = msg.Metadata["version"]
		m.Metadata["type"] = msg.Metadata["type"]
		if err := n.SendMessage(m); err != nil {
			return err
		}
	}
	return nil
}
