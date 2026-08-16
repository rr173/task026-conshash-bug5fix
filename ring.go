package main

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

const (
	minReplicas = 1
	maxReplicas = 1000
)

// HashFunc maps a key to a point on the 32-bit hash ring. The same function is
// applied to a virtual-node key ("node#i") when placing a node and to a bare
// lookup key when answering Get.
type HashFunc func(string) uint32

// Ring is a consistent hashing ring that maps arbitrary keys to physical
// nodes. It is safe for concurrent use.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	hash     HashFunc
	ring     []uint32          // sorted, duplicate-free virtual node addresses
	hashMap  map[uint32]string // virtual address -> physical node name
	nodes    map[string]bool   // physical nodes currently on the ring
}

// NewRing creates an empty ring using a default FNV-1a hash. replicas is the
// number of virtual nodes per physical node and must be in [1, 1000].
func NewRing(replicas int) (*Ring, error) {
	return newRing(replicas, defaultHash)
}

// newRing creates a ring with a caller-supplied hash function (used by tests).
func newRing(replicas int, hash HashFunc) (*Ring, error) {
	if replicas < minReplicas || replicas > maxReplicas {
		return nil, fmt.Errorf("replicas must be in [%d, %d], got %d", minReplicas, maxReplicas, replicas)
	}
	if hash == nil {
		return nil, errors.New("hash function must not be nil")
	}
	return &Ring{
		replicas: replicas,
		hash:     hash,
		hashMap:  make(map[uint32]string),
		nodes:    make(map[string]bool),
	}, nil
}

// defaultHash maps a key to a 32-bit ring address using SHA-256. Unlike FNV,
// SHA-256 has a strong avalanche effect, so virtual-node keys that share a
// prefix and differ only by a sequential suffix ("alpha#0", "alpha#1", ...)
// still land at uniformly distributed, uncorrelated ring addresses.
func defaultHash(s string) uint32 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

// virtualKey builds the string that is hashed for the i-th virtual node of node.
func virtualKey(node string, i int) string {
	return node + "#" + strconv.Itoa(i)
}

// Add inserts a physical node into the ring. Adding a node that already exists
// is idempotent: it does not add duplicate virtual points. When a virtual
// point collides with an address already on the ring (from another node), the
// new node simply skips that point, keeping each ring address uniquely mapped.
func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes[node] {
		return
	}
	for i := 0; i < r.replicas; i++ {
		h := r.hash(virtualKey(node, i))
		if _, exists := r.hashMap[h]; !exists {
			r.ring = append(r.ring, h)
			r.hashMap[h] = node
		}
	}
	r.nodes[node] = true
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

// Remove deletes a physical node and all of its virtual points from the ring.
// It returns an error if the node is not present.
func (r *Ring) Remove(node string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.nodes[node] {
		return fmt.Errorf("node %q not found", node)
	}
	kept := make([]uint32, 0, len(r.ring))
	for _, h := range r.ring {
		if r.hashMap[h] == node {
			delete(r.hashMap, h)
			continue
		}
		kept = append(kept, h)
	}
	r.ring = kept
	delete(r.nodes, node)
	return nil
}

// Get returns the physical node responsible for key. If the ring has no nodes
// it returns an error whose message contains "empty".
func (r *Ring) Get(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return "", errors.New("ring empty: no nodes available")
	}
	kh := r.hash(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= kh })
	if idx == len(r.ring) {
		// The key hash is larger than every ring address: wrap around to the
		// smallest address on the ring.
		idx = 0
	}
	return r.hashMap[r.ring[idx]], nil
}

// Nodes returns the sorted names of the physical nodes currently on the ring.
func (r *Ring) Nodes() []string {
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// maxHash returns the largest virtual address on the ring, or 0 if the ring is
// empty. Used by the smoke test to exercise the wraparound path.
func (r *Ring) maxHash() uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return 0
	}
	return r.ring[len(r.ring)-1]
}

// hashOf returns the ring's hash of a key. Used by the smoke test to synthesize
// a key whose hash exceeds the ring maximum.
func (r *Ring) hashOf(key string) uint32 {
	return r.hash(key)
}
