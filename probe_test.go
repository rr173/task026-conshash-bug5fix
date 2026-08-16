package main

import (
	"fmt"
	"sync"
	"testing"
)

// TestProbeMaxReplicasAccepted verifies that the maximum documented replica
// count (1000) is accepted by the constructor without error.
func TestProbeMaxReplicasAccepted(t *testing.T) {
	_, err := NewRing(1000)
	if err != nil {
		t.Fatalf("NewRing(1000) returned error: %v; max replica count should be accepted", err)
	}
}

// TestProbeCollisionSkipsExisting verifies that when two nodes' virtual keys
// hash to the same address, the first node's ownership is preserved and the
// second node skips that slot rather than overwriting it.
func TestProbeCollisionSkipsExisting(t *testing.T) {
	collide := func(s string) uint32 {
		switch s {
		case "X#0":
			return 500
		case "X#1":
			return 600
		case "Y#0":
			return 500 // collision with X#0
		case "Y#1":
			return 700
		case "exact":
			return 500 // lands exactly on the collision address
		}
		return 0
	}
	r, _ := newRing(2, collide)
	r.Add("X")
	r.Add("Y")
	// A key hashing exactly to address 500 must resolve to X (first owner), not Y.
	got, err := r.Get("exact")
	if err != nil {
		t.Fatal(err)
	}
	if got != "X" {
		t.Errorf("Get(exact) = %q; want X (collision should preserve first owner)", got)
	}
}

// TestProbeRemoveMissingReturnsError verifies that removing a node that was
// never added returns a non-nil error.
func TestProbeRemoveMissingReturnsError(t *testing.T) {
	r, _ := newRing(3, controlHash)
	r.Add("A")
	err := r.Remove("Z")
	if err == nil {
		t.Fatal("Remove of non-existent node should return error, got nil")
	}
}

// TestProbeWraparoundToSmallest verifies that a key whose hash exceeds all
// virtual-node addresses wraps around to the smallest ring address (the first
// node in sorted order), not the largest.
func TestProbeWraparoundToSmallest(t *testing.T) {
	h := func(s string) uint32 {
		switch s {
		case "N#0":
			return 100
		case "N#1":
			return 200
		case "wrap-key":
			return 999 // > max address 200, should wrap to 100 -> N
		}
		return 0
	}
	r, _ := newRing(2, h)
	r.Add("N")
	got, err := r.Get("wrap-key")
	if err != nil {
		t.Fatal(err)
	}
	if got != "N" {
		t.Fatalf("wraparound Get = %q; want N", got)
	}
}

// TestProbeConcurrentAddNodes verifies that calling Add and Nodes concurrently
// does not panic from unsynchronized map access.
func TestProbeConcurrentAddNodes(t *testing.T) {
	r, _ := NewRing(10)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			r.Add(fmt.Sprintf("node-%d", i%50))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			_ = r.Nodes()
		}
	}()
	wg.Wait()
}
