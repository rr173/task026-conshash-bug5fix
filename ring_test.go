package main

import (
	"strconv"
	"strings"
	"sync"
	"testing"
)

// controlHash deterministically maps known virtual-node keys ("NAME#i") and
// lookup keys to fixed ring addresses so tests can assert exact placement and
// the wraparound path. Unknown strings hash to 0.
func controlHash(s string) uint32 {
	switch s {
	// virtual nodes for node "A" (replicas=3)
	case "A#0":
		return 10
	case "A#1":
		return 20
	case "A#2":
		return 30
	// virtual nodes for node "B" (replicas=3)
	case "B#0":
		return 50
	case "B#1":
		return 70
	case "B#2":
		return 90
	// lookup keys
	case "K1":
		return 100 // > max(90) -> wrap to ring[0]=10 -> A
	case "K2":
		return 5 // < min(10) -> A
	case "K3":
		return 60 // between 50 and 70 -> first >= is 70 -> B
	case "K4":
		return 30 // == 30 -> A (exact match)
	}
	return 0
}

func TestNewRingReplicasValidation(t *testing.T) {
	for _, rep := range []int{-1, 0, 1001, 5000} {
		if _, err := newRing(rep, controlHash); err == nil {
			t.Errorf("newRing(%d) expected error, got nil", rep)
		}
	}
	for _, rep := range []int{1, 500, 1000} {
		if _, err := newRing(rep, controlHash); err != nil {
			t.Errorf("newRing(%d) unexpected error: %v", rep, err)
		}
	}
	if _, err := newRing(150, nil); err == nil {
		t.Error("newRing with nil hash expected error")
	}
}

func TestEmptyRingGet(t *testing.T) {
	r, _ := newRing(3, controlHash)
	_, err := r.Get("K1")
	if err == nil {
		t.Fatal("Get on empty ring expected error, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty ring error should mention 'empty', got %q", err.Error())
	}
}

func TestPlacementAndWraparound(t *testing.T) {
	r, _ := newRing(3, controlHash)
	r.Add("A")
	r.Add("B")
	// ring sorted: [10, 20, 30, 50, 70, 90]
	cases := []struct {
		key  string
		want string
	}{
		{"K1", "A"}, // hash 100 > 90 -> wrap to 10 -> A
		{"K2", "A"}, // hash 5 < 10 -> A
		{"K3", "B"}, // hash 60 -> next >= is 70 -> B
		{"K4", "A"}, // hash 30 == 30 -> A
	}
	for _, c := range cases {
		got, err := r.Get(c.key)
		if err != nil {
			t.Fatalf("Get(%q) unexpected error: %v", c.key, err)
		}
		if got != c.want {
			t.Errorf("Get(%q) = %q, want %q", c.key, got, c.want)
		}
	}
}

func TestMinimalMigrationOnRemove(t *testing.T) {
	r, _ := newRing(3, controlHash)
	r.Add("A")
	r.Add("B")
	keys := []string{"K1", "K2", "K3", "K4"}
	before := make(map[string]string, len(keys))
	for _, k := range keys {
		node, _ := r.Get(k)
		before[k] = node
	}
	if err := r.Remove("B"); err != nil {
		t.Fatalf("Remove(B) error: %v", err)
	}
	for _, k := range keys {
		got, err := r.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) after remove error: %v", k, err)
		}
		if got == "B" {
			t.Fatalf("Get(%q) still returns removed node B", k)
		}
		if before[k] != "B" && got != before[k] {
			t.Errorf("key %q migrated %q -> %q though beta removal should not affect it", k, before[k], got)
		}
		if before[k] == "B" && got != "A" {
			t.Errorf("key %q was on B, expected to move to A, got %q", k, got)
		}
	}
}

func TestRemoveMissingNode(t *testing.T) {
	r, _ := newRing(3, controlHash)
	if err := r.Remove("nope"); err == nil {
		t.Error("Remove of missing node expected error, got nil")
	}
}

func TestAddIdempotent(t *testing.T) {
	r, _ := newRing(3, controlHash)
	r.Add("A")
	before := len(r.ring)
	r.Add("A") // idempotent: no duplicate virtual points
	after := len(r.ring)
	if before != after {
		t.Errorf("idempotent Add changed ring size: before=%d after=%d", before, after)
	}
	node, err := r.Get("K2")
	if err != nil || node != "A" {
		t.Errorf("after idempotent Add, Get(K2) = (%q, %v), want (A, nil)", node, err)
	}
}

func TestCollisionDoesNotCorrupt(t *testing.T) {
	// C#0 collides with A#0 (both hash 100). C must skip that slot, keeping the
	// ring address uniquely mapped; removing C must not leave A's slot dangling.
	collide := func(s string) uint32 {
		switch s {
		case "A#0":
			return 100
		case "A#1":
			return 200
		case "C#0":
			return 100 // collision with A#0
		case "C#1":
			return 300
		case "KEY":
			return 250 // > 200, first >= is 300 -> C
		}
		return 0
	}
	r, _ := newRing(2, collide)
	r.Add("A") // ring [100,200], both -> A
	r.Add("C") // C#0 collides -> skip; C#1=300 added. ring [100,200,300]
	got, err := r.Get("KEY")
	if err != nil || got != "C" {
		t.Errorf("Get(KEY) = (%q, %v), want (C, nil)", got, err)
	}
	if err := r.Remove("C"); err != nil {
		t.Fatalf("Remove(C) error: %v", err)
	}
	// ring now [100,200], both -> A. KEY(250) -> wrap to 100 -> A.
	got, err = r.Get("KEY")
	if err != nil || got != "A" {
		t.Errorf("after Remove(C), Get(KEY) = (%q, %v), want (A, nil)", got, err)
	}
}

func TestConcurrentNoPanic(t *testing.T) {
	r, _ := newRing(50, defaultHash)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				node := "n" + strconv.Itoa((id*200+j)%6)
				r.Add(node)
				_, _ = r.Get("k" + strconv.Itoa(j))
				_ = r.Remove(node)
			}
		}(i)
	}
	wg.Wait()
}
