package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	smoke := flag.Bool("smoke-test", false, "run the built-in self-check and exit")
	flag.Parse()
	if *smoke {
		os.Exit(runSmokeTest())
	}
	fmt.Println("consistent hash ring: run with --smoke-test to self-check")
}

// mustRing constructs a ring or terminates the smoke test on invalid input.
func mustRing(replicas int) *Ring {
	r, err := NewRing(replicas)
	if err != nil {
		fmt.Fprintf(os.Stderr, "smoke-test: ring construct failed: %v\n", err)
		os.Exit(1)
	}
	return r
}

// runSmokeTest exercises the ring's invariants deterministically: multi-node
// distribution, minimal migration on node removal, empty-ring error semantics,
// and the wraparound path. It performs no timing and uses no randomness.
func runSmokeTest() int {
	// 1. Keys spread across all nodes.
	r := mustRing(150)
	for _, n := range []string{"alpha", "beta", "gamma"} {
		r.Add(n)
	}
	const nKeys = 100
	keys := make([]string, 0, nKeys)
	owners := make(map[string]string, nKeys)
	counts := make(map[string]int)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("user-%04d", i)
		node, err := r.Get(k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "smoke-test: get %q failed: %v\n", k, err)
			return 1
		}
		keys = append(keys, k)
		owners[k] = node
		counts[node]++
	}
	if len(counts) != 3 {
		fmt.Fprintf(os.Stderr, "smoke-test: expected keys on 3 nodes, got %d (%v)\n", len(counts), counts)
		return 1
	}
	for _, n := range []string{"alpha", "beta", "gamma"} {
		if counts[n] == 0 {
			fmt.Fprintf(os.Stderr, "smoke-test: node %q served no keys\n", n)
			return 1
		}
	}

	// 2. Remove one node: it must serve nothing afterward; keys it did not own
	//    must keep their owner.
	r.Remove("beta")
	for _, k := range keys {
		node, err := r.Get(k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "smoke-test: get after remove %q failed: %v\n", k, err)
			return 1
		}
		if node == "beta" {
			fmt.Fprintf(os.Stderr, "smoke-test: removed node beta still serves key %q\n", k)
			return 1
		}
		if owners[k] != "beta" && node != owners[k] {
			fmt.Fprintf(os.Stderr, "smoke-test: key %q migrated %q -> %q though beta was not its owner\n", k, owners[k], node)
			return 1
		}
	}

	// 3. Empty ring returns an error mentioning "empty".
	empty := mustRing(150)
	if _, err := empty.Get("anything"); err == nil {
		fmt.Fprintln(os.Stderr, "smoke-test: empty ring get returned no error")
		return 1
	} else if !strings.Contains(err.Error(), "empty") {
		fmt.Fprintf(os.Stderr, "smoke-test: empty ring error should mention 'empty', got %q\n", err.Error())
		return 1
	}

	// 4. Wraparound: a one-node ring must answer a key whose hash exceeds the
	//    ring's maximum address. A deterministic scan finds such a key; no
	//    randomness is involved, so the result is stable across runs.
	solo := mustRing(150)
	solo.Add("solo")
	maxHash := solo.maxHash()
	wrapKey := ""
	for i := 0; i < 100000; i++ {
		cand := "wrap-" + strconv.Itoa(i)
		if solo.hashOf(cand) > maxHash {
			wrapKey = cand
			break
		}
	}
	if wrapKey == "" {
		fmt.Fprintln(os.Stderr, "smoke-test: could not synthesize a wraparound key")
		return 1
	}
	node, err := solo.Get(wrapKey)
	if err != nil || node != "solo" {
		fmt.Fprintf(os.Stderr, "smoke-test: wraparound get returned (%q, %v), want (solo, nil)\n", node, err)
		return 1
	}

	fmt.Println("smoke-test: OK")
	return 0
}
