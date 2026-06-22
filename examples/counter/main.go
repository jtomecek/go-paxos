// Package main implements a replicated distributed counter using Paxos.
//
// Every node maintains the same counter value. Increments and decrements are
// proposed through Paxos, so all nodes apply them in the same order and
// converge on the same total.
//
// Usage:
//
//	# Start 3 nodes in separate terminals
//	go run main.go -id 1 -addr localhost:9101 -peers localhost:9102,localhost:9103
//	go run main.go -id 2 -addr localhost:9102 -peers localhost:9101,localhost:9103
//	go run main.go -id 3 -addr localhost:9103 -peers localhost:9101,localhost:9102
//
// Then use the interactive commands:
//
//	add <n>   - add n to the counter (n may be negative)
//	inc       - shorthand for "add 1"
//	dec       - shorthand for "add -1"
//	get       - print the current counter value
//	status    - show node status
//	quit      - exit
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jtomecek/go-paxos/paxos"
)

// Delta represents a replicated change to the counter.
type Delta struct {
	Amount int64 `json:"amount"`
}

// Counter is a thread-safe integer counter updated via committed Paxos values.
type Counter struct {
	mu    sync.RWMutex
	value int64
}

// Apply applies a committed delta to the counter.
func (c *Counter) Apply(d Delta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += d.Amount
	fmt.Printf("[Applied] %+d -> %d\n", d.Amount, c.value)
}

// Value returns the current counter value.
func (c *Counter) Value() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// SimpleLogger implements paxos.Logger with a configurable verbosity level.
type SimpleLogger struct {
	level int // 0=none, 1=error, 2=warn, 3=info, 4=debug
}

func (l SimpleLogger) Debug(msg string, keysAndValues ...interface{}) {
	if l.level >= 4 {
		fmt.Printf("[DEBUG] %s %v\n", msg, keysAndValues)
	}
}
func (l SimpleLogger) Info(msg string, keysAndValues ...interface{}) {
	if l.level >= 3 {
		fmt.Printf("[INFO] %s %v\n", msg, keysAndValues)
	}
}
func (l SimpleLogger) Warn(msg string, keysAndValues ...interface{}) {
	if l.level >= 2 {
		fmt.Printf("[WARN] %s %v\n", msg, keysAndValues)
	}
}
func (l SimpleLogger) Error(msg string, keysAndValues ...interface{}) {
	if l.level >= 1 {
		fmt.Printf("[ERROR] %s %v\n", msg, keysAndValues)
	}
}

func main() {
	nodeID := flag.Int("id", 1, "Node ID (1-65535)")
	addr := flag.String("addr", "localhost:9101", "Listen address")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses")
	logLevel := flag.Int("log", 2, "Log level (0=none, 1=error, 2=warn, 3=info, 4=debug)")
	flag.Parse()

	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}
	if len(peers) == 0 {
		fmt.Println("Warning: No peers specified. Running as single node.")
	}

	logger := SimpleLogger{level: *logLevel}

	cfg := paxos.Config{
		NodeID:            paxos.NodeID(*nodeID),
		Address:           *addr,
		Peers:             peers,
		Logger:            logger,
		HeartbeatInterval: 200 * time.Millisecond,
		ElectionTimeout:   1 * time.Second,
	}

	node, err := paxos.NewNode(cfg)
	if err != nil {
		fmt.Printf("Failed to create node: %v\n", err)
		os.Exit(1)
	}

	counter := &Counter{}

	if err := node.Start(); err != nil {
		fmt.Printf("Failed to start node: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node %d started on %s\n", *nodeID, *addr)
	fmt.Printf("Peers: %v\n", peers)
	fmt.Println("\nCommands:")
	fmt.Println("  add <n> - Add n to the counter (n may be negative)")
	fmt.Println("  inc     - Add 1")
	fmt.Println("  dec     - Subtract 1")
	fmt.Println("  get     - Show the current value")
	fmt.Println("  status  - Show node status")
	fmt.Println("  quit    - Exit")
	fmt.Println()

	// Apply committed deltas in the background as Paxos commits them.
	go func() {
		for cv := range node.Subscribe() {
			var d Delta
			if err := json.Unmarshal(cv.Value, &d); err != nil {
				logger.Error("Failed to unmarshal delta", "error", err)
				continue
			}
			counter.Apply(d)
		}
	}()

	// propose replicates a delta through Paxos and reports the outcome.
	propose := func(amount int64) {
		data, _ := json.Marshal(Delta{Amount: amount})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		instance, err := node.Propose(ctx, data)
		cancel()
		if err != nil {
			fmt.Printf("Failed to apply %+d: %v\n", amount, err)
		} else {
			fmt.Printf("Committed %+d at instance %d\n", amount, instance)
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "add":
			if len(parts) < 2 {
				fmt.Println("Usage: add <n>")
				break
			}
			n, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Printf("Invalid number: %s\n", parts[1])
				break
			}
			propose(n)

		case "inc":
			propose(1)

		case "dec":
			propose(-1)

		case "get":
			fmt.Printf("counter = %d\n", counter.Value())

		case "status":
			fmt.Printf("Node ID: %d\n", *nodeID)
			fmt.Printf("Address: %s\n", *addr)
			fmt.Printf("Is Leader: %v\n", node.IsLeader())
			fmt.Printf("Last Committed: %d\n", node.LastCommitted())

		case "quit", "exit":
			fmt.Println("Shutting down...")
			node.Close()
			return

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}

		fmt.Print("> ")
	}
}
