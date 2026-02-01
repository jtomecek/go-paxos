// Package main implements a simple distributed key-value store using Paxos.
//
// Usage:
//
//	# Start 3 nodes in separate terminals
//	go run main.go -id 1 -addr localhost:9001 -peers localhost:9002,localhost:9003
//	go run main.go -id 2 -addr localhost:9002 -peers localhost:9001,localhost:9003
//	go run main.go -id 3 -addr localhost:9003 -peers localhost:9001,localhost:9002
//
// Then use the interactive commands:
//
//	set key value
//	get key
//	del key
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jtomecek/go-paxos/paxos"
)

// Operation types for the key-value store
type OpType string

const (
	OpSet OpType = "SET"
	OpDel OpType = "DEL"
)

// Operation represents a replicated operation
type Operation struct {
	Type  OpType `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// KVStore is a simple in-memory key-value store
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates a new key-value store
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply applies an operation to the store
func (kv *KVStore) Apply(op Operation) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.Type {
	case OpSet:
		kv.data[op.Key] = op.Value
		fmt.Printf("[Applied] SET %s = %s\n", op.Key, op.Value)
	case OpDel:
		delete(kv.data, op.Key)
		fmt.Printf("[Applied] DEL %s\n", op.Key)
	}
}

// Get retrieves a value from the store
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, ok := kv.data[key]
	return value, ok
}

// SimpleLogger implements paxos.Logger
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
	// Parse command line flags
	nodeID := flag.Int("id", 1, "Node ID (1-65535)")
	addr := flag.String("addr", "localhost:9001", "Listen address")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses")
	logLevel := flag.Int("log", 2, "Log level (0=none, 1=error, 2=warn, 3=info, 4=debug)")
	flag.Parse()

	// Parse peers
	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}

	if len(peers) == 0 {
		fmt.Println("Warning: No peers specified. Running as single node.")
	}

	// Create logger
	logger := SimpleLogger{level: *logLevel}

	// Create Paxos node
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

	// Create KV store
	store := NewKVStore()

	// Start the node
	if err := node.Start(); err != nil {
		fmt.Printf("Failed to start node: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Node %d started on %s\n", *nodeID, *addr)
	fmt.Printf("Peers: %v\n", peers)
	fmt.Println("\nCommands:")
	fmt.Println("  set <key> <value> - Set a key-value pair")
	fmt.Println("  get <key>         - Get a value")
	fmt.Println("  del <key>         - Delete a key")
	fmt.Println("  status            - Show node status")
	fmt.Println("  quit              - Exit")
	fmt.Println()

	// Start applying committed values in background
	go func() {
		for cv := range node.Subscribe() {
			var op Operation
			if err := json.Unmarshal(cv.Value, &op); err != nil {
				logger.Error("Failed to unmarshal operation", "error", err)
				continue
			}
			store.Apply(op)
		}
	}()

	// Interactive command loop
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				break
			}
			key, value := parts[1], parts[2]

			op := Operation{Type: OpSet, Key: key, Value: value}
			data, _ := json.Marshal(op)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			instance, err := node.Propose(ctx, data)
			cancel()

			if err != nil {
				fmt.Printf("Failed to set: %v\n", err)
			} else {
				fmt.Printf("Set committed at instance %d\n", instance)
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				break
			}
			key := parts[1]

			if value, ok := store.Get(key); ok {
				fmt.Printf("%s = %s\n", key, value)
			} else {
				fmt.Printf("Key '%s' not found\n", key)
			}

		case "del":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
				break
			}
			key := parts[1]

			op := Operation{Type: OpDel, Key: key}
			data, _ := json.Marshal(op)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			instance, err := node.Propose(ctx, data)
			cancel()

			if err != nil {
				fmt.Printf("Failed to delete: %v\n", err)
			} else {
				fmt.Printf("Delete committed at instance %d\n", instance)
			}

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
