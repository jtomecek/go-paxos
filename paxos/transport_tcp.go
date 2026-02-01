package paxos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

// TCPTransport implements the Transport interface using TCP connections.
type TCPTransport struct {
	logger Logger

	mu       sync.RWMutex
	listener net.Listener
	conns    map[string]*tcpConn // addr -> connection
	incoming chan MessageEnvelope
	closed   bool
}

type tcpConn struct {
	addr string
	conn net.Conn
	mu   sync.Mutex
}

// NewTCPTransport creates a new TCP transport.
func NewTCPTransport(logger Logger) *TCPTransport {
	if logger == nil {
		logger = NoopLogger{}
	}
	return &TCPTransport{
		logger:   logger,
		conns:    make(map[string]*tcpConn),
		incoming: make(chan MessageEnvelope, 1000),
	}
}

// Listen starts accepting connections on the specified address.
func (t *TCPTransport) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.listener = listener
	t.mu.Unlock()

	go t.acceptLoop()

	t.logger.Info("TCPTransport: listening", "addr", addr)
	return nil
}

// acceptLoop accepts incoming connections.
func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			t.mu.RLock()
			closed := t.closed
			t.mu.RUnlock()

			if closed {
				return
			}

			t.logger.Error("TCPTransport: accept error", "error", err)
			continue
		}

		go t.handleConnection(conn)
	}
}

// handleConnection reads messages from a connection.
func (t *TCPTransport) handleConnection(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	t.logger.Debug("TCPTransport: new connection", "from", addr)

	// Store connection if we don't have one for this address
	t.mu.Lock()
	if _, exists := t.conns[addr]; !exists {
		t.conns[addr] = &tcpConn{addr: addr, conn: conn}
	}
	t.mu.Unlock()

	reader := bufio.NewReader(conn)

	for {
		msg, err := t.readMessage(reader)
		if err != nil {
			if err != io.EOF {
				t.logger.Error("TCPTransport: read error", "error", err, "from", addr)
			}
			break
		}

		envelope := MessageEnvelope{
			From:    addr,
			Message: msg,
		}

		select {
		case t.incoming <- envelope:
		default:
			t.logger.Warn("TCPTransport: incoming buffer full, dropping message")
		}
	}

	t.mu.Lock()
	delete(t.conns, addr)
	t.mu.Unlock()
	conn.Close()
}

// Connect establishes a connection to a peer.
func (t *TCPTransport) Connect(addr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.conns[addr]; exists {
		return nil // Already connected
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}

	tc := &tcpConn{addr: addr, conn: conn}
	t.conns[addr] = tc

	// Start reading from this connection
	go t.handleConnection(conn)

	t.logger.Debug("TCPTransport: connected to peer", "addr", addr)
	return nil
}

// Send transmits a message to a specific peer.
func (t *TCPTransport) Send(ctx context.Context, to string, msg Message) error {
	t.mu.RLock()
	tc, exists := t.conns[to]
	t.mu.RUnlock()

	if !exists {
		// Try to connect
		if err := t.Connect(to); err != nil {
			return err
		}
		t.mu.RLock()
		tc = t.conns[to]
		t.mu.RUnlock()
	}

	return t.writeMessage(tc, msg)
}

// Broadcast sends a message to all connected peers.
func (t *TCPTransport) Broadcast(ctx context.Context, msg Message) error {
	t.mu.RLock()
	conns := make([]*tcpConn, 0, len(t.conns))
	for _, tc := range t.conns {
		conns = append(conns, tc)
	}
	t.mu.RUnlock()

	var wg sync.WaitGroup
	for _, tc := range conns {
		wg.Add(1)
		go func(tc *tcpConn) {
			defer wg.Done()
			if err := t.writeMessage(tc, msg); err != nil {
				t.logger.Debug("TCPTransport: broadcast error", "to", tc.addr, "error", err)
			}
		}(tc)
	}
	wg.Wait()

	return nil
}

// Receive returns a channel of incoming messages.
func (t *TCPTransport) Receive() <-chan MessageEnvelope {
	return t.incoming
}

// Close shuts down the transport.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true

	if t.listener != nil {
		t.listener.Close()
	}

	for _, tc := range t.conns {
		tc.conn.Close()
	}

	close(t.incoming)

	return nil
}

// writeMessage sends a message over a connection.
// Message format: [4 bytes length][message bytes]
func (t *TCPTransport) writeMessage(tc *tcpConn, msg Message) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	var buf bytes.Buffer
	if err := msg.Encode(&buf); err != nil {
		return err
	}

	data := buf.Bytes()
	length := uint32(len(data))

	// Write length prefix
	if err := binary.Write(tc.conn, binary.BigEndian, length); err != nil {
		return err
	}

	// Write message data
	_, err := tc.conn.Write(data)
	return err
}

// readMessage reads a message from a connection.
func (t *TCPTransport) readMessage(reader *bufio.Reader) (Message, error) {
	// Read length prefix
	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	// Decode message
	return decodeMessage(bytes.NewReader(data))
}

// Verify interface compliance
var _ Transport = (*TCPTransport)(nil)
