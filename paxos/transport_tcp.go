package paxos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// handshakeTimeout bounds how long we wait for a peer to identify itself
// before giving up on a freshly established connection.
const handshakeTimeout = 5 * time.Second

// reconnectInterval is how often we try to re-establish connections to
// configured peers that are not currently connected.
const reconnectInterval = 1 * time.Second

// TCPTransport implements the Transport interface using TCP connections.
//
// Connections are identified by the peer's NodeID, which is exchanged in a
// short handshake when a connection is established. This guarantees exactly
// one logical connection per peer: when both nodes dial each other (or a
// connection is re-established), the duplicate is resolved deterministically
// by keeping the one initiated by the lower NodeID. Keying by NodeID rather
// than socket address is what prevents a peer's messages from being delivered
// twice (which would, e.g., let a single Promise be counted twice toward a
// quorum) and prevents two writer goroutines from interleaving frames on the
// same socket.
type TCPTransport struct {
	logger Logger
	nodeID NodeID

	mu         sync.RWMutex
	listenAddr string
	listener   net.Listener
	conns      map[NodeID]*tcpConn // peer NodeID -> active connection
	peerAddrs  map[string]struct{} // configured peer addresses to (re)dial
	addrToNode map[string]NodeID   // learned mapping: peer listen addr -> NodeID
	incoming   chan MessageEnvelope
	closed     bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type tcpConn struct {
	peerID NodeID
	conn   net.Conn
	// initiatedByMe is true if we dialed this connection (vs. accepted it).
	// It is used to deterministically resolve duplicate connections.
	initiatedByMe bool
	mu            sync.Mutex // serializes writes to conn
}

// NewTCPTransport creates a new TCP transport for the given node.
func NewTCPTransport(nodeID NodeID, logger Logger) *TCPTransport {
	if logger == nil {
		logger = NoopLogger{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPTransport{
		logger:     logger,
		nodeID:     nodeID,
		conns:      make(map[NodeID]*tcpConn),
		peerAddrs:  make(map[string]struct{}),
		addrToNode: make(map[string]NodeID),
		incoming:   make(chan MessageEnvelope, 1000),
		ctx:        ctx,
		cancel:     cancel,
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
	t.listenAddr = addr
	t.mu.Unlock()

	t.wg.Add(2)
	go t.acceptLoop()
	go t.reconnectLoop()

	t.logger.Info("TCPTransport: listening", "addr", addr)
	return nil
}

// acceptLoop accepts incoming connections.
func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
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

		// Inbound connection: we accepted it, so initiatedByMe is false.
		go t.setupConn(conn, false)
	}
}

// Connect establishes a connection to a peer. The address is remembered so the
// reconnect loop can restore the connection if it later drops.
func (t *TCPTransport) Connect(addr string) error {
	t.mu.Lock()
	t.peerAddrs[addr] = struct{}{}
	if t.connectedTo(addr) {
		t.mu.Unlock()
		return nil
	}
	t.mu.Unlock()

	return t.dial(addr)
}

// connectedTo reports whether we have a live connection to the peer at addr.
// Must be called with t.mu held.
func (t *TCPTransport) connectedTo(addr string) bool {
	id, ok := t.addrToNode[addr]
	if !ok {
		return false
	}
	_, ok = t.conns[id]
	return ok
}

// dial opens an outbound connection to addr and runs the handshake.
func (t *TCPTransport) dial(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	// Outbound connection: we dialed it, so initiatedByMe is true.
	t.setupConn(conn, true)
	return nil
}

// setupConn performs the identity handshake and, if this connection wins the
// duplicate-resolution tie-break, registers it and starts reading from it.
func (t *TCPTransport) setupConn(conn net.Conn, initiatedByMe bool) {
	// A single reader is used for the handshake and all subsequent messages:
	// the peer may send message bytes immediately after its handshake, and
	// bufio may pull them into the buffer, so we must not discard this reader.
	reader := bufio.NewReader(conn)

	peerID, peerAddr, err := t.handshake(conn, reader)
	if err != nil {
		t.logger.Debug("TCPTransport: handshake failed", "error", err)
		conn.Close()
		return
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		conn.Close()
		return
	}

	// Learn the peer's listen address so the reconnect loop can recognize it.
	if peerAddr != "" {
		t.addrToNode[peerAddr] = peerID
	}

	tc := &tcpConn{peerID: peerID, conn: conn, initiatedByMe: initiatedByMe}
	if !t.registerLocked(tc) {
		t.mu.Unlock()
		conn.Close()
		return
	}
	// Add to the wait group while still holding the lock and before Close can
	// observe a new connection, so Close's wg.Wait never races a late Add.
	t.wg.Add(1)
	t.mu.Unlock()

	t.logger.Debug("TCPTransport: connection established",
		"peer", peerID, "initiatedByMe", initiatedByMe)

	go t.readLoop(tc, reader)
}

// registerLocked installs tc as the connection for its peer, resolving any
// duplicate deterministically. It returns false if tc is the losing duplicate
// and should be discarded. Must be called with t.mu held.
//
// Both endpoints independently keep the connection initiated by the node with
// the lower ID, so they converge on the same physical connection without
// coordination.
func (t *TCPTransport) registerLocked(tc *tcpConn) bool {
	existing, ok := t.conns[tc.peerID]
	if ok {
		// A connection is "won" by the side initiated by the lower NodeID.
		winnerInitiatedByMe := t.nodeID < tc.peerID
		if tc.initiatedByMe != winnerInitiatedByMe {
			return false // tc is the losing duplicate
		}
		existing.conn.Close() // tc wins; drop the old one
	}
	t.conns[tc.peerID] = tc
	return true
}

// handshake exchanges identity with the peer. Each side writes its own NodeID
// and listen address, then reads the peer's. Format:
//
//	[nodeID uint16][addrLen uint16][addr bytes]
func (t *TCPTransport) handshake(conn net.Conn, reader *bufio.Reader) (NodeID, string, error) {
	t.mu.RLock()
	myID := t.nodeID
	myAddr := t.listenAddr
	t.mu.RUnlock()

	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return 0, "", err
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, myID); err != nil {
		return 0, "", err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(myAddr))); err != nil {
		return 0, "", err
	}
	buf.WriteString(myAddr)
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return 0, "", err
	}

	var peerID uint16
	if err := binary.Read(reader, binary.BigEndian, &peerID); err != nil {
		return 0, "", err
	}
	var addrLen uint16
	if err := binary.Read(reader, binary.BigEndian, &addrLen); err != nil {
		return 0, "", err
	}
	addrBytes := make([]byte, addrLen)
	if _, err := io.ReadFull(reader, addrBytes); err != nil {
		return 0, "", err
	}

	if NodeID(peerID) == myID {
		return 0, "", fmt.Errorf("peer reports our own NodeID %d", myID)
	}

	// Clear the deadline now that the handshake is complete; subsequent reads
	// continue from the same reader (which may already hold buffered message
	// bytes the peer sent right after its handshake).
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return 0, "", err
	}

	return NodeID(peerID), string(addrBytes), nil
}

// readLoop reads framed messages from a connection until it errors or closes.
// reader is the same buffered reader used for the handshake, so no bytes the
// peer sent immediately afterward are lost.
func (t *TCPTransport) readLoop(tc *tcpConn, reader *bufio.Reader) {
	defer t.wg.Done()

	for {
		msg, err := t.readMessage(reader)
		if err != nil {
			if err != io.EOF {
				t.logger.Debug("TCPTransport: read error", "error", err, "peer", tc.peerID)
			}
			break
		}

		envelope := MessageEnvelope{From: tc.peerID, Message: msg}
		select {
		case t.incoming <- envelope:
		default:
			t.logger.Warn("TCPTransport: incoming buffer full, dropping message")
		}
	}

	// Remove ourselves only if we're still the registered connection; a newer
	// connection may have replaced us via duplicate resolution.
	t.mu.Lock()
	if cur, ok := t.conns[tc.peerID]; ok && cur == tc {
		delete(t.conns, tc.peerID)
	}
	t.mu.Unlock()
	tc.conn.Close()
}

// Send transmits a message to a specific peer by NodeID. If we have no live
// connection to that peer, the message is dropped (the reconnect loop will
// restore the connection); Paxos tolerates message loss via its own retries.
func (t *TCPTransport) Send(ctx context.Context, to NodeID, msg Message) error {
	t.mu.RLock()
	tc, ok := t.conns[to]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no connection to node %d", to)
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
				t.logger.Debug("TCPTransport: broadcast error", "to", tc.peerID, "error", err)
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
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()

	if t.listener != nil {
		t.listener.Close()
	}
	for _, tc := range t.conns {
		tc.conn.Close()
	}
	t.conns = make(map[NodeID]*tcpConn)
	t.mu.Unlock()

	// Wait for accept/reconnect/read goroutines to finish before closing the
	// incoming channel, so no read loop sends on a closed channel.
	t.wg.Wait()
	close(t.incoming)

	return nil
}

// reconnectLoop periodically re-dials configured peers we are not connected to.
func (t *TCPTransport) reconnectLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.mu.RLock()
			var missing []string
			for addr := range t.peerAddrs {
				if !t.connectedTo(addr) {
					missing = append(missing, addr)
				}
			}
			t.mu.RUnlock()

			for _, addr := range missing {
				if err := t.dial(addr); err != nil {
					t.logger.Debug("TCPTransport: reconnect failed", "addr", addr, "error", err)
				}
			}
		}
	}
}

// writeMessage sends a message over a connection.
// Message format: [4 bytes length][message bytes]
func (t *TCPTransport) writeMessage(tc *tcpConn, msg Message) error {
	var buf bytes.Buffer
	if err := msg.Encode(&buf); err != nil {
		return err
	}
	data := buf.Bytes()

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := binary.Write(tc.conn, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := tc.conn.Write(data)
	return err
}

// readMessage reads a single length-prefixed message from a connection.
func (t *TCPTransport) readMessage(reader *bufio.Reader) (Message, error) {
	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	return decodeMessage(bytes.NewReader(data))
}

// Verify interface compliance
var _ Transport = (*TCPTransport)(nil)
