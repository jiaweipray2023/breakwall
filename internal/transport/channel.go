// Package transport manages the persistent unidirectional TCP channels
// between the inner and outer proxies, with automatic reconnection.
package transport

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/jiaweipray2023/breakwall/internal/proto"
)

const (
	reconnectBaseDelay = 2 * time.Second
	reconnectMaxDelay  = 30 * time.Second
	dialTimeout        = 10 * time.Second
)

// Sender manages a persistent outbound channel that sends frames.
// It reconnects automatically on failure.
type Sender struct {
	mode    string // "connect" or "listen"
	address string

	mu     sync.Mutex
	writer *proto.FrameWriter
	conn   net.Conn

	frameCh chan *proto.Frame
	done    chan struct{}
}

// NewSender creates a new sender for the given mode and address.
func NewSender(mode, address string) *Sender {
	return &Sender{
		mode:    mode,
		address: address,
		frameCh: make(chan *proto.Frame, 1024),
		done:    make(chan struct{}),
	}
}

// Start begins the sender loop in a goroutine.
func (s *Sender) Start() {
	go s.loop()
}

// Send enqueues a frame for sending. Non-blocking if buffer has space.
func (s *Sender) Send(f *proto.Frame) {
	select {
	case s.frameCh <- f:
	case <-s.done:
	}
}

// Stop stops the sender.
func (s *Sender) Stop() {
	close(s.done)
	s.mu.Lock()
	if s.conn != nil {
		s.conn.Close()
	}
	s.mu.Unlock()
}

func (s *Sender) loop() {
	for {
		conn, err := s.establish()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				continue
			}
		}

		s.mu.Lock()
		s.conn = conn
		s.writer = proto.NewFrameWriter(conn)
		s.mu.Unlock()

		log.Printf("[sender] channel established: %s (%s)", s.address, s.mode)

		// Drain the frame channel and write to the connection.
		err = s.writeLoop()
		if err != nil {
			log.Printf("[sender] write error: %v, reconnecting...", err)
		}

		conn.Close()
		s.mu.Lock()
		s.conn = nil
		s.writer = nil
		s.mu.Unlock()
	}
}

func (s *Sender) writeLoop() error {
	for {
		select {
		case f := <-s.frameCh:
			s.mu.Lock()
			w := s.writer
			s.mu.Unlock()
			if w == nil {
				// Re-queue the frame.
				s.frameCh <- f
				return io.ErrClosedPipe
			}
			if err := w.WriteFrame(f); err != nil {
				// Re-queue on failure so it's not lost.
				select {
				case s.frameCh <- f:
				default:
				}
				return err
			}
		case <-s.done:
			return nil
		}
	}
}

func (s *Sender) establish() (net.Conn, error) {
	delay := reconnectBaseDelay
	for {
		select {
		case <-s.done:
			return nil, io.ErrClosedPipe
		default:
		}

		var conn net.Conn
		var err error

		if s.mode == "connect" {
			conn, err = net.DialTimeout("tcp", s.address, dialTimeout)
		} else {
			conn, err = s.listenAndAccept()
		}

		if err == nil {
			return conn, nil
		}

		log.Printf("[sender] establish failed (%s %s): %v, retrying in %v",
			s.mode, s.address, err, delay)

		select {
		case <-time.After(delay):
		case <-s.done:
			return nil, io.ErrClosedPipe
		}

		delay = delay * 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

func (s *Sender) listenAndAccept() (net.Conn, error) {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	return ln.Accept()
}

// Receiver manages a persistent inbound channel that reads frames.
// It dispatches received frames via a callback.
type Receiver struct {
	mode    string
	address string

	handler func(*proto.Frame)
	done    chan struct{}

	mu   sync.Mutex
	conn net.Conn
	ln   net.Listener
}

// NewReceiver creates a new receiver.
func NewReceiver(mode, address string, handler func(*proto.Frame)) *Receiver {
	return &Receiver{
		mode:    mode,
		address: address,
		handler: handler,
		done:    make(chan struct{}),
	}
}

// Start begins the receiver loop in a goroutine.
func (r *Receiver) Start() {
	go r.loop()
}

// Stop stops the receiver.
func (r *Receiver) Stop() {
	close(r.done)
	r.mu.Lock()
	if r.conn != nil {
		r.conn.Close()
	}
	if r.ln != nil {
		r.ln.Close()
	}
	r.mu.Unlock()
}

// SendFrame sends a frame back through the receiver's connection (for pong responses).
func (r *Receiver) SendFrame(f *proto.Frame) error {
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil {
		return io.ErrClosedPipe
	}
	w := proto.NewFrameWriter(conn)
	return w.WriteFrame(f)
}

func (r *Receiver) loop() {
	for {
		conn, err := r.establish()
		if err != nil {
			select {
			case <-r.done:
				return
			default:
				continue
			}
		}

		r.mu.Lock()
		r.conn = conn
		r.mu.Unlock()

		log.Printf("[receiver] channel established: %s (%s)", r.address, r.mode)

		reader := proto.NewFrameReader(conn)
		for {
			f, err := reader.ReadFrame()
			if err != nil {
				log.Printf("[receiver] read error: %v, reconnecting...", err)
				break
			}
			r.handler(f)
		}

		conn.Close()
		r.mu.Lock()
		r.conn = nil
		r.mu.Unlock()
	}
}

func (r *Receiver) establish() (net.Conn, error) {
	delay := reconnectBaseDelay
	for {
		select {
		case <-r.done:
			return nil, io.ErrClosedPipe
		default:
		}

		var conn net.Conn
		var err error

		if r.mode == "connect" {
			conn, err = net.DialTimeout("tcp", r.address, dialTimeout)
		} else {
			conn, err = r.listenOnce()
		}

		if err == nil {
			return conn, nil
		}

		log.Printf("[receiver] establish failed (%s %s): %v, retrying in %v",
			r.mode, r.address, err, delay)

		select {
		case <-time.After(delay):
		case <-r.done:
			return nil, io.ErrClosedPipe
		}

		delay = delay * 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

func (r *Receiver) listenOnce() (net.Conn, error) {
	r.mu.Lock()
	if r.ln == nil {
		ln, err := net.Listen("tcp", r.address)
		if err != nil {
			r.mu.Unlock()
			return nil, err
		}
		r.ln = ln
	}
	ln := r.ln
	r.mu.Unlock()
	return ln.Accept()
}
