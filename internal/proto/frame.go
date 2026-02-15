// Package proto implements the binary framing protocol for multiplexing
// sessions over unidirectional channels.
//
// Frame format:
//
//	+----------+----------+----------+----------+
//	| Type (1) | SessID(4)| Length(4)| Data (N) |
//	+----------+----------+----------+----------+
package proto

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// Frame types.
const (
	FrameOpen  uint8 = 0x01 // Open a new session (data = service name)
	FrameData  uint8 = 0x02 // Session data payload
	FrameClose uint8 = 0x03 // Close a session
	FramePing  uint8 = 0x04 // Heartbeat request
	FramePong  uint8 = 0x05 // Heartbeat response
)

const (
	HeaderSize = 9          // 1 (type) + 4 (session ID) + 4 (data length)
	MaxDataLen = 64 * 1024  // 64KB max per frame
)

// Frame represents a single protocol frame.
type Frame struct {
	Type   uint8
	SessID uint32
	Data   []byte
}

// NewOpenFrame creates a frame to open a new session for the named service.
func NewOpenFrame(sessID uint32, serviceName string) *Frame {
	return &Frame{Type: FrameOpen, SessID: sessID, Data: []byte(serviceName)}
}

// NewDataFrame creates a data frame for a session.
func NewDataFrame(sessID uint32, data []byte) *Frame {
	return &Frame{Type: FrameData, SessID: sessID, Data: data}
}

// NewCloseFrame creates a frame to close a session.
func NewCloseFrame(sessID uint32) *Frame {
	return &Frame{Type: FrameClose, SessID: sessID}
}

// NewPingFrame creates a heartbeat request frame.
func NewPingFrame() *Frame {
	return &Frame{Type: FramePing}
}

// NewPongFrame creates a heartbeat response frame.
func NewPongFrame() *Frame {
	return &Frame{Type: FramePong}
}

// FrameWriter writes frames to an io.Writer with thread safety.
type FrameWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewFrameWriter creates a new thread-safe frame writer.
func NewFrameWriter(w io.Writer) *FrameWriter {
	return &FrameWriter{w: w}
}

// WriteFrame writes a single frame to the underlying writer.
func (fw *FrameWriter) WriteFrame(f *Frame) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	header := make([]byte, HeaderSize)
	header[0] = f.Type
	binary.BigEndian.PutUint32(header[1:5], f.SessID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(f.Data)))

	if _, err := fw.w.Write(header); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}
	if len(f.Data) > 0 {
		if _, err := fw.w.Write(f.Data); err != nil {
			return fmt.Errorf("write frame data: %w", err)
		}
	}
	return nil
}

// FrameReader reads frames from an io.Reader.
type FrameReader struct {
	r io.Reader
}

// NewFrameReader creates a new frame reader.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{r: r}
}

// ReadFrame reads a single frame from the underlying reader.
func (fr *FrameReader) ReadFrame() (*Frame, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(fr.r, header); err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}

	f := &Frame{
		Type:   header[0],
		SessID: binary.BigEndian.Uint32(header[1:5]),
	}

	dataLen := binary.BigEndian.Uint32(header[5:9])
	if dataLen > MaxDataLen {
		return nil, fmt.Errorf("frame data length %d exceeds max %d", dataLen, MaxDataLen)
	}

	if dataLen > 0 {
		f.Data = make([]byte, dataLen)
		if _, err := io.ReadFull(fr.r, f.Data); err != nil {
			return nil, fmt.Errorf("read frame data: %w", err)
		}
	}

	return f, nil
}
