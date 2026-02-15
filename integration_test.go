package breakwall_test

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jiaweipray2023/breakwall/internal/config"
	"github.com/jiaweipray2023/breakwall/internal/proto"
	"github.com/jiaweipray2023/breakwall/internal/session"
	"github.com/jiaweipray2023/breakwall/internal/transport"
	"gopkg.in/yaml.v3"
)

// TestEndToEnd starts a mock target server, both proxies, and a client,
// then verifies that data flows correctly through the entire pipeline.
func TestEndToEnd(t *testing.T) {
	// 1. Start a mock target TCP server (simulates SSH/VNC server in DMZ).
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("target listen: %v", err)
	}
	defer targetLn.Close()
	targetAddr := targetLn.Addr().String()
	t.Logf("target server at %s", targetAddr)

	// The mock server echoes back whatever it receives with a prefix.
	go func() {
		for {
			conn, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					reply := append([]byte("echo:"), buf[:n]...)
					c.Write(reply)
				}
			}(conn)
		}
	}()

	// 2. Find free ports for channels and service.
	fwdPort := freePort(t)
	revPort := freePort(t)
	svcPort := freePort(t)

	serviceName := "test-svc"

	// 3. Start bw-outer components.
	outerMgr := session.NewManager()
	outerRevSender := transport.NewSender("connect", fmt.Sprintf("127.0.0.1:%d", revPort))
	outerRevSender.Start()

	var outerFwdReceiver *transport.Receiver
	outerFwdReceiver = transport.NewReceiver("listen", fmt.Sprintf("127.0.0.1:%d", fwdPort),
		func(f *proto.Frame) {
			switch f.Type {
			case proto.FrameOpen:
				sess := outerMgr.RegisterSession(f.SessID, string(f.Data))
				go handleTargetForTest(sess, targetAddr, outerMgr, outerRevSender)
			case proto.FrameData:
				outerMgr.Dispatch(f.SessID, f.Data)
			case proto.FrameClose:
				outerMgr.Remove(f.SessID)
			case proto.FramePing:
				outerFwdReceiver.SendFrame(proto.NewPongFrame())
			}
		},
	)
	outerFwdReceiver.Start()

	// 4. Start bw-inner components.
	innerMgr := session.NewManager()
	innerFwdSender := transport.NewSender("connect", fmt.Sprintf("127.0.0.1:%d", fwdPort))
	innerFwdSender.Start()

	var innerRevReceiver *transport.Receiver
	innerRevReceiver = transport.NewReceiver("listen", fmt.Sprintf("127.0.0.1:%d", revPort),
		func(f *proto.Frame) {
			switch f.Type {
			case proto.FrameData:
				innerMgr.Dispatch(f.SessID, f.Data)
			case proto.FrameClose:
				innerMgr.Remove(f.SessID)
			case proto.FramePing:
				innerRevReceiver.SendFrame(proto.NewPongFrame())
			}
		},
	)
	innerRevReceiver.Start()

	// Service listener.
	svcLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", svcPort))
	if err != nil {
		t.Fatalf("svc listen: %v", err)
	}
	defer svcLn.Close()

	go func() {
		for {
			conn, err := svcLn.Accept()
			if err != nil {
				return
			}
			go handleClientForTest(conn, serviceName, innerMgr, innerFwdSender)
		}
	}()

	// Give channels time to establish.
	time.Sleep(500 * time.Millisecond)

	// 5. Connect as a client and test roundtrip.
	clientConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", svcPort))
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer clientConn.Close()

	testMsg := "hello breakwall"
	_, err = clientConn.Write([]byte(testMsg))
	if err != nil {
		t.Fatalf("client write: %v", err)
	}

	// Read response.
	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}

	expected := "echo:" + testMsg
	got := string(buf[:n])
	if got != expected {
		t.Fatalf("response mismatch: got %q, want %q", got, expected)
	}

	t.Logf("roundtrip success: sent %q, received %q", testMsg, got)

	// Cleanup.
	clientConn.Close()
	time.Sleep(200 * time.Millisecond)

	innerFwdSender.Stop()
	innerRevReceiver.Stop()
	outerRevSender.Stop()
	outerFwdReceiver.Stop()
	innerMgr.CloseAll()
	outerMgr.CloseAll()
}

// TestConfigParsing verifies that YAML config files can be loaded correctly.
func TestConfigParsing(t *testing.T) {
	innerYAML := `
services:
  - name: "ssh"
    listen: ":2222"
  - name: "vnc"
    listen: ":5900"
forward:
  mode: "connect"
  address: "10.0.0.1:9001"
reverse:
  mode: "listen"
  address: ":9002"
`
	var inner config.InnerConfig
	if err := yaml.Unmarshal([]byte(innerYAML), &inner); err != nil {
		t.Fatalf("unmarshal inner config: %v", err)
	}
	if len(inner.Services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(inner.Services))
	}
	if inner.Forward.Mode != "connect" {
		t.Fatalf("expected forward mode 'connect', got %q", inner.Forward.Mode)
	}
	if inner.Reverse.Mode != "listen" {
		t.Fatalf("expected reverse mode 'listen', got %q", inner.Reverse.Mode)
	}

	outerYAML := `
services:
  - name: "ssh"
    target: "192.168.1.10:22"
  - name: "vnc"
    target: "192.168.1.10:5900"
forward:
  mode: "listen"
  address: ":9001"
reverse:
  mode: "connect"
  address: "10.0.0.2:9002"
`
	var outer config.OuterConfig
	if err := yaml.Unmarshal([]byte(outerYAML), &outer); err != nil {
		t.Fatalf("unmarshal outer config: %v", err)
	}
	if len(outer.Services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(outer.Services))
	}
	if outer.Services[0].Target != "192.168.1.10:22" {
		t.Fatalf("expected target '192.168.1.10:22', got %q", outer.Services[0].Target)
	}
}

// TestFrameProtocol tests the frame encoding/decoding.
func TestFrameProtocol(t *testing.T) {
	pr, pw := io.Pipe()
	writer := proto.NewFrameWriter(pw)
	reader := proto.NewFrameReader(pr)

	frames := []*proto.Frame{
		proto.NewOpenFrame(1, "ssh-server"),
		proto.NewDataFrame(1, []byte("hello world")),
		proto.NewCloseFrame(1),
		proto.NewPingFrame(),
		proto.NewPongFrame(),
	}

	go func() {
		for _, f := range frames {
			if err := writer.WriteFrame(f); err != nil {
				t.Errorf("write frame: %v", err)
				return
			}
		}
		pw.Close()
	}()

	for i, expected := range frames {
		got, err := reader.ReadFrame()
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if got.Type != expected.Type {
			t.Errorf("frame %d type: got %d, want %d", i, got.Type, expected.Type)
		}
		if got.SessID != expected.SessID {
			t.Errorf("frame %d sessID: got %d, want %d", i, got.SessID, expected.SessID)
		}
		if string(got.Data) != string(expected.Data) {
			t.Errorf("frame %d data: got %q, want %q", i, got.Data, expected.Data)
		}
	}
}

// Helper functions that mirror the main proxy logic for testing.

func handleClientForTest(conn net.Conn, serviceName string, mgr *session.Manager, fwdSender *transport.Sender) {
	defer conn.Close()
	sess := mgr.NewSession(serviceName)
	defer mgr.Remove(sess.ID)

	fwdSender.Send(proto.NewOpenFrame(sess.ID, serviceName))

	go func() {
		for {
			select {
			case data := <-sess.DataCh:
				conn.Write(data)
			case <-sess.Done:
				return
			}
		}
	}()

	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			fwdSender.Send(proto.NewDataFrame(sess.ID, data))
		}
		if err != nil {
			fwdSender.Send(proto.NewCloseFrame(sess.ID))
			return
		}
	}
}

func handleTargetForTest(sess *session.Session, target string, mgr *session.Manager, revSender *transport.Sender) {
	defer mgr.Remove(sess.ID)
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		revSender.Send(proto.NewCloseFrame(sess.ID))
		return
	}
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case data := <-sess.DataCh:
				conn.Write(data)
			case <-sess.Done:
				return
			}
		}
	}()

	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			revSender.Send(proto.NewDataFrame(sess.ID, data))
		}
		if err != nil {
			revSender.Send(proto.NewCloseFrame(sess.ID))
			break
		}
	}
	wg.Wait()
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// Ensure os import is used (for possible future use).
var _ = os.Stdout
