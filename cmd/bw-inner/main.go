// bw-inner is the internal network proxy.
//
// It accepts SSH/VNC client connections on the internal network, multiplexes
// them through the forward isolation channel, and delivers responses from
// the reverse isolation channel back to the clients.
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jiaweipray2023/breakwall/internal/config"
	"github.com/jiaweipray2023/breakwall/internal/proto"
	"github.com/jiaweipray2023/breakwall/internal/session"
	"github.com/jiaweipray2023/breakwall/internal/transport"
)

func main() {
	cfgPath := flag.String("config", "configs/inner.yaml", "path to inner proxy config file")
	flag.Parse()

	log.SetPrefix("[bw-inner] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := config.LoadInnerConfig(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	mgr := session.NewManager()

	// Forward channel: sends client data to the outer proxy (inner → outer).
	fwdSender := transport.NewSender(cfg.Forward.Mode, cfg.Forward.Address)
	fwdSender.Start()
	log.Printf("forward channel: %s %s", cfg.Forward.Mode, cfg.Forward.Address)

	// Reverse channel: receives server responses from the outer proxy (outer → inner).
	var revReceiver *transport.Receiver
	revReceiver = transport.NewReceiver(cfg.Reverse.Mode, cfg.Reverse.Address,
		func(f *proto.Frame) {
			switch f.Type {
			case proto.FrameData:
				if !mgr.Dispatch(f.SessID, f.Data) {
					log.Printf("dropped data for unknown session %d", f.SessID)
				}
			case proto.FrameClose:
				mgr.Remove(f.SessID)
			case proto.FramePing:
				revReceiver.SendFrame(proto.NewPongFrame())
			default:
				log.Printf("unexpected frame type %d on reverse channel", f.Type)
			}
		},
	)
	revReceiver.Start()
	log.Printf("reverse channel: %s %s", cfg.Reverse.Mode, cfg.Reverse.Address)

	// Start a listener for each configured service.
	for _, svc := range cfg.Services {
		svc := svc
		go startServiceListener(svc, mgr, fwdSender)
	}

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	mgr.CloseAll()
	fwdSender.Stop()
	revReceiver.Stop()
	log.Println("shutdown complete")
}

func startServiceListener(svc config.ServiceConfig, mgr *session.Manager, fwdSender *transport.Sender) {
	ln, err := net.Listen("tcp", svc.Listen)
	if err != nil {
		log.Fatalf("failed to listen for service %q on %s: %v", svc.Name, svc.Listen, err)
	}
	log.Printf("service %q listening on %s", svc.Name, svc.Listen)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error for service %q: %v", svc.Name, err)
			continue
		}
		go handleClient(conn, svc.Name, mgr, fwdSender)
	}
}

func handleClient(conn net.Conn, serviceName string, mgr *session.Manager, fwdSender *transport.Sender) {
	defer conn.Close()

	sess := mgr.NewSession(serviceName)
	defer mgr.Remove(sess.ID)

	log.Printf("client connected: %s → session %d (%s)",
		conn.RemoteAddr(), sess.ID, serviceName)

	// Tell the outer proxy to open a new session.
	fwdSender.Send(proto.NewOpenFrame(sess.ID, serviceName))

	// Goroutine: read data from the reverse channel and write to the client.
	go func() {
		for {
			select {
			case data := <-sess.DataCh:
				if _, err := conn.Write(data); err != nil {
					log.Printf("session %d: write to client error: %v", sess.ID, err)
					sess.Close()
					return
				}
			case <-sess.Done:
				return
			}
		}
	}()

	// Main: read data from the client and send through the forward channel.
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-sess.Done:
			return
		default:
		}

		n, err := conn.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			fwdSender.Send(proto.NewDataFrame(sess.ID, data))
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("session %d: read from client error: %v", sess.ID, err)
			}
			// Tell the outer proxy this session is done.
			fwdSender.Send(proto.NewCloseFrame(sess.ID))
			return
		}
	}
}
