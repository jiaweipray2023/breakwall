// bw-outer is the external DMZ proxy.
//
// It receives session data from the inner proxy through the forward isolation
// channel, connects to actual SSH/VNC servers in the DMZ, and sends responses
// back through the reverse isolation channel.
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jiaweipray2023/breakwall/internal/config"
	"github.com/jiaweipray2023/breakwall/internal/proto"
	"github.com/jiaweipray2023/breakwall/internal/session"
	"github.com/jiaweipray2023/breakwall/internal/transport"
)

const targetDialTimeout = 10 * time.Second

func main() {
	cfgPath := flag.String("config", "configs/outer.yaml", "path to outer proxy config file")
	flag.Parse()

	log.SetPrefix("[bw-outer] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := config.LoadOuterConfig(*cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Build service name → target address mapping.
	targets := make(map[string]string)
	for _, svc := range cfg.Services {
		targets[svc.Name] = svc.Target
		log.Printf("service %q → %s", svc.Name, svc.Target)
	}

	mgr := session.NewManager()

	// Reverse channel: sends server responses to the inner proxy (outer → inner).
	revSender := transport.NewSender(cfg.Reverse.Mode, cfg.Reverse.Address)
	revSender.Start()
	log.Printf("reverse channel: %s %s", cfg.Reverse.Mode, cfg.Reverse.Address)

	// Forward channel: receives client data from the inner proxy (inner → outer).
	var fwdReceiver *transport.Receiver
	fwdReceiver = transport.NewReceiver(cfg.Forward.Mode, cfg.Forward.Address,
		func(f *proto.Frame) {
			switch f.Type {
			case proto.FrameOpen:
				serviceName := string(f.Data)
				target, ok := targets[serviceName]
				if !ok {
					log.Printf("unknown service %q for session %d", serviceName, f.SessID)
					revSender.Send(proto.NewCloseFrame(f.SessID))
					return
				}
				sess := mgr.RegisterSession(f.SessID, serviceName)
				go handleTarget(sess, target, mgr, revSender)

			case proto.FrameData:
				if !mgr.Dispatch(f.SessID, f.Data) {
					log.Printf("dropped data for unknown session %d", f.SessID)
				}

			case proto.FrameClose:
				mgr.Remove(f.SessID)

			case proto.FramePing:
				fwdReceiver.SendFrame(proto.NewPongFrame())

			default:
				log.Printf("unexpected frame type %d on forward channel", f.Type)
			}
		},
	)
	fwdReceiver.Start()
	log.Printf("forward channel: %s %s", cfg.Forward.Mode, cfg.Forward.Address)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	mgr.CloseAll()
	revSender.Stop()
	fwdReceiver.Stop()
	log.Println("shutdown complete")
}

func handleTarget(sess *session.Session, target string, mgr *session.Manager, revSender *transport.Sender) {
	defer mgr.Remove(sess.ID)

	// Connect to the actual SSH/VNC server in the DMZ.
	conn, err := net.DialTimeout("tcp", target, targetDialTimeout)
	if err != nil {
		log.Printf("session %d: failed to connect to target %s: %v", sess.ID, target, err)
		revSender.Send(proto.NewCloseFrame(sess.ID))
		return
	}
	defer conn.Close()

	log.Printf("session %d: connected to target %s", sess.ID, target)

	// Goroutine: read data from the session channel (from inner proxy) and write to target.
	go func() {
		for {
			select {
			case data := <-sess.DataCh:
				if _, err := conn.Write(data); err != nil {
					log.Printf("session %d: write to target error: %v", sess.ID, err)
					sess.Close()
					return
				}
			case <-sess.Done:
				return
			}
		}
	}()

	// Main: read data from the target server and send back through the reverse channel.
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
			revSender.Send(proto.NewDataFrame(sess.ID, data))
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("session %d: read from target error: %v", sess.ID, err)
			}
			// Tell the inner proxy this session is done.
			revSender.Send(proto.NewCloseFrame(sess.ID))
			return
		}
	}
}
