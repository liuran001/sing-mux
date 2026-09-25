package mux

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// replyAfterEOF reads a stream to EOF, i.e. until the client half-closes, and
// only then answers with how many bytes it got -- the shape of an HTTP/1.0
// upload or of any request/response protocol whose request ends with FIN.
type replyAfterEOF struct{}

func (replyAfterEOF) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	defer conn.Close()
	n, err := io.Copy(io.Discard, conn)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(conn, "received %d bytes", n)
	return err
}

func (replyAfterEOF) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return conn.Close()
}

// loopbackDialer hands the client one end of a loopback TCP connection whose
// other end is served by service.
type loopbackDialer struct {
	t       *testing.T
	service *Service
}

func (d loopbackDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_ = d.service.NewConnection(context.Background(), conn, M.Metadata{})
		conn.Close()
	}()
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", listener.Addr().String())
}

func (d loopbackDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

// A client that half-closes its upload must still receive the reply the peer
// sends after seeing that EOF. mihomo's relay does exactly this through
// N.CloseWrite whenever the local side finishes sending first.
func TestHalfClosedStreamStillReceivesTheReply(t *testing.T) {
	for _, protocol := range []string{"h2mux", "yamux"} {
		for _, size := range []int{16, 1 << 20} {
			t.Run(fmt.Sprintf("%s/%d", protocol, size), func(t *testing.T) {
				service, err := NewService(ServiceOptions{
					NewStreamContext: func(ctx context.Context, _ net.Conn) context.Context { return ctx },
					Logger:           logger.NOP(),
					Handler:          replyAfterEOF{},
				})
				if err != nil {
					t.Fatal(err)
				}
				client, err := NewClient(Options{
					Dialer:         loopbackDialer{t: t, service: service},
					Logger:         logger.NOP(),
					Protocol:       protocol,
					MaxConnections: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				conn, err := client.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.com:80"))
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

				if _, err = conn.Write(bytes.Repeat([]byte{'x'}, size)); err != nil {
					t.Fatal(err)
				}
				if err = N.CloseWrite(conn); err != nil {
					t.Fatal(err)
				}
				reply, err := io.ReadAll(conn)
				if err != nil {
					t.Fatalf("reading the reply after CloseWrite: %v", err)
				}
				if want := fmt.Sprintf("received %d bytes", size); string(reply) != want {
					t.Fatalf("got reply %q, want %q", reply, want)
				}
			})
		}
	}
}
