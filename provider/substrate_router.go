//go:build computeprovider_substrate

package computeprovider

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"

	"temporal-substrate-demo/workerprotocol"
)

// targetActorHeader selects the actor at atenet-router
// (go.ate.dev/substrate/internal/atenet.TargetActorHeader, which is internal).
const targetActorHeader = "ate-target-actor"

// routerWorkers is the workerLifecycle adapter: workerprotocol calls over a
// tunnel through atenet-router's CONNECT-TLS listener, the same path kubectl
// ate proxy uses (internal/ateclient/router.go). The router resumes the actor
// if needed and requires a client certificate whose SPIFFE ID is listed in
// its --ingress-allowed-spiffe-ids.
type routerWorkers struct {
	addr string      // atenet-router.ate-system.svc:8444
	tls  *tls.Config // servicedns server verification + podidentity client cert
}

func (r *routerWorkers) Start(ctx context.Context, actor *ateapi.ObjectRef, req workerprotocol.StartRequest) error {
	return r.client(actor).Start(ctx, req)
}

func (r *routerWorkers) Drain(ctx context.Context, actor *ateapi.ObjectRef) error {
	return r.client(actor).Drain(ctx)
}

func (r *routerWorkers) client(actor *ateapi.ObjectRef) workerprotocol.Client {
	return workerprotocol.Client{
		Host: actor.GetName(),
		Dial: func(ctx context.Context) (net.Conn, error) { return r.tunnel(ctx, actor) },
	}
}

// tunnel opens a connection to workerprotocol.Port on the actor.
func (r *routerWorkers) tunnel(ctx context.Context, actor *ateapi.ObjectRef) (net.Conn, error) {
	d := tls.Dialer{Config: r.tls}
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, fmt.Errorf("dial atenet-router %s: %w", r.addr, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	dest := net.JoinHostPort(actor.GetName(), strconv.Itoa(workerprotocol.Port))
	connect := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: dest},
		Host:   dest,
		Header: http.Header{targetActorHeader: {actor.GetAtespace() + "/" + actor.GetName()}},
	}
	if err := connect.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, connect)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CONNECT response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		conn.Close()
		return nil, fmt.Errorf("router refused CONNECT to %s/%s: %s %s", actor.GetAtespace(), actor.GetName(), resp.Status, strings.TrimSpace(string(msg)))
	}
	// Keep any bytes the reader already buffered past the CONNECT response.
	return &bufferedConn{Conn: conn, r: br}, nil
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }
