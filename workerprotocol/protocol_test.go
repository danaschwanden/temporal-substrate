package workerprotocol

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// The contract tests run the real Client against the real Handler. Both
// sides of the seam import this package, so these tests cover what the
// provider sends and what the actor accepts.

type fakeWorker struct {
	mu      sync.Mutex
	ready   bool
	started *StartRequest
	drains  int
	err     error
}

func (f *fakeWorker) Ready() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.ready }

func (f *fakeWorker) Start(_ context.Context, req StartRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.started != nil && *f.started != req {
		return ErrConflict
	}
	f.started = &req
	return nil
}

func (f *fakeWorker) Drain(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drains++
	f.started = nil
	return f.err
}

func serve(t *testing.T, w Worker) Client {
	srv := httptest.NewServer(NewHandler(w))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	return Client{
		Host: "worker-0",
		Dial: func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
	}
}

var req = StartRequest{
	Identity:       "worker-0",
	Address:        "temporal-frontend.temporal.svc.cluster.local:7233",
	Namespace:      "default",
	DeploymentName: "hello",
	BuildID:        "v1",
}

func TestStartRoundTripsEveryField(t *testing.T) {
	w := &fakeWorker{ready: true}
	tls := req
	tls.TLS = true
	if err := serve(t, w).Start(context.Background(), tls); err != nil {
		t.Fatal(err)
	}
	if *w.started != tls {
		t.Fatalf("worker got %+v, sent %+v", *w.started, tls)
	}
}

func TestStartIsIdempotentAndConflictsOnChange(t *testing.T) {
	w := &fakeWorker{ready: true}
	c := serve(t, w)
	ctx := context.Background()
	if err := c.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx, req); err != nil {
		t.Fatalf("repeat /start must succeed: %v", err)
	}
	other := req
	other.BuildID = "v2"
	if err := c.Start(ctx, other); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestStartFailureSurfacesWorkerMessage(t *testing.T) {
	w := &fakeWorker{ready: true, err: errors.New("dial temporal: connection refused")}
	err := serve(t, w).Start(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("want 502 with the Worker's message, got %v", err)
	}
}

func TestDrainIsIdempotent(t *testing.T) {
	w := &fakeWorker{ready: true}
	c := serve(t, w)
	for range 2 {
		if err := c.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if w.drains != 2 {
		t.Fatalf("drains = %d", w.drains)
	}
}

func TestInvalidRequestRejectedOnBothSides(t *testing.T) {
	bad := req
	bad.Address = "no-port"
	// Client side: never sent.
	dialed := false
	c := Client{Host: "x", Dial: func(context.Context) (net.Conn, error) { dialed = true; return nil, errors.New("unreachable") }}
	if err := c.Start(context.Background(), bad); err == nil || dialed {
		t.Fatalf("client must reject before dialing: err=%v dialed=%v", err, dialed)
	}
	// Server side: a client that skips validation still gets 400.
	srv := httptest.NewServer(NewHandler(&fakeWorker{ready: true}))
	defer srv.Close()
	resp, err := http.Post(srv.URL+PathStart, "application/json", strings.NewReader(`{"identity":"worker-0"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}

func TestReadyz(t *testing.T) {
	w := &fakeWorker{}
	srv := httptest.NewServer(NewHandler(w))
	defer srv.Close()
	for _, want := range []int{http.StatusServiceUnavailable, http.StatusOK} {
		resp, err := http.Get(srv.URL + PathReady)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("status %d, want %d", resp.StatusCode, want)
		}
		w.mu.Lock()
		w.ready = true
		w.mu.Unlock()
	}
}

func TestTimingBudget(t *testing.T) {
	if DrainTimeout <= WorkerStopTimeout {
		t.Fatal("the provider must wait longer for /drain than the Worker may take")
	}
	if StartTimeout <= DialTimeout {
		t.Fatal("the provider must wait longer for /start than the Worker's dial")
	}
}

func TestClientHonoursContext(t *testing.T) {
	// A Worker that never answers.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	c := Client{Host: "x", Dial: func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", ln.Addr().String())
		cancel() // give up as soon as the request is on its way
		return conn, err
	}}
	if err := c.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
