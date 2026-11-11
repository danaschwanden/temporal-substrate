package lifecycle

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"temporal-substrate-demo/workerprotocol"
)

// The tests drive the module the way the provider does: through
// workerprotocol.Client against workerprotocol.NewHandler. Only the Temporal
// connection is faked, at the connectFunc seam.

type fakeTemporal struct {
	mu       sync.Mutex
	connects []workerprotocol.StartRequest
	live     int // sessions not yet stopped
	fail     error
	delay    time.Duration
	deadline time.Duration // deadline seen by the last connect
}

func (f *fakeTemporal) connect(ctx context.Context, req workerprotocol.StartRequest) (session, error) {
	if dl, ok := ctx.Deadline(); ok {
		f.mu.Lock()
		f.deadline = time.Until(dl)
		f.mu.Unlock()
	}
	time.Sleep(f.delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return nil, f.fail
	}
	f.connects = append(f.connects, req)
	f.live++
	return stopFunc(func() { f.mu.Lock(); f.live--; f.mu.Unlock() }), nil
}

func (f *fakeTemporal) state() (connects, live int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.connects), f.live
}

type stopFunc func()

func (s stopFunc) Stop() { s() }

func newActor(t *testing.T) (*fakeTemporal, workerprotocol.Client) {
	f := &fakeTemporal{}
	srv := httptest.NewServer(workerprotocol.NewHandler(&actorWorker{connect: f.connect}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	return f, workerprotocol.Client{Host: "worker-0", Dial: func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}}
}

var req = workerprotocol.StartRequest{
	Identity:       "worker-0",
	Address:        "temporal-frontend.temporal.svc.cluster.local:7233",
	Namespace:      "default",
	DeploymentName: "hello",
	BuildID:        "v1",
}

func TestDisconnectedUntilStartAndAfterDrain(t *testing.T) {
	f, c := newActor(t)
	ctx := context.Background()
	if _, live := f.state(); live != 0 {
		t.Fatal("must not connect before /start (the golden snapshot is taken then)")
	}
	if err := c.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	if connects, live := f.state(); connects != 1 || live != 1 {
		t.Fatalf("after /start: connects=%d live=%d", connects, live)
	}
	if f.connects[0] != req {
		t.Fatalf("connected with %+v, want the /start request", f.connects[0])
	}
	if err := c.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, live := f.state(); live != 0 {
		t.Fatal("a drained actor must hold no session: it is about to be snapshotted")
	}
}

func TestRepeatedStartDoesNotReconnect(t *testing.T) {
	f, c := newActor(t)
	for range 3 {
		if err := c.Start(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if connects, _ := f.state(); connects != 1 {
		t.Fatalf("connects = %d, want 1", connects)
	}
}

func TestConcurrentStartsConnectOnce(t *testing.T) {
	f, c := newActor(t)
	f.delay = 50 * time.Millisecond
	var wg sync.WaitGroup
	var failures atomic.Int32
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Start(context.Background(), req); err != nil {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if connects, live := f.state(); connects != 1 || live != 1 || failures.Load() != 0 {
		t.Fatalf("connects=%d live=%d failures=%d", connects, live, failures.Load())
	}
}

func TestStartWithDifferentVersionConflicts(t *testing.T) {
	_, c := newActor(t)
	if err := c.Start(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	v2 := req
	v2.BuildID = "v2"
	if err := c.Start(context.Background(), v2); !errors.Is(err, workerprotocol.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}

func TestFailedConnectLeavesActorRetryable(t *testing.T) {
	f, c := newActor(t)
	f.fail = errors.New("connection refused")
	if err := c.Start(context.Background(), req); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("want the connect error, got %v", err)
	}
	f.mu.Lock()
	f.fail = nil
	f.mu.Unlock()
	if err := c.Start(context.Background(), req); err != nil {
		t.Fatalf("retry after a failed connect: %v", err)
	}
}

func TestConnectIsBoundedByDialTimeout(t *testing.T) {
	f, c := newActor(t)
	if err := c.Start(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if f.deadline <= 0 || f.deadline > workerprotocol.DialTimeout {
		t.Fatalf("connect deadline %v, want within DialTimeout (%v)", f.deadline, workerprotocol.DialTimeout)
	}
}

func TestDrainBeforeStartAndRestartAfterDrain(t *testing.T) {
	f, c := newActor(t)
	ctx := context.Background()
	if err := c.Drain(ctx); err != nil {
		t.Fatalf("drain before start: %v", err)
	}
	for range 2 { // resume, suspend, resume
		if err := c.Start(ctx, req); err != nil {
			t.Fatal(err)
		}
		if err := c.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if connects, live := f.state(); connects != 2 || live != 0 {
		t.Fatalf("connects=%d live=%d, want a fresh connection per resume", connects, live)
	}
}

func TestOptionsValidation(t *testing.T) {
	if err := (Options{Register: func(Registry) {}}).validate(); err == nil {
		t.Fatal("empty TaskQueue must be rejected")
	}
	if err := (Options{TaskQueue: "q"}).validate(); err == nil {
		t.Fatal("nil Register must be rejected")
	}
}
