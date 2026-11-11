// Package workerprotocol is the contract between the substrate compute
// provider (in the Temporal server) and the Temporal Worker inside a Substrate
// actor. It is the only code both sides share, and it uses only the standard
// library, so importing it adds nothing to the Temporal server build.
//
// The contract keeps a Worker disconnected from Temporal whenever Substrate
// may snapshot it:
//
//	GET  /readyz  Substrate's wakeup probe. 200 once the process is initialized.
//	              The golden snapshot is taken right after the first 200.
//	POST /start   Provider, after resuming the actor. Body: StartRequest.
//	              Connect to Temporal and poll. Idempotent.
//	POST /drain   Provider, before suspending the actor. Stop polling, finish
//	              in-flight tasks within WorkerStopTimeout, disconnect. Idempotent.
//
// Server side: NewHandler(Worker). Client side: Client.
package workerprotocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Port is the actor port the Worker serves the protocol on. The ActorTemplate's
// wakeupProbe must use it too.
const Port = 80

const (
	PathReady = "/readyz"
	PathStart = "/start"
	PathDrain = "/drain"
)

// Timing budget. The invariants hold by construction: the provider waits
// longer than the Worker may take.
const (
	// DialTimeout bounds the Worker's connection to Temporal in /start.
	DialTimeout = 10 * time.Second
	// StartTimeout bounds a /start call: the router may resume the actor
	// first, then the Worker dials.
	StartTimeout = DialTimeout + 35*time.Second
	// WorkerStopTimeout bounds how long /drain waits for in-flight tasks.
	WorkerStopTimeout = 25 * time.Second
	// DrainTimeout bounds a /drain call.
	DrainTimeout = WorkerStopTimeout + 15*time.Second
)

// StartRequest tells the Worker which Temporal deployment version to serve and
// where. The provider is the single source of these values: it knows the
// version from the Worker Controller and the frontend from its config.
type StartRequest struct {
	// Identity of the Worker in Temporal: the actor name, which the actor
	// cannot see from inside.
	Identity string `json:"identity"`
	// Address of the Temporal frontend, host:port. The provider allows
	// exactly this host and port in the actor's egress policy.
	Address string `json:"address"`
	// TLS: dial with TLS through the egress gateway, which terminates it and
	// may inject an API key. False: cleartext h2c.
	TLS            bool   `json:"tls"`
	Namespace      string `json:"namespace"`
	DeploymentName string `json:"deploymentName"`
	BuildID        string `json:"buildId"`
}

// Validate reports the first missing or malformed field.
func (r StartRequest) Validate() error {
	for _, f := range []struct{ name, value string }{
		{"identity", r.Identity}, {"address", r.Address}, {"namespace", r.Namespace},
		{"deploymentName", r.DeploymentName}, {"buildId", r.BuildID},
	} {
		if f.value == "" {
			return fmt.Errorf("start request: %s is empty", f.name)
		}
	}
	if _, _, err := net.SplitHostPort(r.Address); err != nil {
		return fmt.Errorf("start request: address %q is not host:port", r.Address)
	}
	return nil
}

// Worker is what runs inside the actor.
type Worker interface {
	// Ready reports whether the process is initialized.
	Ready() bool
	// Start connects and polls. Idempotent for an equal request; an error
	// wrapping ErrConflict if already started with a different one.
	Start(ctx context.Context, req StartRequest) error
	// Drain stops polling, waits for in-flight tasks and disconnects. Idempotent.
	Drain(ctx context.Context) error
}

// ErrConflict: the Worker is already started with a different StartRequest.
var ErrConflict = errors.New("worker already started with a different request")

// NewHandler serves the protocol for w.
//
// Status codes: 400 invalid request, 409 ErrConflict, 502 Start failed (could
// not reach Temporal), 500 Drain failed, 503 not ready.
func NewHandler(w Worker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+PathReady, func(rw http.ResponseWriter, _ *http.Request) {
		if !w.Ready() {
			http.Error(rw, "initializing", http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST "+PathStart, func(rw http.ResponseWriter, r *http.Request) {
		var req StartRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
			http.Error(rw, "body must be a JSON StartRequest: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := req.Validate(); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		switch err := w.Start(r.Context(), req); {
		case err == nil:
			rw.WriteHeader(http.StatusOK)
		case errors.Is(err, ErrConflict):
			http.Error(rw, err.Error(), http.StatusConflict)
		default:
			http.Error(rw, err.Error(), http.StatusBadGateway)
		}
	})
	mux.HandleFunc("POST "+PathDrain, func(rw http.ResponseWriter, r *http.Request) {
		if err := w.Drain(r.Context()); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
	return mux
}

// Client calls the protocol on one Worker. Dial opens a connection to the
// Worker's Port (for the provider: a tunnel through atenet-router). Each call
// uses its own connection.
type Client struct {
	Dial func(ctx context.Context) (net.Conn, error)
	// Host is sent as the HTTP Host header (the actor name).
	Host string
}

// Start sends /start. Callers should bound ctx with StartTimeout.
func (c Client) Start(ctx context.Context, req StartRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return c.post(ctx, PathStart, body)
}

// Drain sends /drain. Callers should bound ctx with DrainTimeout.
func (c Client) Drain(ctx context.Context) error {
	return c.post(ctx, PathDrain, nil)
}

func (c Client) post(ctx context.Context, path string, body []byte) error {
	conn, err := c.Dial(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+c.Host+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true
	if err := req.Write(conn); err != nil {
		return fmt.Errorf("%s: write: %w", path, err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", path, ctx.Err())
		}
		return fmt.Errorf("%s: read response: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	err = fmt.Errorf("%s: worker answered %s: %s", path, resp.Status, strings.TrimSpace(string(msg)))
	if resp.StatusCode == http.StatusConflict {
		err = fmt.Errorf("%w (%v)", ErrConflict, err)
	}
	return err
}
