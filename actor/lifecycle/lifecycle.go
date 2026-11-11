// Package lifecycle makes a Temporal Worker safe to run as a Substrate actor.
//
// It serves workerprotocol on the actor's port and keeps the Worker
// disconnected from Temporal except between /start and /drain, so no
// connection, long-poll or cached workflow state is ever inside a snapshot.
// Everything the Worker needs to connect (address, namespace, deployment
// version, identity) arrives in the /start request; the app only says which
// task queue it polls and what it registers.
//
//	func main() {
//		err := lifecycle.Run(ctx, lifecycle.Options{
//			TaskQueue: "my-task-queue",
//			Register: func(r lifecycle.Registry) {
//				r.RegisterWorkflow(MyWorkflow)
//				r.RegisterActivity(MyActivity)
//			},
//		})
//	}
//
// Do all expensive, connection-free initialization before Run: the golden
// snapshot is taken as soon as Run answers the wakeup probe, so every actor
// inherits it. Don't create anything there that must differ per actor (IDs,
// random seeds); every actor restored from the snapshot would share it.
package lifecycle

import (
	"context"
	"errors"
	"sync"

	"temporal-substrate-demo/workerprotocol"
)

// Registry is the Temporal Worker the app registers its code with.
// go.temporal.io/sdk/worker.Worker satisfies it.
type Registry interface {
	RegisterWorkflow(w interface{})
	RegisterActivity(a interface{})
}

// Options configure Run.
type Options struct {
	// TaskQueue the Worker polls.
	TaskQueue string
	// Register registers the app's workflows and activities. Called on every
	// /start, with a new Temporal Worker each time.
	Register func(Registry)
	// CAFile verifies the frontend when a StartRequest asks for TLS (with
	// Substrate: the egress gateway's MITM CA from the template's trust-bundle
	// volume). Default: $SSL_CERT_FILE.
	CAFile string
}

func (o Options) validate() error {
	if o.TaskQueue == "" {
		return errors.New("lifecycle: Options.TaskQueue is empty")
	}
	if o.Register == nil {
		return errors.New("lifecycle: Options.Register is nil")
	}
	return nil
}

// session is one connected, polling Temporal Worker.
type session interface {
	// Stop stops polling, waits for in-flight tasks (bounded by
	// workerprotocol.WorkerStopTimeout) and disconnects.
	Stop()
}

// connectFunc connects and starts polling for req. The Temporal SDK adapter is
// in temporal.go; tests use a fake.
type connectFunc func(ctx context.Context, req workerprotocol.StartRequest) (session, error)

// actorWorker implements workerprotocol.Worker: at most one session, started
// and stopped only through the protocol.
type actorWorker struct {
	connect connectFunc

	// mu serializes Start and Drain, so a Drain that arrives during a
	// connect waits for it and then stops it.
	mu      sync.Mutex
	current *workerprotocol.StartRequest
	sess    session
}

var _ workerprotocol.Worker = (*actorWorker)(nil)

// Ready is true from the moment the protocol is served: Run is called after
// the app's initialization.
func (w *actorWorker) Ready() bool { return true }

func (w *actorWorker) Start(ctx context.Context, req workerprotocol.StartRequest) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current != nil {
		if *w.current == req {
			return nil
		}
		return workerprotocol.ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, workerprotocol.DialTimeout)
	defer cancel()
	s, err := w.connect(ctx, req)
	if err != nil {
		return err
	}
	w.current, w.sess = &req, s
	return nil
}

func (w *actorWorker) Drain(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sess != nil {
		w.sess.Stop()
		w.current, w.sess = nil, nil
	}
	return nil
}
