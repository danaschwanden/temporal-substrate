package lifecycle

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"temporal-substrate-demo/workerprotocol"
)

// With TLS (Temporal Cloud), Substrate's egress gateway replaces this with the
// real API key. The actor never holds the secret, so no snapshot does either.
const apiKeyPlaceholder = "injected-by-substrate-egress"

// Run serves workerprotocol on workerprotocol.Port until ctx is done.
func Run(ctx context.Context, opts Options) error {
	if err := opts.validate(); err != nil {
		return err
	}
	if opts.CAFile == "" {
		opts.CAFile = os.Getenv("SSL_CERT_FILE")
	}
	w := &actorWorker{connect: sdkConnect(opts)}
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", workerprotocol.Port),
		Handler:           workerprotocol.NewHandler(w),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = w.Drain(context.Background())
		_ = srv.Shutdown(context.Background())
	}()
	slog.Info("serving workerprotocol; waiting for /start", "port", workerprotocol.Port, "task_queue", opts.TaskQueue)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// sdkConnect is the Temporal SDK adapter for connectFunc. It always opens a
// new connection: it runs after a resume, when any connection from before
// the snapshot would be dead.
func sdkConnect(opts Options) connectFunc {
	return func(ctx context.Context, req workerprotocol.StartRequest) (session, error) {
		copts := client.Options{
			HostPort:  req.Address,
			Namespace: req.Namespace,
			Identity:  req.Identity,
		}
		if req.TLS {
			roots, err := loadRoots(opts.CAFile)
			if err != nil {
				return nil, err
			}
			copts.Credentials = client.NewAPIKeyStaticCredentials(apiKeyPlaceholder)
			copts.ConnectionOptions = client.ConnectionOptions{
				TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
			}
		}
		c, err := client.DialContext(ctx, copts)
		if err != nil {
			return nil, fmt.Errorf("dial temporal at %s: %w", req.Address, err)
		}
		w := worker.New(c, opts.TaskQueue, worker.Options{
			Identity: req.Identity,
			// Serverless Workers need Worker Versioning: each actor pool
			// serves one Worker Deployment Version.
			DeploymentOptions: worker.DeploymentOptions{
				UseVersioning: true,
				Version: worker.WorkerDeploymentVersion{
					DeploymentName: req.DeploymentName,
					BuildID:        req.BuildID,
				},
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			WorkerStopTimeout: workerprotocol.WorkerStopTimeout,
		})
		opts.Register(w)
		if err := w.Start(); err != nil { // non-blocking
			c.Close()
			return nil, fmt.Errorf("start worker: %w", err)
		}
		slog.Info("polling", "identity", req.Identity, "address", req.Address,
			"version", req.DeploymentName+"/"+req.BuildID, "task_queue", opts.TaskQueue)
		return sdkSession{c: c, w: w}, nil
	}
}

type sdkSession struct {
	c client.Client
	w worker.Worker
}

func (s sdkSession) Stop() {
	s.w.Stop()
	s.c.Close()
	slog.Info("drained")
}

func loadRoots(caFile string) (*x509.CertPool, error) {
	if caFile == "" {
		return nil, errors.New("TLS requested but no CA file: set Options.CAFile or SSL_CERT_FILE")
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", caFile)
	}
	return roots, nil
}
