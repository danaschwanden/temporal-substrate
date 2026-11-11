//go:build computeprovider_substrate

package computeprovider

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"

	"temporal-substrate-demo/workerprotocol"
)

// TestRouterWorkersEndToEnd runs routerWorkers and substrateTLS against a
// local stand-in for atenet-router (mTLS listener, CONNECT with the
// ate-target-actor header) with the real workerprotocol server behind it, so
// the provider's adapter is tested against the actor side of the contract.
func TestRouterWorkersEndToEnd(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := newCA(t)
	writePEM(t, filepath.Join(dir, "ca.crt"), "CERTIFICATE", ca.Raw)

	serverCert := issue(t, ca, caKey, func(c *x509.Certificate) {
		c.DNSNames = []string{"atenet-router.ate-system.svc"}
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	})
	spiffe, _ := url.Parse("spiffe://cluster.local/ns/temporal/sa/temporal-server")
	clientCert := issue(t, ca, caKey, func(c *x509.Certificate) {
		c.URIs = []*url.URL{spiffe}
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	})
	// Pod-certificate layout: private key first, then the chain, one file.
	keyDER, _ := x509.MarshalPKCS8PrivateKey(clientCert.PrivateKey)
	bundle := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]})...)
	if err := os.WriteFile(filepath.Join(dir, "credential-bundle.pem"), bundle, 0o600); err != nil {
		t.Fatal(err)
	}

	roots := x509.NewCertPool()
	roots.AddCert(ca)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Behind the router: the real workerprotocol server with a recording Worker.
	type seen struct{ target, dest, spiffeID string }
	got := make(chan seen, 8)
	worker := &recordingWorker{}
	handler := workerprotocol.NewHandler(worker)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				br := bufio.NewReader(conn)
				connect, err := http.ReadRequest(br)
				if err != nil || connect.Method != http.MethodConnect {
					return
				}
				id := conn.(*tls.Conn).ConnectionState().PeerCertificates[0].URIs[0].String()
				got <- seen{connect.Header.Get(targetActorHeader), connect.Host, id}
				conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				_ = rec.Result().Write(conn)
			}(conn)
		}
	}()

	tlsCfg, err := substrateTLS(filepath.Join(dir, "ca.crt"), filepath.Join(dir, "credential-bundle.pem"), "atenet-router.ate-system.svc")
	if err != nil {
		t.Fatal(err)
	}
	w := &routerWorkers{addr: ln.Addr().String(), tls: tlsCfg}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	actor := &ateapi.ObjectRef{Atespace: "hello-v1", Name: "worker-0"}
	req := workerprotocol.StartRequest{
		Identity: "worker-0", Address: "temporal-frontend.temporal.svc.cluster.local:7233",
		Namespace: "default", DeploymentName: "hello", BuildID: "v1",
	}
	if err := w.Start(ctx, actor, req); err != nil {
		t.Fatalf("Start: %v", err)
	}
	s := <-got
	if s.target != "hello-v1/worker-0" || s.dest != "worker-0:80" {
		t.Fatalf("router saw %+v", s)
	}
	if s.spiffeID != spiffe.String() {
		t.Fatalf("client presented %q", s.spiffeID)
	}
	if got := worker.last(); got != req {
		t.Fatalf("Worker received %+v, want %+v", got, req)
	}

	if err := w.Drain(ctx, actor); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	<-got
	if worker.drains() != 1 {
		t.Fatal("Worker did not receive /drain")
	}

	// A Worker that can't reach Temporal: its 502 reaches the provider.
	worker.setErr(errors.New("dial temporal: connection refused"))
	if err := w.Start(ctx, actor, req); err == nil || !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("want the Worker's 502 surfaced, got %v", err)
	}
	<-got

	// A server certificate for another name must be rejected.
	wrong, _ := substrateTLS(filepath.Join(dir, "ca.crt"), filepath.Join(dir, "credential-bundle.pem"), "api.ate-system.svc")
	if err := (&routerWorkers{addr: ln.Addr().String(), tls: wrong}).Start(ctx, actor, req); err == nil {
		t.Fatal("hostname verification must fail for the wrong server name")
	}
}

type recordingWorker struct {
	mu     sync.Mutex
	req    workerprotocol.StartRequest
	nDrain int
	err    error
}

func (r *recordingWorker) Ready() bool { return true }
func (r *recordingWorker) Start(_ context.Context, req workerprotocol.StartRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.req = req
	return r.err
}
func (r *recordingWorker) Drain(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nDrain++
	return nil
}
func (r *recordingWorker) last() workerprotocol.StartRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.req
}
func (r *recordingWorker) drains() int      { r.mu.Lock(); defer r.mu.Unlock(); return r.nDrain }
func (r *recordingWorker) setErr(err error) { r.mu.Lock(); defer r.mu.Unlock(); r.err = err }

func newCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	return ca, key
}

func issue(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, mut func(*x509.Certificate)) tls.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	mut(tmpl)
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
