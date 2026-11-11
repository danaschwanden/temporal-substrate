//go:build computeprovider_substrate

package computeprovider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common/dynamicconfig"
)

// Operator-side settings, read from the Temporal server's environment. They
// belong to whoever runs the server, not to the tenant writing the compute
// config. The defaults match a Substrate install in the same cluster.
//
// TODO: move to dynamic config settings (like WorkerControllerAWSIntermediaryRoles)
// so they can differ per namespace and change without a restart.
const (
	envSubstrateAPIAddress    = "SUBSTRATE_API_ADDRESS"    // default api.ate-system.svc:443
	envSubstrateRouterAddress = "SUBSTRATE_ROUTER_ADDRESS" // default atenet-router.ate-system.svc:8444
	// Trust bundle for the servicedns.podcert.ate.dev/identity signer, which
	// signs both ate-api-server's and atenet-router's serving certificates.
	envSubstrateCAFile = "SUBSTRATE_CA_FILE"
	// Pod certificate from the podidentity.podcert.ate.dev/identity signer:
	// one PEM with the private key followed by the chain. Its SPIFFE ID
	// (spiffe://cluster.local/ns/<ns>/sa/<sa>) is the principal for
	// ate-api-server and must be allowed by atenet-router.
	envSubstrateCredentialBundle = "SUBSTRATE_CREDENTIAL_BUNDLE"

	defaultSubstrateAPIAddress    = "api.ate-system.svc:443"
	defaultSubstrateRouterAddress = "atenet-router.ate-system.svc:8444"
)

func dialSubstrate(_ context.Context, _ *dynamicconfig.Collection) (actorControl, workerLifecycle, error) {
	apiAddr := envOr(envSubstrateAPIAddress, defaultSubstrateAPIAddress)
	routerAddr := envOr(envSubstrateRouterAddress, defaultSubstrateRouterAddress)
	caFile, bundleFile := os.Getenv(envSubstrateCAFile), os.Getenv(envSubstrateCredentialBundle)
	if caFile == "" || bundleFile == "" {
		return nil, nil, errors.New("substrate provider needs " + envSubstrateCAFile + " and " + envSubstrateCredentialBundle)
	}

	apiTLS, err := substrateTLS(caFile, bundleFile, hostOf(apiAddr))
	if err != nil {
		return nil, nil, err
	}
	routerTLS, err := substrateTLS(caFile, bundleFile, hostOf(routerAddr))
	if err != nil {
		return nil, nil, err
	}

	// The api Service is headless (one address per replica): resolve with the
	// dns resolver and spread calls round-robin, as the Substrate docs advise.
	conn, err := grpc.NewClient("dns:///"+apiAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(apiTLS)),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial ate-api-server at %s: %w", apiAddr, err)
	}
	return ateapi.NewControlClient(conn), &routerWorkers{addr: routerAddr, tls: routerTLS}, nil
}

// substrateTLS presents the pod certificate and verifies the server against
// the servicedns trust bundle. The kubelet rotates both files, so both are
// re-read on every handshake instead of once at startup.
func substrateTLS(caFile, bundleFile, serverName string) (*tls.Config, error) {
	if _, err := loadPool(caFile); err != nil { // fail fast on a bad mount
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pem, err := os.ReadFile(bundleFile)
			if err != nil {
				return nil, fmt.Errorf("read pod certificate: %w", err)
			}
			// X509KeyPair takes the CERTIFICATE blocks from the first argument
			// and the first PRIVATE KEY block from the second, so the
			// one-file bundle works for both.
			pair, err := tls.X509KeyPair(pem, pem)
			if err != nil {
				return nil, fmt.Errorf("parse pod certificate: %w", err)
			}
			return &pair, nil
		},
		// Standard verification, but against a pool loaded per handshake.
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server sent no certificate")
			}
			roots, err := loadPool(caFile)
			if err != nil {
				return err
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				DNSName:       serverName,
				Roots:         roots,
				Intermediates: inter,
			})
			return err
		},
	}, nil
}

func loadPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", envSubstrateCAFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", caFile)
	}
	return pool, nil
}

func hostOf(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return strings.TrimSuffix(h, ".")
	}
	return addr
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
