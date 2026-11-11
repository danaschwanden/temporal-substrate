//go:build computeprovider_substrate

package computeprovider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common/dynamicconfig"
)

// Operator-side settings. These belong to whoever runs the Temporal server,
// not to the tenant writing the WCI spec.
//
// TODO: move to dynamic config settings (like WorkerControllerAWSIntermediaryRoles)
// so they can differ per namespace and change without a restart.
const (
	envSubstrateAddress = "SUBSTRATE_API_ADDRESS"   // ate-api-server host:port
	envSubstrateCert    = "SUBSTRATE_TLS_CERT_FILE" // client cert: an SPIFFE SVID ate-api-server trusts
	envSubstrateKey     = "SUBSTRATE_TLS_KEY_FILE"
	envSubstrateCA      = "SUBSTRATE_TLS_CA_FILE" // bundle that verifies ate-api-server's serving cert
)

// dialSubstrate builds a lazily connecting ate-api-server client authenticated
// with SPIFFE mTLS. ate-api-server also accepts OIDC JWTs; mTLS is used here
// because it needs no token minting or refresh in the provider.
func dialSubstrate(_ context.Context, _ *dynamicconfig.Collection) (actorControl, error) {
	addr := os.Getenv(envSubstrateAddress)
	certFile, keyFile, caFile := os.Getenv(envSubstrateCert), os.Getenv(envSubstrateKey), os.Getenv(envSubstrateCA)
	if addr == "" || certFile == "" || keyFile == "" || caFile == "" {
		return nil, errors.New("substrate provider needs " + envSubstrateAddress + ", " +
			envSubstrateCert + ", " + envSubstrateKey + " and " + envSubstrateCA)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", envSubstrateCA, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates in %s", caFile)
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    roots,
		// SVIDs are short-lived and rotated on disk, so load the pair on every
		// handshake instead of once at startup.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pair, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("load substrate client cert: %w", err)
			}
			return &pair, nil
		},
	}
	// TODO: the serving cert carries a SPIFFE URI SAN, not necessarily a DNS
	// name matching addr. Verify the expected SPIFFE ID via
	// VerifyPeerCertificate (or use go-spiffe's tlsconfig) instead of relying
	// on hostname verification.

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("dial ate-api-server at %s: %w", addr, err)
	}
	return ateapi.NewControlClient(conn), nil
}
