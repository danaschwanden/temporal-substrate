// Command setup registers the demo with Temporal: the namespace, the Worker
// Deployment, and its version v1 with a compute config that hands scaling to
// the WCI and its substrate compute provider. It is idempotent: anything that
// already exists is left as it is.
//
// The calls mirror temporal-auto-scaled-workers/tests/integration/worker_set_test.go.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	computepb "go.temporal.io/api/compute/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	addr := flag.String("address", envOr("TEMPORAL_ADDRESS", "localhost:7233"), "Temporal frontend (default: $TEMPORAL_ADDRESS)")
	namespace := flag.String("namespace", "default", "Temporal namespace")
	deployment := flag.String("deployment", "hello", "Worker Deployment name")
	buildID := flag.String("build-id", "v1", "Build ID of the version")
	atespace := flag.String("atespace", "hello-v1", "Substrate atespace dedicated to this version's actor pool")
	template := flag.String("actor-template", "hello-worker", "Substrate ActorTemplate for this build ID")
	temporalHost := flag.String("temporal-host", "temporal-frontend.temporal.svc.cluster.local", "frontend host the Workers dial; the provider sends it in /start and allows it in their egress policy")
	poolSize := flag.String("pool-size", "3", "actors in the pool")
	maxCount := flag.String("max-count", "3", "rate-based scaler: max running Workers (<= pool size)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// 1. Namespace (the SQLite dev server starts without one).
	nsClient, err := client.NewNamespaceClient(client.Options{HostPort: *addr})
	if err != nil {
		log.Fatalln("dial:", err)
	}
	err = nsClient.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        *namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	if exists := new(serviceerror.NamespaceAlreadyExists); err != nil && !errors.As(err, &exists) {
		log.Fatalln("register namespace:", err)
	}
	nsClient.Close()
	log.Println("namespace ready:", *namespace)

	c, err := client.Dial(client.Options{HostPort: *addr, Namespace: *namespace})
	if err != nil {
		log.Fatalln("dial:", err)
	}
	defer c.Close()
	svc := c.WorkflowService()

	// A freshly registered namespace takes a few seconds to reach every
	// service, so retry the first calls.
	retry := func(what string, call func() error) {
		for {
			err := call()
			if err == nil || isAlreadyExists(err) {
				log.Println(what, "ready")
				return
			}
			if ctx.Err() != nil {
				log.Fatalln(what+":", err)
			}
			log.Printf("%s: %v (retrying)", what, err)
			time.Sleep(2 * time.Second)
		}
	}

	// 2. The Worker Deployment.
	retry("deployment "+*deployment, func() error {
		_, err := svc.CreateWorkerDeployment(ctx, &workflowservice.CreateWorkerDeploymentRequest{
			Namespace:      *namespace,
			DeploymentName: *deployment,
			Identity:       "hello-setup",
			RequestId:      uuid.NewString(),
		})
		return err
	})

	// 3. Version v1 with its compute config. Provider and scaler details are
	// payloads made with the SDK's default converter, as the API asks for
	// server-implemented providers.
	// The deployment version itself isn't in here: the WCI passes it to the
	// provider, which hands it to each Worker in /start.
	providerDetails := mustPayload(map[string]string{
		"atespace":       *atespace,
		"actor_template": *template,
		"pool_size":      *poolSize,
		"temporal_host":  *temporalHost,
		"temporal_port":  "7233",
		"temporal_tls":   "false", // the in-cluster dev server is cleartext
	})
	scalerDetails := mustPayload(map[string]string{
		"min_count": "0",
		// Start one Worker as soon as the version exists, so its task queue
		// gets registered with the version. The scaler then scales it back to
		// zero once the queue is idle.
		"initial_count":            "1",
		"max_count":                *maxCount,
		"metrics_poll_interval_ms": "10000",
		"scale_down_cooldown_ms":   "30000",
	})
	retry("version "+*deployment+"/"+*buildID, func() error {
		_, err := svc.CreateWorkerDeploymentVersion(ctx, &workflowservice.CreateWorkerDeploymentVersionRequest{
			Namespace: *namespace,
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: *deployment,
				BuildId:        *buildID,
			},
			Identity:  "hello-setup",
			RequestId: uuid.NewString(),
			ComputeConfig: &computepb.ComputeConfig{
				ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
					// No task types: the catch-all group serves workflow,
					// activity and Nexus tasks.
					"default": {
						Provider: &computepb.ComputeProvider{Type: "substrate", Details: providerDetails},
						Scaler:   &computepb.ComputeScaler{Type: "rate-based", Details: scalerDetails},
					},
				},
			},
		})
		return err
	})
}

func mustPayload(v map[string]string) *commonpb.Payload {
	p, err := converter.GetDefaultDataConverter().ToPayload(v)
	if err != nil {
		log.Fatalln("encode details:", err)
	}
	return p
}

func isAlreadyExists(err error) bool {
	var exists *serviceerror.AlreadyExists
	return errors.As(err, &exists)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
