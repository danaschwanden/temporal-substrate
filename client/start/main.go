// Command start is the quickstart's starter with three changes:
//   - The workflow is pinned to the Worker Deployment Version the WCI scales.
//   - It is started by its registered name, "SayHelloWorkflow", so the client
//     doesn't import the actor's code.
//   - Each run gets its own workflow ID, so several can run at once (the
//     quickstart's fixed "greeting-workflow" would collide). Without a version
//
// (or a Current Version on the deployment) the task would wait for an
// unversioned Worker, and the WCI, which serves one version, would never see it.
package main

import (
	"context"
	"log"
	"os"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{
		HostPort:  envOr("TEMPORAL_ADDRESS", "localhost:7233"),
		Namespace: envOr("TEMPORAL_NAMESPACE", "default"),
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	name := "Temporal"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	options := client.StartWorkflowOptions{
		ID:        "greeting-workflow-" + uuid.NewString()[:8],
		TaskQueue: "my-task-queue",
		VersioningOverride: &client.PinnedVersioningOverride{
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: envOr("TEMPORAL_DEPLOYMENT_NAME", "hello"),
				BuildID:        envOr("TEMPORAL_BUILD_ID", "v1"),
			},
		},
	}

	we, err := c.ExecuteWorkflow(context.Background(), options, "SayHelloWorkflow", name)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	var result string
	if err := we.Get(context.Background(), &result); err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow result:", result)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
