// Command worker is the quickstart's Worker, run as a Substrate actor.
//
// Compared with the quickstart's worker/main.go, it doesn't dial Temporal
// itself. lifecycle.Run connects when the substrate compute provider sends
// /start (with the address, namespace and deployment version) and disconnects
// on /drain, so the actor is never connected while Substrate snapshots it.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"my-org/quickstart"
	"my-org/quickstart/lifecycle"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	err := lifecycle.Run(ctx, lifecycle.Options{
		TaskQueue: "my-task-queue",
		Register: func(r lifecycle.Registry) {
			r.RegisterWorkflow(quickstart.SayHelloWorkflow)
			r.RegisterActivity(quickstart.Greet)
		},
	})
	if err != nil {
		slog.Error("worker", "err", err)
		os.Exit(1)
	}
}
