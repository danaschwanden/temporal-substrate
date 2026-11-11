//go:build computeprovider_substrate

// Substrate compute provider for Temporal Serverless Workers.
//
// Drop this file (and substrate_dial.go) into
// temporal-auto-scaled-workers/wci/workflow/compute_provider/ and apply
// 0001-add-substrate-compute-provider-type.patch. Build with
// `-tags computeprovider_substrate`.
//
// Model: one WCI scaling group == one pool of Substrate actors, all created
// from the same ActorTemplate (one template per Worker Deployment Version).
// The WCI's rate-based algorithm asks for a worker-set size N; the provider
// keeps actors [0, N) RUNNING and suspends actors [N, pool). Substrate does
// not suspend idle actors on its own, so scale-down has to come from here.

package computeprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/common/dynamicconfig"
)

// Customer-facing config keys (the "config" map of a scaling group's compute spec).
const (
	configSubstrateAtespace      = "atespace"       // required: Substrate atespace holding the pool
	configSubstrateActorTemplate = "actor_template" // required: ActorTemplate for this build ID
	configSubstratePoolSize      = "pool_size"      // optional: actors in the pool; must be >= the algorithm's max_count
	configSubstrateNamePrefix    = "name_prefix"    // optional: overrides the derived actor-name prefix

	defaultSubstratePoolSize = 30 // matches the rate-based algorithm's default max_count
	maxSubstratePoolSize     = 1000
	substrateConcurrency     = 16 // parallel ateapi calls per UpdateWorkerSetSize
)

// actorControl is the subset of ateapi.ControlClient the provider uses.
// ateapi.ControlClient satisfies it; tests use a fake.
type actorControl interface {
	GetActorTemplate(ctx context.Context, in *ateapi.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapi.ActorTemplate, error)
	ListActors(ctx context.Context, in *ateapi.ListActorsRequest, opts ...grpc.CallOption) (*ateapi.ListActorsResponse, error)
	CreateActor(ctx context.Context, in *ateapi.CreateActorRequest, opts ...grpc.CallOption) (*ateapi.Actor, error)
	ResumeActor(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error)
	SuspendActor(ctx context.Context, in *ateapi.SuspendActorRequest, opts ...grpc.CallOption) (*ateapi.SuspendActorResponse, error)
	RevertActor(ctx context.Context, in *ateapi.RevertActorRequest, opts ...grpc.CallOption) (*ateapi.RevertActorResponse, error)
}

type substrateComputeProvider struct {
	control actorControl
}

type substrateConfig struct {
	atespace   string
	template   string
	poolSize   int32
	namePrefix string
}

func init() {
	RegisterComputeProvider(iface.ComputeProviderTypeSubstrate, NewSubstrateComputeProvider)
}

// NewSubstrateComputeProvider connects to ate-api-server. The endpoint and the
// credentials are operator-side settings (see substrate_dial.go), never part of
// the customer config: a customer-chosen endpoint would let a tenant point the
// server's Substrate identity at an arbitrary host.
func NewSubstrateComputeProvider(ctx context.Context, dc *dynamicconfig.Collection) (ComputeProvider, error) {
	control, err := dialSubstrate(ctx, dc)
	if err != nil {
		return nil, NewProviderError(FailureInternal, errors.Join(errWCIOwned, err))
	}
	return &substrateComputeProvider{control: control}, nil
}

func (p *substrateComputeProvider) LaunchStrategy() LaunchStrategy {
	return LaunchStrategyWorkerSet
}

func (p *substrateComputeProvider) ValidateConfig(ctx context.Context, rc RequestContext, config ComputeProviderConfig) error {
	cfg, err := parseSubstrateConfig(rc, config)
	if err != nil {
		return NewProviderError(FailureRejected, err)
	}
	_, err = p.control.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
		ActorTemplate: &ateapi.ObjectRef{Atespace: cfg.atespace, Name: cfg.template},
	})
	if err != nil {
		return classifySubstrateError(fmt.Errorf("actor template %s/%s: %w", cfg.atespace, cfg.template, err))
	}
	// TODO: check the template's GoldenSnapshotStatus. Without a golden
	// snapshot every new actor cold-boots the Worker instead of resuming a
	// pre-warmed one, which defeats the point of this provider.
	return nil
}

func (p *substrateComputeProvider) InvokeWorker(context.Context, RequestContext, ComputeProviderConfig) error {
	return errors.ErrUnsupported
}

// UpdateWorkerSetSize converges the pool to exactly `size` running actors.
// It is idempotent and keeps no state: the WCI retries this activity, so the
// desired state is derived from actor names alone and every call recomputes it.
func (p *substrateComputeProvider) UpdateWorkerSetSize(ctx context.Context, rc RequestContext, config ComputeProviderConfig, size int32) error {
	cfg, err := parseSubstrateConfig(rc, config)
	if err != nil {
		return NewProviderError(FailureRejected, err)
	}
	if size < 0 || size > cfg.poolSize {
		return NewProviderError(FailureRejected,
			fmt.Errorf("requested size %d outside pool [0, %d]; raise %q or lower the algorithm's max_count", size, cfg.poolSize, configSubstratePoolSize))
	}

	existing, err := p.listPool(ctx, cfg)
	if err != nil {
		return classifySubstrateError(err)
	}

	// Lowest indices stay running, so scale-down always suspends the
	// highest-numbered (most recently started) actors first.
	var tasks []func(context.Context) error
	for i := int32(0); i < cfg.poolSize; i++ {
		name := cfg.actorName(i)
		actor, ok := existing[name]
		state := actor.GetStatus().GetState()
		switch {
		case i < size && !ok:
			tasks = append(tasks, func(ctx context.Context) error { return p.createAndResume(ctx, cfg, name) })
		case i < size && state == ateapi.ActorState_ACTOR_STATE_CRASHED:
			tasks = append(tasks, func(ctx context.Context) error { return p.revertAndResume(ctx, cfg, name) })
		case i < size && !isRunningOrResuming(state):
			tasks = append(tasks, func(ctx context.Context) error { return p.resume(ctx, cfg, name) })
		case i >= size && ok && isRunningOrResuming(state):
			tasks = append(tasks, func(ctx context.Context) error { return p.suspend(ctx, cfg, name) })
		}
	}
	return classifySubstrateError(runBounded(ctx, substrateConcurrency, tasks))
}

func (p *substrateComputeProvider) listPool(ctx context.Context, cfg substrateConfig) (map[string]*ateapi.Actor, error) {
	pool := map[string]*ateapi.Actor{}
	prefix := cfg.namePrefix + "-"
	token := ""
	for {
		resp, err := p.control.ListActors(ctx, &ateapi.ListActorsRequest{
			Atespace: cfg.atespace,
			Options:  &ateapi.ListOptions{PageSize: 1000, PageToken: token},
		})
		if err != nil {
			return nil, fmt.Errorf("list actors in %s: %w", cfg.atespace, err)
		}
		// ListActors has no name filter, so a shared atespace means paging
		// through other actors too. A dedicated atespace per Worker
		// deployment keeps this cheap.
		for _, a := range resp.GetActors() {
			if name := a.GetMetadata().GetName(); strings.HasPrefix(name, prefix) {
				pool[name] = a
			}
		}
		if token = resp.GetNextPageToken(); token == "" {
			return pool, nil
		}
	}
}

func (p *substrateComputeProvider) createAndResume(ctx context.Context, cfg substrateConfig, name string) error {
	_, err := p.control.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: cfg.atespace, Name: name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: cfg.atespace, Name: cfg.template},
	}})
	// AlreadyExists: a previous attempt of this activity created it; carry on.
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create actor %s: %w", name, err)
	}
	return p.resume(ctx, cfg, name)
}

func (p *substrateComputeProvider) revertAndResume(ctx context.Context, cfg substrateConfig, name string) error {
	// A crashed Worker is reverted to its durable snapshot before resuming.
	// Any task it held times out on the Temporal side and is retried.
	if _, err := p.control.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: cfg.ref(name)}); err != nil {
		return fmt.Errorf("revert actor %s: %w", name, err)
	}
	return p.resume(ctx, cfg, name)
}

func (p *substrateComputeProvider) resume(ctx context.Context, cfg substrateConfig, name string) error {
	// ResumeActor is a no-op (resumed=false) for an actor that is already RUNNING.
	if _, err := p.control.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: cfg.ref(name)}); err != nil {
		return fmt.Errorf("resume actor %s: %w", name, err)
	}
	return nil
}

func (p *substrateComputeProvider) suspend(ctx context.Context, cfg substrateConfig, name string) error {
	// TODO(drain): this snapshots the Worker as-is, possibly mid-Activity. The
	// task is not lost (Temporal times it out and retries it elsewhere), but a
	// graceful drain first is better: signal the Worker through the router to
	// stop polling, wait for in-flight tasks, then suspend.
	_, err := p.control.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   cfg.ref(name),
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("suspend actor %s: %w", name, err)
	}
	return nil
}

func isRunningOrResuming(s ateapi.ActorState) bool {
	return s == ateapi.ActorState_ACTOR_STATE_RUNNING || s == ateapi.ActorState_ACTOR_STATE_RESUMING
}

// --- config -----------------------------------------------------------------

func parseSubstrateConfig(rc RequestContext, config ComputeProviderConfig) (substrateConfig, error) {
	cfg := substrateConfig{poolSize: defaultSubstratePoolSize}

	var ok bool
	if cfg.atespace, ok = config[configSubstrateAtespace].(string); !ok || cfg.atespace == "" {
		return cfg, fmt.Errorf("%q not found in config", configSubstrateAtespace)
	}
	if cfg.template, ok = config[configSubstrateActorTemplate].(string); !ok || cfg.template == "" {
		return cfg, fmt.Errorf("%q not found in config", configSubstrateActorTemplate)
	}
	if raw, present := config[configSubstratePoolSize]; present {
		// Spec configs arrive as JSON, so numbers may be strings (as in the
		// other providers) or float64.
		n, err := toInt32(raw)
		if err != nil || n < 1 || n > maxSubstratePoolSize {
			return cfg, fmt.Errorf("%q must be an integer in [1, %d]", configSubstratePoolSize, maxSubstratePoolSize)
		}
		cfg.poolSize = n
	}
	if prefix, _ := config[configSubstrateNamePrefix].(string); prefix != "" {
		cfg.namePrefix = prefix
	} else {
		cfg.namePrefix = derivedNamePrefix(rc)
	}
	if !isShortName(cfg.namePrefix) || len(cfg.namePrefix) > 50 {
		return cfg, fmt.Errorf("%q must be lowercase [a-z0-9-] and at most 50 characters", configSubstrateNamePrefix)
	}
	return cfg, nil
}

// derivedNamePrefix is stable per (namespace, deployment, build ID). Those
// values may contain characters that Substrate names (k8s short names) reject,
// so the prefix is a hash, not the values themselves.
func derivedNamePrefix(rc RequestContext) string {
	sum := sha256.Sum256([]byte(rc.NamespaceName + "/" + rc.DeploymentName + "/" + rc.DeploymentBuildID))
	return "tw-" + hex.EncodeToString(sum[:])[:12]
}

func (c substrateConfig) actorName(i int32) string {
	return c.namePrefix + "-" + strconv.Itoa(int(i))
}

func (c substrateConfig) ref(name string) *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: c.atespace, Name: name}
}

func toInt32(v any) (int32, error) {
	switch n := v.(type) {
	case string:
		i, err := strconv.ParseInt(n, 10, 32)
		return int32(i), err
	case float64:
		if n != float64(int32(n)) {
			return 0, fmt.Errorf("not an integer: %v", n)
		}
		return int32(n), nil
	case int:
		return int32(n), nil
	}
	return 0, fmt.Errorf("unsupported type %T", v)
}

func isShortName(s string) bool {
	if s == "" || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

// --- helpers ----------------------------------------------------------------

// runBounded runs tasks with at most `limit` in flight and joins all errors, so
// one bad actor does not stop the rest of the pool from converging.
func runBounded(ctx context.Context, limit int, tasks []func(context.Context) error) error {
	sem := make(chan struct{}, limit)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, task := range tasks {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := task(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// classifySubstrateError maps gRPC codes from ate-api-server onto the WCI's
// provider-agnostic failure classes. For joined errors the first classified
// cause wins.
func classifySubstrateError(err error) error {
	if err == nil {
		return nil
	}
	var pe *ProviderError
	if errors.As(err, &pe) {
		return err
	}
	class := FailureUnclassified
	// status.Code walks wrapped and joined errors (errors.As on GRPCStatus).
	switch status.Code(err) {
	case codes.NotFound:
		class = FailureNotFound
	case codes.PermissionDenied, codes.Unauthenticated:
		class = FailureAccessDenied
	case codes.InvalidArgument:
		class = FailureRejected
	case codes.ResourceExhausted:
		class = FailureThrottled
	case codes.Aborted, codes.FailedPrecondition:
		// Version conflicts and actors mid-transition (RESUMING/SUSPENDING):
		// safe to retry once the actor settles.
		class = FailureConflict
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal:
		class = FailureUnavailable
	}
	return NewProviderError(class, err)
}
