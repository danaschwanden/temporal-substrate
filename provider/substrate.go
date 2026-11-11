//go:build computeprovider_substrate

// Substrate compute provider for Temporal Serverless Workers.
//
// Drop the substrate*.go files into
// temporal-auto-scaled-workers/wci/workflow/compute_provider/ and apply
// patches/0001-add-substrate-compute-provider-type.patch. Build with
// `-tags computeprovider_substrate`.
//
// Model: one WCI scaling group == one Substrate atespace == one pool of
// actors named worker-0 … worker-(pool_size-1), all created from the same
// ActorTemplate. The atespace is dedicated to one Worker Deployment Version:
// the provider owns every actor in it. The WCI's rate-based scaler asks for
// a worker-set size N; the provider keeps actors [0, N) running and polling,
// and drains and suspends actors [N, pool). Substrate does not suspend idle
// actors on its own, so scale-down has to come from here.
//
// The provider talks to the Worker inside each actor through workerprotocol:
// POST /start after resuming it, POST /drain before suspending it, both
// through atenet-router. The /start request carries everything the Worker
// needs to connect (frontend address, namespace, deployment version,
// identity), so this provider is the single source of those values.

package computeprovider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/common/dynamicconfig"

	"temporal-substrate-demo/workerprotocol"
)

// Customer-facing config keys (the provider "details" of a scaling group).
const (
	configSubstrateAtespace      = "atespace"       // required: atespace dedicated to this version's pool
	configSubstrateActorTemplate = "actor_template" // required: ActorTemplate for this build ID
	configSubstratePoolSize      = "pool_size"      // optional: actors in the pool; must be >= the scaler's max_count

	// The Temporal frontend. The provider sends it to the Worker in /start
	// and allows exactly this host:port in each actor's egress policy (actors
	// don't inherit one from their template), so the two can't diverge.
	configSubstrateTemporalHost          = "temporal_host"           // required
	configSubstrateTemporalPort          = "temporal_port"           // optional: default 7233
	configSubstrateTemporalTLS           = "temporal_tls"            // optional: "true" (default) = https rule, "false" = cleartext h2c
	configSubstrateTemporalCredentialURI = "temporal_credential_uri" // optional: inject Authorization: Bearer <credential> (needs TLS)

	defaultSubstratePoolSize = 30 // matches the rate-based scaler's default max_count
	maxSubstratePoolSize     = 1000
	substrateConcurrency     = 16 // parallel calls per UpdateWorkerSetSize
)

// knownConfigKeys: anything else is rejected, so a typo can't silently fall
// back to a default.
var knownConfigKeys = []string{
	configSubstrateAtespace, configSubstrateActorTemplate, configSubstratePoolSize,
	configSubstrateTemporalHost, configSubstrateTemporalPort, configSubstrateTemporalTLS,
	configSubstrateTemporalCredentialURI,
}

// actorControl is the subset of ateapi.ControlClient the provider uses.
// ateapi.ControlClient satisfies it; tests use a fake.
type actorControl interface {
	GetActorTemplate(ctx context.Context, in *ateapi.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapi.ActorTemplate, error)
	ListActors(ctx context.Context, in *ateapi.ListActorsRequest, opts ...grpc.CallOption) (*ateapi.ListActorsResponse, error)
	CreateActor(ctx context.Context, in *ateapi.CreateActorRequest, opts ...grpc.CallOption) (*ateapi.Actor, error)
	CreateActorEgressPolicy(ctx context.Context, in *ateapi.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapi.EgressPolicy, error)
	ResumeActor(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error)
	SuspendActor(ctx context.Context, in *ateapi.SuspendActorRequest, opts ...grpc.CallOption) (*ateapi.SuspendActorResponse, error)
	RevertActor(ctx context.Context, in *ateapi.RevertActorRequest, opts ...grpc.CallOption) (*ateapi.RevertActorResponse, error)
}

// workerLifecycle sends workerprotocol calls to the Worker inside an actor.
// The adapter in substrate_router.go tunnels them through atenet-router;
// tests use a fake.
type workerLifecycle interface {
	// Start makes the Worker connect to Temporal and poll. Idempotent.
	Start(ctx context.Context, actor *ateapi.ObjectRef, req workerprotocol.StartRequest) error
	// Drain stops polling, waits for in-flight tasks and disconnects. Idempotent.
	Drain(ctx context.Context, actor *ateapi.ObjectRef) error
}

type substrateComputeProvider struct {
	control actorControl
	workers workerLifecycle
}

type substrateConfig struct {
	atespace string
	template string
	poolSize int32

	temporalHost          string
	temporalPort          int32
	temporalTLS           bool
	temporalCredentialURI string
}

func init() {
	RegisterComputeProvider(iface.ComputeProviderTypeSubstrate, NewSubstrateComputeProvider)
}

// The WCI calls a provider's constructor on every activity, so the
// connections are created once per process and shared.
var (
	sharedMu      sync.Mutex
	sharedControl actorControl
	sharedWorkers workerLifecycle
)

// NewSubstrateComputeProvider returns a provider backed by the process-wide
// ate-api-server and atenet-router connections. Endpoints and credentials are
// operator-side settings (substrate_dial.go), never part of the tenant's
// config: a tenant-chosen endpoint would let a tenant point the server's
// Substrate identity at an arbitrary host.
func NewSubstrateComputeProvider(ctx context.Context, dc *dynamicconfig.Collection) (ComputeProvider, error) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedControl == nil {
		control, workers, err := dialSubstrate(ctx, dc)
		if err != nil { // not cached: the next activity retries
			return nil, NewProviderError(FailureInternal, errors.Join(errWCIOwned, err))
		}
		sharedControl, sharedWorkers = control, workers
	}
	return &substrateComputeProvider{control: sharedControl, workers: sharedWorkers}, nil
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

// UpdateWorkerSetSize converges the pool to exactly `size` polling actors.
// It is idempotent and keeps no state: the WCI retries this activity, so the
// desired state is derived from actor names alone and every call recomputes it.
func (p *substrateComputeProvider) UpdateWorkerSetSize(ctx context.Context, rc RequestContext, config ComputeProviderConfig, size int32) error {
	cfg, err := parseSubstrateConfig(rc, config)
	if err != nil {
		return NewProviderError(FailureRejected, err)
	}
	if size < 0 || size > cfg.poolSize {
		return NewProviderError(FailureRejected,
			fmt.Errorf("requested size %d outside pool [0, %d]; raise %q or lower the scaler's max_count", size, cfg.poolSize, configSubstratePoolSize))
	}

	existing, err := p.listPool(ctx, cfg)
	if err != nil {
		return classifySubstrateError(err)
	}

	// Lowest indices stay running, so scale-down always drains the
	// highest-numbered (most recently started) actors first.
	var tasks []func(context.Context) error
	for i := int32(0); i < cfg.poolSize; i++ {
		name := cfg.actorName(i)
		actor, exists := existing[name]
		state := actor.GetStatus().GetState()
		switch {
		case i < size:
			tasks = append(tasks, func(ctx context.Context) error { return p.ensurePolling(ctx, rc, cfg, name, exists, state) })
		case exists && isRunningOrResuming(state):
			tasks = append(tasks, func(ctx context.Context) error { return p.drainAndSuspend(ctx, cfg, name) })
		}
	}
	return classifySubstrateError(runBounded(ctx, substrateConcurrency, tasks))
}

// ensurePolling brings one actor to RUNNING and tells its Worker to poll.
// /start is sent even to actors that are already running: it is idempotent,
// and it repairs an earlier attempt that resumed the actor but failed to start
// the Worker.
func (p *substrateComputeProvider) ensurePolling(ctx context.Context, rc RequestContext, cfg substrateConfig, name string, exists bool, state ateapi.ActorState) error {
	if !exists {
		_, err := p.control.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: cfg.atespace, Name: name},
			ActorTemplate: &ateapi.ObjectRef{Atespace: cfg.atespace, Name: cfg.template},
		}})
		// AlreadyExists: a previous attempt of this activity created it.
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create actor %s: %w", name, err)
		}
	}
	if !isRunningOrResuming(state) {
		// Before the first resume: the egress gateway caches a missing policy
		// as a deny for ~10s, so a Worker resumed first would fail to dial.
		if err := p.ensureEgressPolicy(ctx, cfg, name); err != nil {
			return err
		}
		if state == ateapi.ActorState_ACTOR_STATE_CRASHED {
			// Back to the last durable snapshot. Any task the crashed Worker
			// held times out on the Temporal side and is retried.
			if _, err := p.control.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: cfg.ref(name)}); err != nil {
				return fmt.Errorf("revert actor %s: %w", name, err)
			}
		}
		// ResumeActor waits for the template's wakeupProbe, so the Worker's
		// HTTP server is up when it returns.
		if _, err := p.control.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: cfg.ref(name)}); err != nil {
			return fmt.Errorf("resume actor %s: %w", name, err)
		}
	}
	req := cfg.startRequest(rc, name)
	err := p.startWorker(ctx, cfg, name, req)
	if errors.Is(err, workerprotocol.ErrConflict) {
		// Polling with an older request (the config changed): reconnect.
		drainCtx, cancel := context.WithTimeout(ctx, workerprotocol.DrainTimeout)
		err = p.workers.Drain(drainCtx, cfg.ref(name))
		cancel()
		if err == nil {
			err = p.startWorker(ctx, cfg, name, req)
		}
	}
	if err != nil {
		return fmt.Errorf("start worker in actor %s: %w", name, err)
	}
	return nil
}

func (p *substrateComputeProvider) startWorker(ctx context.Context, cfg substrateConfig, name string, req workerprotocol.StartRequest) error {
	ctx, cancel := context.WithTimeout(ctx, workerprotocol.StartTimeout)
	defer cancel()
	return p.workers.Start(ctx, cfg.ref(name), req)
}

func (p *substrateComputeProvider) ensureEgressPolicy(ctx context.Context, cfg substrateConfig, name string) error {
	ports := &ateapi.Ports{Numbers: []int32{cfg.temporalPort}}
	rule := &ateapi.EgressRule{}
	if cfg.temporalTLS {
		rule.Https = &ateapi.HttpsRule{Hostnames: []string{cfg.temporalHost}, Ports: ports}
		if cfg.temporalCredentialURI != "" {
			rule.Https.Effects = &ateapi.HttpRuleEffects{ReplaceHeaders: []*ateapi.CredentialHeader{{
				Header:        "Authorization",
				Prefix:        "Bearer ",
				CredentialUri: cfg.temporalCredentialURI,
			}}}
		}
	} else {
		rule.Http = &ateapi.HttpRule{Hostnames: []string{cfg.temporalHost}, Ports: ports}
	}
	_, err := p.control.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{
		Actor: cfg.ref(name),
		EgressPolicy: &ateapi.EgressPolicy{
			Metadata: &ateapi.ResourceMetadata{Atespace: cfg.atespace, Name: "default"},
			Rules:    []*ateapi.EgressRule{rule},
		},
	})
	// TODO: on AlreadyExists, compare and update the policy if the config changed.
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create egress policy for actor %s: %w", name, err)
	}
	return nil
}

func (p *substrateComputeProvider) drainAndSuspend(ctx context.Context, cfg substrateConfig, name string) error {
	// Best effort: if the drain fails or times out, suspend anyway. A task the
	// Worker still held is not lost: Temporal times it out and retries it on
	// another Worker.
	drainCtx, cancel := context.WithTimeout(ctx, workerprotocol.DrainTimeout)
	_ = p.workers.Drain(drainCtx, cfg.ref(name))
	cancel()

	_, err := p.control.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   cfg.ref(name),
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("suspend actor %s: %w", name, err)
	}
	return nil
}

func (p *substrateComputeProvider) listPool(ctx context.Context, cfg substrateConfig) (map[string]*ateapi.Actor, error) {
	// The atespace is dedicated to this pool, so every actor in it is ours.
	// Only the names worker-<i> below pool_size are ever acted on.
	pool := map[string]*ateapi.Actor{}
	token := ""
	for {
		resp, err := p.control.ListActors(ctx, &ateapi.ListActorsRequest{
			Atespace: cfg.atespace,
			Options:  &ateapi.ListOptions{PageSize: 1000, PageToken: token},
		})
		if err != nil {
			return nil, fmt.Errorf("list actors in %s: %w", cfg.atespace, err)
		}
		for _, a := range resp.GetActors() {
			pool[a.GetMetadata().GetName()] = a
		}
		if token = resp.GetNextPageToken(); token == "" {
			return pool, nil
		}
	}
}

func isRunningOrResuming(s ateapi.ActorState) bool {
	return s == ateapi.ActorState_ACTOR_STATE_RUNNING || s == ateapi.ActorState_ACTOR_STATE_RESUMING
}

// --- config -----------------------------------------------------------------

func parseSubstrateConfig(rc RequestContext, config ComputeProviderConfig) (substrateConfig, error) {
	cfg := substrateConfig{poolSize: defaultSubstratePoolSize, temporalPort: 7233, temporalTLS: true}

	for k := range config {
		if !slices.Contains(knownConfigKeys, k) {
			return cfg, fmt.Errorf("unknown config key %q (known: %v)", k, knownConfigKeys)
		}
	}
	var ok bool
	if cfg.atespace, ok = config[configSubstrateAtespace].(string); !ok || cfg.atespace == "" {
		return cfg, fmt.Errorf("%q not found in config", configSubstrateAtespace)
	}
	if cfg.template, ok = config[configSubstrateActorTemplate].(string); !ok || cfg.template == "" {
		return cfg, fmt.Errorf("%q not found in config", configSubstrateActorTemplate)
	}
	if cfg.temporalHost, ok = config[configSubstrateTemporalHost].(string); !ok || cfg.temporalHost == "" {
		return cfg, fmt.Errorf("%q not found in config", configSubstrateTemporalHost)
	}
	if raw, present := config[configSubstratePoolSize]; present {
		// Configs arrive as JSON, so numbers may be strings (as in the other
		// providers) or float64.
		n, err := toInt32(raw)
		if err != nil || n < 1 || n > maxSubstratePoolSize {
			return cfg, fmt.Errorf("%q must be an integer in [1, %d]", configSubstratePoolSize, maxSubstratePoolSize)
		}
		cfg.poolSize = n
	}
	if raw, present := config[configSubstrateTemporalPort]; present {
		n, err := toInt32(raw)
		if err != nil || n < 1 || n > 65535 {
			return cfg, fmt.Errorf("%q must be a port number", configSubstrateTemporalPort)
		}
		cfg.temporalPort = n
	}
	if raw, present := config[configSubstrateTemporalTLS]; present {
		b, err := toBool(raw)
		if err != nil {
			return cfg, fmt.Errorf("%q must be true or false", configSubstrateTemporalTLS)
		}
		cfg.temporalTLS = b
	}
	cfg.temporalCredentialURI, _ = config[configSubstrateTemporalCredentialURI].(string)
	if cfg.temporalCredentialURI != "" && !cfg.temporalTLS {
		return cfg, fmt.Errorf("%q needs %q: the gateway injects credentials only into HTTPS it terminates", configSubstrateTemporalCredentialURI, configSubstrateTemporalTLS)
	}
	// The version comes from the WCI; the Worker gets it in /start.
	if err := cfg.startRequest(rc, cfg.actorName(0)).Validate(); err != nil {
		return cfg, fmt.Errorf("cannot build a valid /start request: %w", err)
	}
	return cfg, nil
}

func (c substrateConfig) actorName(i int32) string {
	return "worker-" + strconv.Itoa(int(i))
}

// startRequest is everything the Worker in actor `name` needs to connect: the
// frontend from this config, the version from the Worker Controller.
func (c substrateConfig) startRequest(rc RequestContext, name string) workerprotocol.StartRequest {
	return workerprotocol.StartRequest{
		Identity:       name,
		Address:        net.JoinHostPort(c.temporalHost, strconv.Itoa(int(c.temporalPort))),
		TLS:            c.temporalTLS,
		Namespace:      rc.NamespaceName,
		DeploymentName: rc.DeploymentName,
		BuildID:        rc.DeploymentBuildID,
	}
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

func toBool(v any) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		return strconv.ParseBool(b)
	}
	return false, fmt.Errorf("unsupported type %T", v)
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
