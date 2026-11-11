//go:build computeprovider_substrate

package computeprovider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"temporal-substrate-demo/workerprotocol"
)

// fake is an in-memory stand-in for ate-api-server and for the Workers behind
// atenet-router. It records every call in order.
type fake struct {
	mu        sync.Mutex
	actors    map[string]ateapi.ActorState
	policies  map[string]*ateapi.EgressPolicy
	polling   map[string]*workerprotocol.StartRequest
	templates map[string]bool
	calls     []string
	failOn    map[string]error // "op:name" -> error
}

func newFake() *fake {
	return &fake{
		actors:    map[string]ateapi.ActorState{},
		policies:  map[string]*ateapi.EgressPolicy{},
		polling:   map[string]*workerprotocol.StartRequest{},
		templates: map[string]bool{"hello-v1/hello-worker": true},
		failOn:    map[string]error{},
	}
}

func (f *fake) record(op, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, op+":"+name)
	return f.failOn[op+":"+name]
}

func (f *fake) callsFor(name string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if op, n, _ := strings.Cut(c, ":"); n == name {
			out = append(out, op)
		}
	}
	return out
}

func (f *fake) count(want ateapi.ActorState) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.actors {
		if s == want {
			n++
		}
	}
	return n
}

func (f *fake) GetActorTemplate(_ context.Context, in *ateapi.GetActorTemplateRequest, _ ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	ref := in.GetActorTemplate()
	if !f.templates[ref.GetAtespace()+"/"+ref.GetName()] {
		return nil, status.Error(codes.NotFound, "no such template")
	}
	return &ateapi.ActorTemplate{}, nil
}

func (f *fake) ListActors(_ context.Context, in *ateapi.ListActorsRequest, _ ...grpc.CallOption) (*ateapi.ListActorsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &ateapi.ListActorsResponse{}
	for name, s := range f.actors {
		resp.Actors = append(resp.Actors, &ateapi.Actor{
			Metadata: &ateapi.ResourceMetadata{Atespace: in.GetAtespace(), Name: name},
			Status:   &ateapi.ActorStatus{State: s},
		})
	}
	return resp, nil
}

func (f *fake) CreateActor(_ context.Context, in *ateapi.CreateActorRequest, _ ...grpc.CallOption) (*ateapi.Actor, error) {
	name := in.GetActor().GetMetadata().GetName()
	if err := f.record("create", name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.actors[name]; ok {
		return nil, status.Error(codes.AlreadyExists, name)
	}
	f.actors[name] = ateapi.ActorState_ACTOR_STATE_SUSPENDED // seeded from the golden snapshot
	return in.GetActor(), nil
}

func (f *fake) CreateActorEgressPolicy(_ context.Context, in *ateapi.CreateActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapi.EgressPolicy, error) {
	name := in.GetActor().GetName()
	if err := f.record("policy", name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.policies[name]; ok {
		return nil, status.Error(codes.AlreadyExists, name)
	}
	f.policies[name] = in.GetEgressPolicy()
	return in.GetEgressPolicy(), nil
}

func (f *fake) ResumeActor(_ context.Context, in *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
	name := in.GetActor().GetName()
	if err := f.record("resume", name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actors[name] = ateapi.ActorState_ACTOR_STATE_RUNNING
	return &ateapi.ResumeActorResponse{}, nil
}

func (f *fake) SuspendActor(_ context.Context, in *ateapi.SuspendActorRequest, _ ...grpc.CallOption) (*ateapi.SuspendActorResponse, error) {
	name := in.GetActor().GetName()
	if err := f.record("suspend", name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.polling[name] != nil {
		return nil, fmt.Errorf("test: %s suspended while still polling", name)
	}
	f.actors[name] = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	return &ateapi.SuspendActorResponse{}, nil
}

func (f *fake) RevertActor(_ context.Context, in *ateapi.RevertActorRequest, _ ...grpc.CallOption) (*ateapi.RevertActorResponse, error) {
	name := in.GetActor().GetName()
	if err := f.record("revert", name); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actors[name] = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	return &ateapi.RevertActorResponse{}, nil
}

// workerLifecycle side of the fake: it behaves like actor/lifecycle
// (idempotent start, ErrConflict on a different request).
func (f *fake) Start(_ context.Context, actor *ateapi.ObjectRef, req workerprotocol.StartRequest) error {
	name := actor.GetName()
	if err := f.record("start", name); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actors[name] != ateapi.ActorState_ACTOR_STATE_RUNNING {
		return fmt.Errorf("test: /start sent to %s in state %v", name, f.actors[name])
	}
	if cur := f.polling[name]; cur != nil && *cur != req {
		return workerprotocol.ErrConflict
	}
	f.polling[name] = &req
	return nil
}

func (f *fake) Drain(_ context.Context, actor *ateapi.ObjectRef) error {
	name := actor.GetName()
	if err := f.record("drain", name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polling[name] = nil
	return nil
}

func newTestProvider(f *fake) *substrateComputeProvider {
	return &substrateComputeProvider{control: f, workers: f}
}

var (
	testRC  = RequestContext{NamespaceName: "default", DeploymentName: "hello", DeploymentBuildID: "v1"}
	testCfg = ComputeProviderConfig{
		"atespace":       "hello-v1",
		"actor_template": "hello-worker",
		"pool_size":      "5",
		"temporal_host":  "temporal-frontend.temporal.svc.cluster.local",
		"temporal_tls":   "false",
	}
)

func TestScaleUpThenDownThenToZero(t *testing.T) {
	f := newFake()
	p := newTestProvider(f)
	ctx := context.Background()

	for _, size := range []int32{3, 1, 0} {
		if err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, size); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if got := f.count(ateapi.ActorState_ACTOR_STATE_RUNNING); got != int(size) {
			t.Fatalf("size %d: %d actors running", size, got)
		}
	}
	if _, ok := f.actors["worker-3"]; ok {
		t.Fatal("actors beyond the requested size must not be created")
	}
	// Scale-down keeps the lowest indices: worker-0 ran until size 0.
	if got := f.callsFor("worker-0"); !slices.Equal(got, []string{"create", "policy", "resume", "start", "start", "drain", "suspend"}) {
		t.Fatalf("worker-0 lifecycle: %v", got)
	}
}

func TestStartRequestCarriesVersionAndFrontend(t *testing.T) {
	f := newFake()
	if err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 1); err != nil {
		t.Fatal(err)
	}
	want := workerprotocol.StartRequest{
		Identity:       "worker-0",
		Address:        "temporal-frontend.temporal.svc.cluster.local:7233",
		TLS:            false,
		Namespace:      "default",
		DeploymentName: "hello",
		BuildID:        "v1",
	}
	if got := f.polling["worker-0"]; got == nil || *got != want {
		t.Fatalf("/start request %+v, want %+v", got, want)
	}
	// The egress policy allows exactly the address the Worker was told to dial.
	h := f.policies["worker-0"].GetRules()[0].Http
	if !slices.Equal(h.Hostnames, []string{"temporal-frontend.temporal.svc.cluster.local"}) || !slices.Equal(h.Ports.Numbers, []int32{7233}) {
		t.Fatalf("egress rule %+v does not match the /start address", h)
	}
}

func TestFirstStartOrder(t *testing.T) {
	f := newFake()
	if err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 1); err != nil {
		t.Fatal(err)
	}
	// The policy must exist before the first resume, or the Worker's first
	// dial is denied (and the deny is cached).
	if got := f.callsFor("worker-0"); !slices.Equal(got, []string{"create", "policy", "resume", "start"}) {
		t.Fatalf("calls: %v", got)
	}
}

func TestTLSPolicyWithCredential(t *testing.T) {
	f := newFake()
	cfg := ComputeProviderConfig{
		"atespace": "hello-v1", "actor_template": "hello-worker",
		"temporal_host":           "ns.acct.tmprl.cloud",
		"temporal_credential_uri": "ate-secret://k8s.io/default/temporal/api-key/key",
	}
	if err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, cfg, 1); err != nil {
		t.Fatal(err)
	}
	r := f.policies["worker-0"].GetRules()[0]
	if r.Https == nil || r.Http != nil {
		t.Fatalf("TLS is the default and must produce an https rule: %+v", r)
	}
	if h := r.Https.Effects.ReplaceHeaders[0]; h.Header != "Authorization" || h.Prefix != "Bearer " {
		t.Fatalf("credential header: %+v", h)
	}
	if !f.polling["worker-0"].TLS {
		t.Fatal("the Worker must be told to dial with TLS")
	}
}

func TestConfigChangeReconnectsPollingWorker(t *testing.T) {
	f := newFake()
	p := newTestProvider(f)
	ctx := context.Background()
	if err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, 1); err != nil {
		t.Fatal(err)
	}
	moved := ComputeProviderConfig{}
	for k, v := range testCfg {
		moved[k] = v
	}
	moved["temporal_port"] = "7234"
	f.calls = nil
	if err := p.UpdateWorkerSetSize(ctx, testRC, moved, 1); err != nil {
		t.Fatal(err)
	}
	if got := f.callsFor("worker-0"); !slices.Equal(got, []string{"start", "drain", "start"}) {
		t.Fatalf("a changed /start request must drain and reconnect, calls: %v", got)
	}
	if f.polling["worker-0"].Address != "temporal-frontend.temporal.svc.cluster.local:7234" {
		t.Fatalf("worker polls with %+v", f.polling["worker-0"])
	}
}

func TestRetryIsIdempotent(t *testing.T) {
	f := newFake()
	p := newTestProvider(f)
	ctx := context.Background()

	// First attempt: worker-1 is created and resumed, but can't reach Temporal yet.
	f.failOn["start:worker-1"] = errors.New("worker answered 502")
	if err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, 2); err == nil {
		t.Fatal("want an error from the failed /start")
	}

	// The WCI retries the activity: worker-1 is only started again.
	delete(f.failOn, "start:worker-1")
	f.calls = nil
	if err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, 2); err != nil {
		t.Fatal(err)
	}
	if got := f.callsFor("worker-1"); !slices.Equal(got, []string{"start"}) {
		t.Fatalf("retry should only re-send /start, calls: %v", got)
	}
	if f.polling["worker-0"] == nil || f.polling["worker-1"] == nil {
		t.Fatal("both Workers should be polling")
	}
}

func TestPolicyFailureBlocksResume(t *testing.T) {
	f := newFake()
	f.failOn["policy:worker-0"] = status.Error(codes.Unavailable, "ate-api-server down")
	err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 1)
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.Class != FailureUnavailable {
		t.Fatalf("want FailureUnavailable, got %v", err)
	}
	if got := f.callsFor("worker-0"); !slices.Equal(got, []string{"create", "policy"}) {
		t.Fatalf("must not resume without a policy, calls: %v", got)
	}
}

func TestCrashedActorIsRevertedBeforeResume(t *testing.T) {
	f := newFake()
	f.actors["worker-0"] = ateapi.ActorState_ACTOR_STATE_CRASHED
	f.policies["worker-0"] = &ateapi.EgressPolicy{}

	if err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 1); err != nil {
		t.Fatal(err)
	}
	if got := f.callsFor("worker-0"); !slices.Equal(got, []string{"policy", "revert", "resume", "start"}) {
		t.Fatalf("calls: %v", got)
	}
}

func TestFailedDrainStillSuspends(t *testing.T) {
	f := newFake()
	f.actors["worker-0"] = ateapi.ActorState_ACTOR_STATE_RUNNING
	f.failOn["drain:worker-0"] = errors.New("router: actor unreachable")

	if err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 0); err != nil {
		t.Fatal(err)
	}
	if f.actors["worker-0"] != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatal("a failed drain must not keep the actor running")
	}
}

func TestOnlyActsOnPoolNames(t *testing.T) {
	// The atespace is dedicated, but stray actors still must not be touched.
	f := newFake()
	f.actors["debug-shell"] = ateapi.ActorState_ACTOR_STATE_RUNNING
	f.actors["worker-9"] = ateapi.ActorState_ACTOR_STATE_RUNNING // beyond pool_size 5
	if err := newTestProvider(f).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 0); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("calls: %v", f.calls)
	}
}

func TestRejectsSizeAbovePool(t *testing.T) {
	err := newTestProvider(newFake()).UpdateWorkerSetSize(context.Background(), testRC, testCfg, 6)
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.Class != FailureRejected {
		t.Fatalf("want FailureRejected, got %v", err)
	}
}

func TestValidateConfig(t *testing.T) {
	p := newTestProvider(newFake())
	ctx := context.Background()

	if err := p.ValidateConfig(ctx, testRC, testCfg); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	missingTemplate := ComputeProviderConfig{"atespace": "hello-v1", "actor_template": "nope", "temporal_host": "t"}
	var pe *ProviderError
	if err := p.ValidateConfig(ctx, testRC, missingTemplate); !errors.As(err, &pe) || pe.Class != FailureNotFound {
		t.Fatalf("want FailureNotFound, got %v", err)
	}

	base := func(k string, v any) ComputeProviderConfig {
		c := ComputeProviderConfig{"atespace": "hello-v1", "actor_template": "hello-worker", "temporal_host": "t"}
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
		return c
	}
	for name, cfg := range map[string]ComputeProviderConfig{
		"no atespace":         base("atespace", nil),
		"no temporal_host":    base("temporal_host", nil),
		"bad pool":            base("pool_size", "0"),
		"float pool":          base("pool_size", 2.5),
		"bad port":            base("temporal_port", "70000"),
		"bad tls":             base("temporal_tls", "maybe"),
		"typo key":            base("pool_sise", "3"),
		"removed name_prefix": base("name_prefix", "hello-v1"),
		"credential, no TLS":  {"atespace": "hello-v1", "actor_template": "hello-worker", "temporal_host": "t", "temporal_tls": "false", "temporal_credential_uri": "ate-secret://k8s.io/a/b/c/d"},
	} {
		if err := p.ValidateConfig(ctx, testRC, cfg); !errors.As(err, &pe) || pe.Class != FailureRejected {
			t.Errorf("%s: want FailureRejected, got %v", name, err)
		}
	}

	// The version comes from the WCI; without it no valid /start is possible.
	if err := p.ValidateConfig(ctx, RequestContext{NamespaceName: "default"}, testCfg); !errors.As(err, &pe) || pe.Class != FailureRejected {
		t.Fatalf("missing deployment version: want FailureRejected, got %v", err)
	}
}

func TestConstructorSharesConnections(t *testing.T) {
	sharedMu.Lock()
	sharedControl, sharedWorkers = nil, nil
	sharedMu.Unlock()
	t.Setenv(envSubstrateCAFile, "")
	if _, err := NewSubstrateComputeProvider(context.Background(), nil); err == nil {
		t.Fatal("want an error without credentials")
	}
	if sharedControl != nil {
		t.Fatal("a failed dial must not be cached")
	}

	f := newFake()
	sharedMu.Lock()
	sharedControl, sharedWorkers = f, f
	sharedMu.Unlock()
	t.Cleanup(func() { sharedControl, sharedWorkers = nil, nil })
	a, _ := NewSubstrateComputeProvider(context.Background(), nil)
	b, _ := NewSubstrateComputeProvider(context.Background(), nil)
	if a.(*substrateComputeProvider).control != b.(*substrateComputeProvider).control {
		t.Fatal("each activity must reuse the process-wide connection")
	}
}
