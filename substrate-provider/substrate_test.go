//go:build computeprovider_substrate

package computeprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeControl is an in-memory stand-in for ate-api-server.
type fakeControl struct {
	mu        sync.Mutex
	actors    map[string]ateapi.ActorState
	templates map[string]bool
	calls     []string
	failOn    map[string]error // "op:name" -> error
}

func newFake() *fakeControl {
	return &fakeControl{
		actors:    map[string]ateapi.ActorState{},
		templates: map[string]bool{"workers/worker-v1": true},
		failOn:    map[string]error{},
	}
}

func (f *fakeControl) record(op, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, op+":"+name)
	return f.failOn[op+":"+name]
}

func (f *fakeControl) set(name string, s ateapi.ActorState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actors[name] = s
}

func (f *fakeControl) running() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.actors {
		if s == ateapi.ActorState_ACTOR_STATE_RUNNING {
			n++
		}
	}
	return n
}

func (f *fakeControl) GetActorTemplate(_ context.Context, in *ateapi.GetActorTemplateRequest, _ ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	ref := in.GetActorTemplate()
	if !f.templates[ref.GetAtespace()+"/"+ref.GetName()] {
		return nil, status.Error(codes.NotFound, "no such template")
	}
	return &ateapi.ActorTemplate{}, nil
}

func (f *fakeControl) ListActors(_ context.Context, in *ateapi.ListActorsRequest, _ ...grpc.CallOption) (*ateapi.ListActorsResponse, error) {
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

func (f *fakeControl) CreateActor(_ context.Context, in *ateapi.CreateActorRequest, _ ...grpc.CallOption) (*ateapi.Actor, error) {
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

func (f *fakeControl) ResumeActor(_ context.Context, in *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
	name := in.GetActor().GetName()
	if err := f.record("resume", name); err != nil {
		return nil, err
	}
	f.set(name, ateapi.ActorState_ACTOR_STATE_RUNNING)
	return &ateapi.ResumeActorResponse{}, nil
}

func (f *fakeControl) SuspendActor(_ context.Context, in *ateapi.SuspendActorRequest, _ ...grpc.CallOption) (*ateapi.SuspendActorResponse, error) {
	name := in.GetActor().GetName()
	if err := f.record("suspend", name); err != nil {
		return nil, err
	}
	f.set(name, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	return &ateapi.SuspendActorResponse{}, nil
}

func (f *fakeControl) RevertActor(_ context.Context, in *ateapi.RevertActorRequest, _ ...grpc.CallOption) (*ateapi.RevertActorResponse, error) {
	name := in.GetActor().GetName()
	if err := f.record("revert", name); err != nil {
		return nil, err
	}
	f.set(name, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	return &ateapi.RevertActorResponse{}, nil
}

var (
	testRC  = RequestContext{NamespaceName: "default", DeploymentName: "orders", DeploymentBuildID: "v1"}
	testCfg = ComputeProviderConfig{
		"atespace":       "workers",
		"actor_template": "worker-v1",
		"pool_size":      "5",
		"name_prefix":    "orders-v1",
	}
)

func TestScaleUpThenDownThenToZero(t *testing.T) {
	f := newFake()
	p := &substrateComputeProvider{control: f}
	ctx := context.Background()

	for _, size := range []int32{3, 1, 0} {
		if err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, size); err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if got := f.running(); got != int(size) {
			t.Fatalf("size %d: %d actors running", size, got)
		}
	}
	// Scale-down keeps the lowest indices: after 3 -> 1, only actor 0 ran.
	if s := f.actors["orders-v1-0"]; s != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("orders-v1-0 should be suspended at size 0, got %v", s)
	}
	if _, ok := f.actors["orders-v1-3"]; ok {
		t.Fatal("actors beyond the requested size must not be created")
	}
}

func TestRetryIsIdempotent(t *testing.T) {
	f := newFake()
	p := &substrateComputeProvider{control: f}
	ctx := context.Background()

	// First attempt: actor 1 is created but resuming it fails.
	f.failOn["resume:orders-v1-1"] = status.Error(codes.Unavailable, "worker pool saturated")
	err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, 2)
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.Class != FailureUnavailable {
		t.Fatalf("want FailureUnavailable, got %v", err)
	}

	// WCI retries the activity: it must converge without re-creating actors.
	delete(f.failOn, "resume:orders-v1-1")
	f.calls = nil
	if err := p.UpdateWorkerSetSize(ctx, testRC, testCfg, 2); err != nil {
		t.Fatal(err)
	}
	if got := f.running(); got != 2 {
		t.Fatalf("%d running, want 2", got)
	}
	if fmt.Sprint(f.calls) != "[resume:orders-v1-1]" {
		t.Fatalf("retry should only resume the missing actor, calls: %v", f.calls)
	}
}

func TestCrashedActorIsRevertedBeforeResume(t *testing.T) {
	f := newFake()
	f.actors["orders-v1-0"] = ateapi.ActorState_ACTOR_STATE_CRASHED
	p := &substrateComputeProvider{control: f}

	if err := p.UpdateWorkerSetSize(context.Background(), testRC, testCfg, 1); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(f.calls) != "[revert:orders-v1-0 resume:orders-v1-0]" {
		t.Fatalf("calls: %v", f.calls)
	}
}

func TestIgnoresOtherActorsInAtespace(t *testing.T) {
	f := newFake()
	f.actors["someone-elses-agent"] = ateapi.ActorState_ACTOR_STATE_RUNNING
	p := &substrateComputeProvider{control: f}

	if err := p.UpdateWorkerSetSize(context.Background(), testRC, testCfg, 0); err != nil {
		t.Fatal(err)
	}
	if f.actors["someone-elses-agent"] != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Fatal("scale-to-zero must not touch actors outside the pool")
	}
}

func TestRejectsSizeAbovePool(t *testing.T) {
	p := &substrateComputeProvider{control: newFake()}
	err := p.UpdateWorkerSetSize(context.Background(), testRC, testCfg, 6)
	var pe *ProviderError
	if !errors.As(err, &pe) || pe.Class != FailureRejected {
		t.Fatalf("want FailureRejected, got %v", err)
	}
}

func TestValidateConfig(t *testing.T) {
	p := &substrateComputeProvider{control: newFake()}
	ctx := context.Background()

	if err := p.ValidateConfig(ctx, testRC, testCfg); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	missingTemplate := ComputeProviderConfig{"atespace": "workers", "actor_template": "nope"}
	var pe *ProviderError
	if err := p.ValidateConfig(ctx, testRC, missingTemplate); !errors.As(err, &pe) || pe.Class != FailureNotFound {
		t.Fatalf("want FailureNotFound, got %v", err)
	}

	for name, cfg := range map[string]ComputeProviderConfig{
		"no atespace": {"actor_template": "worker-v1"},
		"bad pool":    {"atespace": "workers", "actor_template": "worker-v1", "pool_size": "0"},
		"bad prefix":  {"atespace": "workers", "actor_template": "worker-v1", "name_prefix": "Orders_V1"},
		"float pool":  {"atespace": "workers", "actor_template": "worker-v1", "pool_size": 2.5},
	} {
		if err := p.ValidateConfig(ctx, testRC, cfg); !errors.As(err, &pe) || pe.Class != FailureRejected {
			t.Errorf("%s: want FailureRejected, got %v", name, err)
		}
	}
}

func TestDerivedNamePrefixIsStableAndValid(t *testing.T) {
	rc := RequestContext{NamespaceName: "prod.acme", DeploymentName: "Orders/Service", DeploymentBuildID: "2026-11-10@abc"}
	a, b := derivedNamePrefix(rc), derivedNamePrefix(rc)
	if a != b || !isShortName(a) {
		t.Fatalf("prefix %q not stable or not a valid short name", a)
	}
	other := rc
	other.DeploymentBuildID = "2026-11-11@def"
	if derivedNamePrefix(other) == a {
		t.Fatal("different build IDs must get different pools")
	}
}
