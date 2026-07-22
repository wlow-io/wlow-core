package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wlow/wlow/pkg/artifact"
)

// ExecuteRequest contains all the information needed to execute a task in a sandbox.
type ExecuteRequest struct {
	Manifest     *artifact.Manifest
	Bytes        []byte
	ArtifactPath string
	Snapshot     *SnapshotRootfs
	Input        map[string]any
}

// SnapshotRootfs describes the local file system state for a microVM snapshot.
type SnapshotRootfs struct {
	Dir        string
	RootfsPath string
}

// ExecuteResult contains the output of a sandboxed execution.
type ExecuteResult struct {
	Output map[string]any
}

// Executor is the interface for sandboxed task execution environments.
type Executor interface {
	Runtime() artifact.Runtime
	Execute(ctx context.Context, req ExecuteRequest) (*ExecuteResult, error)
}

// ExecutorRegistry manages available task executors.
type ExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[artifact.Runtime]Executor
}

// NewExecutorRegistry creates a new registry with the given executors.
func NewExecutorRegistry(executors ...Executor) (*ExecutorRegistry, error) {
	reg := &ExecutorRegistry{executors: make(map[artifact.Runtime]Executor)}
	for _, executor := range executors {
		if err := reg.Register(executor); err != nil {
			return nil, err
		}
	}
	return reg, nil
}

// DefaultExecutorRegistry creates a registry with all standard executors.
func DefaultExecutorRegistry() (*ExecutorRegistry, error) {
	return NewExecutorRegistry(
		newWasmExecutor(DefaultPolicy()),
		NewProcessExecutor(),
		NewMicroVMExecutor(""),
		NewSnapshotExecutor(""),
	)
}

// RegistryFor builds a registry containing only the executors needed for the
// given runtimes. A runner started with --runtimes=linux-process therefore
// never imports microvm wiring at runtime, and a runner without /dev/kvm can
// safely advertise just `process` and `wasm`.
func RegistryFor(dataDir string, runtimes ...artifact.Runtime) (*ExecutorRegistry, error) {
	if len(runtimes) == 0 {
		return nil, errors.New("at least one runtime required")
	}
	executors := make([]Executor, 0, len(runtimes))
	for idx := 0; idx < len(runtimes); idx++ {
		runtime := runtimes[idx]
		switch runtime {
		case artifact.RuntimeWasm:
			executors = append(executors, newWasmExecutor(DefaultPolicy()))
		case artifact.RuntimeProcess:
			executors = append(executors, NewProcessExecutor())
		case artifact.RuntimeMicroVM:
			executors = append(executors, NewMicroVMExecutor(dataDir))
		case artifact.RuntimeSnapshot:
			executors = append(executors, NewSnapshotExecutor(dataDir))
		default:
			return nil, fmt.Errorf("unknown runtime: %s", runtime)
		}
	}
	return NewExecutorRegistry(executors...)
}

// Register adds a new executor to the registry.
func (r *ExecutorRegistry) Register(executor Executor) error {
	if r == nil {
		return errors.New("executor registry required")
	}
	if executor == nil {
		return errors.New("executor required")
	}
	runtime := executor.Runtime()
	if runtime == "" {
		return errors.New("executor runtime required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executors[runtime] = executor
	return nil
}

// Get retrieves an executor for the given runtime.
func (r *ExecutorRegistry) Get(runtime artifact.Runtime) (Executor, error) {
	if r == nil {
		return nil, errors.New("executor registry required")
	}
	if runtime == "" {
		runtime = artifact.RuntimeWasm
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[runtime]
	if !ok {
		return nil, fmt.Errorf("executor not found for runtime: %s", runtime)
	}
	return executor, nil
}
