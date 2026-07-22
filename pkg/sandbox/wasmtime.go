//go:build !no_wasm

package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v43"
	"github.com/wlow/wlow/pkg/artifact"
)

// WasmExecutor executes tasks in a WebAssembly sandbox using Wasmtime.
type WasmExecutor struct {
	engine *wasmtime.Engine
	policy Policy
}

// NewWasmExecutor creates a new WebAssembly executor with the given policy.
func NewWasmExecutor(policy Policy) *WasmExecutor {
	cfg := wasmtime.NewConfig()
	cfg.SetConsumeFuel(true)
	cfg.SetEpochInterruption(true)
	return &WasmExecutor{
		engine: wasmtime.NewEngineWithConfig(cfg),
		policy: policy,
	}
}

// Runtime returns the WebAssembly runtime identifier.
func (e *WasmExecutor) Runtime() artifact.Runtime {
	return artifact.RuntimeWasm
}

// Execute runs the WebAssembly module and returns the results.
func (e *WasmExecutor) Execute(ctx context.Context, req ExecuteRequest) (*ExecuteResult, error) {
	if e == nil || e.engine == nil {
		return nil, errors.New("wasm executor required")
	}
	if len(req.Bytes) == 0 {
		return nil, errors.New("wasm bytes required")
	}
	if req.Manifest == nil {
		return nil, errors.New("manifest required")
	}
	if req.Manifest.IOProtocolValue() != artifact.IOProtocolExternRefJSON {
		return e.executeComponent(ctx, req)
	}
	if err := e.policy.Authorize(req.Manifest); err != nil {
		return nil, err
	}
	store := wasmtime.NewStore(e.engine)
	applyLimits(store, req.Manifest.ResourceHints)
	module, err := wasmtime.NewModule(e.engine, req.Bytes)
	if err != nil {
		return nil, err
	}
	instance, err := wasmtime.NewInstance(store, module, []wasmtime.AsExtern{})
	if err != nil {
		return nil, err
	}
	return callProcess(ctx, e.engine, store, instance, req)
}

func (e *WasmExecutor) executeComponent(_ context.Context, req ExecuteRequest) (*ExecuteResult, error) {
	if req.Manifest.IOProtocolValue() != artifact.IOProtocolComponentWlowCore {
		return nil, errors.New("unsupported wasm io protocol: " + string(req.Manifest.IOProtocolValue()))
	}
	if req.Manifest.WITWorld == "" {
		return nil, errors.New("wasm component wit_world required")
	}
	if err := e.policy.Authorize(req.Manifest); err != nil {
		return nil, err
	}
	return nil, errors.New("wasip2 component execution requires a component-model host shim")
}

func applyLimits(store *wasmtime.Store, hints artifact.ResourceHints) {
	if hints.MemoryBytes > 0 {
		store.Limiter(hints.MemoryBytes, -1, -1, -1, -1)
	}
	fuel := hints.Fuel
	if fuel == 0 {
		fuel = 10_000_000
	}
	_ = store.SetFuel(fuel)
	store.SetEpochDeadline(1)
}

func callProcess(ctx context.Context, engine *wasmtime.Engine, store *wasmtime.Store, instance *wasmtime.Instance, req ExecuteRequest) (*ExecuteResult, error) {
	process := instance.GetFunc(store, entrypoint(req.Manifest))
	if process == nil {
		return nil, errors.New("wasm export not found: " + entrypoint(req.Manifest))
	}
	stop := interruptAfter(ctx, engine, req.Manifest.ResourceHints.Timeout)
	defer stop()
	input, err := json.Marshal(req.Input)
	if err != nil {
		return nil, err
	}
	out, err := process.Call(store, string(input))
	if err != nil {
		return nil, err
	}
	return decodeOutput(out)
}

func decodeOutput(out any) (*ExecuteResult, error) {
	if out == nil {
		return &ExecuteResult{Output: map[string]any{}}, nil
	}
	text, ok := out.(string)
	if !ok {
		return nil, errors.New("wasm process must return json string")
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return nil, err
	}
	return &ExecuteResult{Output: decoded}, nil
}

func interruptAfter(ctx context.Context, engine *wasmtime.Engine, timeout time.Duration) func() {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			engine.IncrementEpoch()
		case <-time.After(timeout):
			engine.IncrementEpoch()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func entrypoint(m *artifact.Manifest) string {
	if m.Entrypoint != "" {
		return m.Entrypoint
	}
	return "process"
}
