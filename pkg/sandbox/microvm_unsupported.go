package sandbox

import (
	"context"
	"errors"

	"github.com/wlow/wlow/pkg/artifact"
)

type unsupportedMicroVMExecutor struct {
	runtime artifact.Runtime
}

// NewMicroVMExecutor returns a stub executor for microVM runtimes on unsupported platforms.
func NewMicroVMExecutor(_ string) Executor {
	return unsupportedMicroVMExecutor{runtime: artifact.RuntimeMicroVM}
}

// NewSnapshotExecutor returns a stub executor for snapshot runtimes on unsupported platforms.
func NewSnapshotExecutor(_ string) Executor {
	return unsupportedMicroVMExecutor{runtime: artifact.RuntimeSnapshot}
}

func (e unsupportedMicroVMExecutor) Runtime() artifact.Runtime {
	return e.runtime
}

func (e unsupportedMicroVMExecutor) Execute(context.Context, ExecuteRequest) (*ExecuteResult, error) {
	if e.runtime == "" {
		return nil, errors.New("microvm runtime required")
	}
	return nil, errors.New("go microvm executor removed; use runner-rs wlow-runner")
}
