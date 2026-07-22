package workflow

import (
	"context"
	"fmt"

	"github.com/wlow/wlow/pkg/artifact"
)

// Resolver maps a (tenant, processor, ref) tuple to the runtime that should
// execute it. Implementations typically look up the artifact manifest. Engine
// and result handler use this to publish tasks on per-runtime subjects so that
// runners can subscribe by capability.
type Resolver interface {
	ResolveRuntime(ctx context.Context, tenant, processorID, ref string) (artifact.Runtime, error)
}

// PlacementResolver is a Resolver that can also resolve placement.
type PlacementResolver interface {
	Resolver
	ResolvePlacement(ctx context.Context, tenant, processorID, ref string) (artifact.Runtime, []string, error)
}

// SandboxSubjectForRuntime is the canonical subject used for sandboxed task
// dispatch when a resolver is wired. It allows runners to subscribe to only
// the runtimes they support.
//
//	<processor-prefix>.sandbox.<runtime>.<processor_id>
func SandboxSubjectForRuntime(runtime artifact.Runtime, processorID string) string {
	return SandboxSubjectForRuntimeWithPrefix("PROCESSOR", runtime, processorID)
}

// SandboxSubjectForRuntimeWithPrefix returns a sandboxed task subject with a custom prefix.
func SandboxSubjectForRuntimeWithPrefix(prefix string, runtime artifact.Runtime, processorID string) string {
	if runtime == "" {
		runtime = artifact.RuntimeWasm
	}
	return fmt.Sprintf("%s.sandbox.%s.%s", subjectPrefix(prefix), runtime, processorID)
}

// SandboxSubjectForNode returns a sandboxed task subject for a specific node.
func SandboxSubjectForNode(nodeID string, runtime artifact.Runtime, processorID string) string {
	return SandboxSubjectForNodeWithPrefix("PROCESSOR", nodeID, runtime, processorID)
}

// SandboxSubjectForNodeWithPrefix returns a sandboxed task subject for a specific node with a custom prefix.
func SandboxSubjectForNodeWithPrefix(prefix string, nodeID string, runtime artifact.Runtime, processorID string) string {
	if runtime == "" {
		runtime = artifact.RuntimeWasm
	}
	return fmt.Sprintf("%s.node.%s.sandbox.%s.%s", subjectPrefix(prefix), nodeID, runtime, processorID)
}

// RouteTask returns the subject to publish a task on. If a resolver is
// supplied and the task is sandboxed with a processor id, route by runtime.
// Otherwise fall back to the legacy single-bucket subject.
func RouteTask(ctx context.Context, resolver Resolver, locality *LocalityScheduler, t *Task) (string, error) {
	return RouteTaskWithProcessorPrefix(ctx, resolver, locality, "PROCESSOR", t)
}

// RouteTaskWithProcessorPrefix returns the subject to publish a task on with a custom processor prefix.
func RouteTaskWithProcessorPrefix(ctx context.Context, resolver Resolver, locality *LocalityScheduler, processorSubjectPrefix string, t *Task) (string, error) {
	if t == nil {
		return "", fmt.Errorf("task required")
	}
	if resolver == nil || t.ExecutionMode != artifact.ExecutionSandboxed || t.ProcessorID == "" {
		return t.RouteSubject(), nil
	}
	processorID, ref := t.ProcessorRef()
	if placement, ok := resolver.(PlacementResolver); ok {
		runtime, artifacts, err := placement.ResolvePlacement(ctx, t.TenantID(), processorID, ref)
		if err != nil {
			return "", fmt.Errorf("resolve placement for %s@%s: %w", processorID, ref, err)
		}
		if locality != nil {
			nodeID, err := locality.PickNode(ctx, artifacts)
			if err != nil {
				return "", err
			}
			if nodeID != "" {
				return SandboxSubjectForNodeWithPrefix(processorSubjectPrefix, nodeID, runtime, processorID), nil
			}
		}
		return SandboxSubjectForRuntimeWithPrefix(processorSubjectPrefix, runtime, processorID), nil
	}
	runtime, err := resolver.ResolveRuntime(ctx, t.TenantID(), processorID, ref)
	if err != nil {
		return "", fmt.Errorf("resolve runtime for %s@%s: %w", processorID, ref, err)
	}
	return SandboxSubjectForRuntimeWithPrefix(processorSubjectPrefix, runtime, processorID), nil
}
