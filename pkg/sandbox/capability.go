package sandbox

import (
	"errors"
	"slices"

	"github.com/wlow/wlow/pkg/artifact"
)

// WlowCoreWIT is the WIT (WebAssembly Interface Type) definition for the core wlow interfaces.
const WlowCoreWIT = `package wlow:core@0.1.0;

interface context {
  tenant: func() -> string;
  task-id: func() -> string;
  workflow-id: func() -> string;
  attempt: func() -> u32;
}

interface log {
  write: func(level: string, message: string);
}

interface heartbeat {
  beat: func();
  progress: func(percent: u8, message: string);
}

interface state {
  get: func(scope: string, key: string) -> option<list<u8>>;
  put: func(scope: string, key: string, value: list<u8>);
  cas: func(scope: string, key: string, old: option<list<u8>>, new: list<u8>) -> bool;
}

interface http {
  fetch: func(method: string, url: string, headers: list<tuple<string, string>>, body: list<u8>) -> result<tuple<u16, list<u8>>, string>;
}

interface blob {
  read: func(key: string) -> result<list<u8>, string>;
  write: func(key: string, data: list<u8>) -> result<_, string>;
}

interface mcp-client {
  call-tool: func(server: string, name: string, args: list<u8>) -> result<list<u8>, string>;
}

world deterministic-processor {
  import context;
  import log;
  import heartbeat;
  import state;
  export process: func(input: list<u8>) -> result<list<u8>, string>;
}

world effectful-processor {
  import context;
  import log;
  import heartbeat;
  import state;
  import http;
  import blob;
  import mcp-client;
  export process: func(input: list<u8>) -> result<list<u8>, string>;
}`

// Policy defines the set of allowed capabilities for a sandbox.
type Policy struct {
	Allowed []artifact.Capability
}

// DefaultPolicy returns a policy that allows all standard capabilities.
func DefaultPolicy() Policy {
	return Policy{
		Allowed: []artifact.Capability{
			artifact.CapabilityContext,
			artifact.CapabilityLog,
			artifact.CapabilityHeartbeat,
			artifact.CapabilityState,
			artifact.CapabilityHTTP,
			artifact.CapabilityBlob,
			artifact.CapabilityMCP,
		},
	}
}

// DeterministicPolicy returns a policy restricted to deterministic capabilities.
func DeterministicPolicy() Policy {
	return Policy{
		Allowed: []artifact.Capability{
			artifact.CapabilityContext,
			artifact.CapabilityLog,
			artifact.CapabilityState,
		},
	}
}

// Authorize checks if the given manifest's required capabilities are allowed by the policy.
func (p Policy) Authorize(m *artifact.Manifest) error {
	if m == nil {
		return errors.New("manifest required")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	allowed := p.Allowed
	if len(allowed) == 0 {
		allowed = DefaultPolicy().Allowed
	}
	if m.Deterministic {
		allowed = intersect(allowed, DeterministicPolicy().Allowed)
	}
	for _, cap := range m.Capabilities {
		if !slices.Contains(allowed, cap) {
			return errors.New("capability denied: " + string(cap))
		}
	}
	return nil
}

func intersect(left, right []artifact.Capability) []artifact.Capability {
	out := make([]artifact.Capability, 0, len(left))
	for _, cap := range left {
		if slices.Contains(right, cap) {
			out = append(out, cap)
		}
	}
	return out
}
