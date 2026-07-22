package artifact

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	// HashAlgorithm is the primary hashing algorithm for artifacts.
	HashAlgorithm = "blake3-keyed"
	// HashAlgorithmOCI is the hashing algorithm used for OCI-compatible artifacts.
	HashAlgorithmOCI = "oci-sha256"
	// ManifestKind is the constant identifier for processor manifest version 1.
	ManifestKind = "wlow.processor.manifest.v1"

	// DefaultTenant is the fallback tenant ID.
	DefaultTenant = "default"
	// DefaultChunkTarget is the target size for content-defined chunking.
	DefaultChunkTarget = 64 * 1024
	// DefaultChunkMin is the minimum size for content-defined chunking.
	DefaultChunkMin = 16 * 1024
	// DefaultChunkMax is the maximum size for content-defined chunking.
	DefaultChunkMax = 256 * 1024
	// DefaultMaxChunkSize is the upper bound for any single chunk.
	DefaultMaxChunkSize = DefaultChunkMax

	// BlobBucket is the NATS KV bucket for binary artifacts.
	BlobBucket = "wlow-artifact-blobs" // inline binary artifacts (process scripts, WASM)
	// ChunkBucket is the NATS KV bucket for data chunks.
	ChunkBucket = "wlow-artifact-chunks" // legacy; unused, kept for migration
	// ManifestBucket is the NATS KV bucket for processor manifests.
	ManifestBucket = "wlow-artifact-manifests"
	// RefBucket is the NATS KV bucket for artifact references.
	RefBucket = "wlow-artifact-refs"
	// TenantKeyBucket is the NATS KV bucket for tenant encryption keys.
	TenantKeyBucket = "wlow-tenant-keys"
	// QuotaBucket is the NATS KV bucket for tenant resource quotas.
	QuotaBucket = "wlow-tenant-quotas"
)

var safeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._=-]*$`)

// ExecutionMode defines how a processor is executed (e.g. attached to runner or sandboxed).
type ExecutionMode string

const (
	// ExecutionAttached means the processor runs directly in the runner process.
	ExecutionAttached ExecutionMode = "attached"
	// ExecutionSandboxed means the processor runs in an isolated sandbox (MicroVM/WASM).
	ExecutionSandboxed ExecutionMode = "sandboxed"
)

// Runtime defines the execution environment for a processor.
type Runtime string

const (
	// RuntimeProcess runs the processor as a native OS process.
	RuntimeProcess Runtime = "process"
	// RuntimeWasm runs the processor as a WebAssembly component.
	RuntimeWasm Runtime = "wasm"
	// RuntimeMicroVM runs the processor in a Firecracker microVM.
	RuntimeMicroVM Runtime = "microvm"
	// RuntimeSnapshot runs the processor from a microVM memory/disk snapshot.
	RuntimeSnapshot Runtime = "snapshot"
)

// IOProtocol defines the communication protocol between the runner and processor.
type IOProtocol string

const (
	// IOProtocolExternRefJSON uses WASM externref for JSON exchange.
	IOProtocolExternRefJSON IOProtocol = "externref-json-v0"
	// IOProtocolJSONStdio uses standard I/O for JSON exchange.
	IOProtocolJSONStdio IOProtocol = "json-stdin-stdout-v0"
	// IOProtocolJSONVsock uses Firecracker vsock for JSON exchange.
	IOProtocolJSONVsock IOProtocol = "json-vsock-v0"
	// IOProtocolJSONVsockStream uses vsock streams for bidirectional JSON.
	IOProtocolJSONVsockStream IOProtocol = "json-vsock-stream-v0"
	// IOProtocolComponentWlowCore uses the standard wlow:core WASM component interface.
	IOProtocolComponentWlowCore IOProtocol = "component-wlow-core-v0"
)

// Capability defines a permission or interface required by a processor.
type Capability string

const (
	// CapabilityContext allows access to workflow context.
	CapabilityContext Capability = "wlow:core/context"
	// CapabilityLog allows logging.
	CapabilityLog Capability = "wlow:core/log"
	// CapabilityHeartbeat allows sending heartbeats.
	CapabilityHeartbeat Capability = "wlow:core/heartbeat"
	// CapabilityState allows managing processor state.
	CapabilityState Capability = "wlow:core/state"
	// CapabilityHTTP allows making outbound HTTP requests.
	CapabilityHTTP Capability = "wlow:net/http"
	// CapabilityBlob allows access to blob storage.
	CapabilityBlob Capability = "wlow:storage/blob"
	// CapabilityMCP allows using the Model Context Protocol.
	CapabilityMCP Capability = "wlow:mcp/client"
)

// Kind defines the type of data stored in an artifact.
// Kind specifies the storage representation of an artifact.
type Kind string

const (
	// KindBlob indicates a flat binary blob.
	KindBlob Kind = "blob"
	// KindObject indicates a structured object with metadata.
	KindObject Kind = "object"
	// KindFileTree indicates a hierarchical file system tree.
	KindFileTree Kind = "file-tree"
	// KindMemorySnapshot indicates a microVM memory state.
	KindMemorySnapshot Kind = "memory-snapshot"
	// KindVMState indicates a microVM execution state.
	KindVMState Kind = "vm-state"
	// KindVMConfig indicates a microVM configuration.
	KindVMConfig Kind = "vm-config"
	// KindCompiledWasm indicates a pre-compiled WASM module.
	KindCompiledWasm Kind = "compiled-wasm"
	// KindComposite indicates an artifact composed of other artifacts.
	KindComposite Kind = "composite"
	// KindOCIDescriptor indicates a reference to an OCI image layer.
	KindOCIDescriptor Kind = "oci-descriptor"
	// KindRemoteObject indicates a reference to a remote external file.
	KindRemoteObject Kind = "remote-object"
)

const (
	// RoleOCIIndex identifies the OCI index role.
	RoleOCIIndex = "oci.index"
	// RoleRootfs identifies the root filesystem role.
	RoleRootfs = "rootfs"
	// RoleSnapshotConfig identifies the snapshot configuration role.
	RoleSnapshotConfig = "snapshot.config"
	// RoleSnapshotState identifies the snapshot CPU/device state role.
	RoleSnapshotState = "snapshot.state"
	// RoleSnapshotMemory identifies the snapshot memory state role.
	RoleSnapshotMemory = "snapshot.memory"
	// RoleSnapshotMemoryIndex identifies the snapshot memory page index role.
	RoleSnapshotMemoryIndex = "snapshot.memory.index"
	// RoleSnapshotRootfs identifies the snapshot root filesystem role.
	RoleSnapshotRootfs = "snapshot.rootfs"
	// RoleSnapshotRuntime identifies the snapshot runtime metadata role.
	RoleSnapshotRuntime = "snapshot.runtime"
)

// ChunkRef represents a content-addressed chunk of data.
type ChunkRef struct {
	Hash string `json:"hash"`
	Size int    `json:"size"`
}

// BlobRef represents a flat file composed of chunks.
type BlobRef struct {
	Size   int64      `json:"size"`
	Chunks []ChunkRef `json:"chunks"`
}

// ObjectRef represents a named structured object.
type ObjectRef struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Digest string `json:"digest,omitempty"`
}

// RemoteRef represents a reference to a file stored in an external registry.
type RemoteRef struct {
	Ref       string `json:"ref,omitempty"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	MediaType string `json:"media_type,omitempty"`
}

// OCIDescriptor represents an OCI-compatible image layer descriptor.
type OCIDescriptor struct {
	Digest      string            `json:"digest"`
	MediaType   string            `json:"media_type"`
	Size        int64             `json:"size"`
	Object      *ObjectRef        `json:"object,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// FileNode represents a single file or directory in a FileTree.
type FileNode struct {
	Path   string     `json:"path"`
	Mode   uint32     `json:"mode"`
	UID    uint32     `json:"uid,omitempty"`
	GID    uint32     `json:"gid,omitempty"`
	Size   int64      `json:"size,omitempty"`
	Chunks []ChunkRef `json:"chunks,omitempty"`
	Link   string     `json:"link,omitempty"`
}

// FileTree represents a hierarchical file system layout.
type FileTree struct {
	Files []FileNode `json:"files"`
}

// CompositeRef represents a reference to a sub-artifact in a composite artifact.
type CompositeRef struct {
	Role string `json:"role"`
	Hash string `json:"hash"`
	Kind Kind   `json:"kind,omitempty"`
}

// Artifact represents a multi-modal data container.
type Artifact struct {
	Kind     Kind              `json:"kind"`
	Tree     *FileTree         `json:"tree,omitempty"`
	Blob     *BlobRef          `json:"blob,omitempty"`
	Object   *ObjectRef        `json:"object,omitempty"`
	OCI      *OCIDescriptor    `json:"oci,omitempty"`
	Remote   *RemoteRef        `json:"remote,omitempty"`
	Linked   []CompositeRef    `json:"linked,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ResourceHints provides optimization hints for resource allocation.
type ResourceHints struct {
	MemoryBytes int64         `json:"memory_bytes,omitempty"`
	Fuel        uint64        `json:"fuel,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`
}

// Manifest is the complete definition of a processor and its assets.
type Manifest struct {
	Kind             string              `json:"kind"`
	Tenant           string              `json:"tenant"`
	ProcessorID      string              `json:"processor_id"`
	Version          string              `json:"version"`
	Runtime          Runtime             `json:"runtime"`
	IOProtocol       IOProtocol          `json:"io_protocol"`
	RuntimeConfig    map[string]any      `json:"runtime_config,omitempty"`
	Entrypoint       string              `json:"entrypoint,omitempty"`
	HashAlgorithm    string              `json:"hash_algorithm"`
	ArtifactHash     string              `json:"artifact_hash"`
	ArtifactSize     int64               `json:"artifact_size"`
	Chunks           []ChunkRef          `json:"chunks"`
	Artifacts        map[string]Artifact `json:"artifacts,omitempty"`
	WITWorld         string              `json:"wit_world,omitempty"`
	Capabilities     []Capability        `json:"capabilities_required,omitempty"`
	Deterministic    bool                `json:"deterministic"`
	ResourceHints    ResourceHints       `json:"resource_hints,omitempty"`
	CompiledArtifact string              `json:"compiled_artifact,omitempty"`
	BuildProvenance  map[string]string   `json:"build_provenance,omitempty"`
	Cache            CachePolicy         `json:"cache,omitempty"`
	CreatedAt        time.Time           `json:"created_at"`
}

// CachePolicy defines the execution caching behavior for a processor.
type CachePolicy struct {
	Enabled bool          `json:"enabled"`
	TTL     time.Duration `json:"ttl,omitempty"`
}

// Validate checks the manifest for consistency and required fields.
func (m *Manifest) Validate() error {
	if m == nil {
		return errors.New("manifest required")
	}
	m.applyDefaults()
	if m.Kind != ManifestKind {
		return fmt.Errorf("unsupported manifest kind: %s", m.Kind)
	}
	if !safeName.MatchString(m.Tenant) {
		return fmt.Errorf("invalid tenant: %s", m.Tenant)
	}
	if !safeName.MatchString(m.ProcessorID) {
		return fmt.Errorf("invalid processor id: %s", m.ProcessorID)
	}
	if !safeName.MatchString(m.Version) {
		return fmt.Errorf("invalid version: %s", m.Version)
	}
	if len(m.Chunks) == 0 && len(m.Artifacts) == 0 {
		// Direct-command process processors store their entrypoint in runtime_config
		// and carry no payload artifact — no file to upload, no OCI image to pull.
		if m.Runtime != RuntimeProcess || !hasDirectCommand(m.RuntimeConfig) {
			return errors.New("manifest artifact required")
		}
	}
	if err := ValidateArtifacts(m.Artifacts); err != nil {
		return err
	}
	if m.HashAlgorithm != HashAlgorithm && m.HashAlgorithm != HashAlgorithmOCI {
		return fmt.Errorf("unsupported hash algorithm: %s", m.HashAlgorithm)
	}
	if m.Runtime == "" {
		return errors.New("runtime required")
	}
	if m.IOProtocol == "" {
		return errors.New("io protocol required")
	}
	return nil
}

// RuntimeValue returns the manifest runtime or the default (Wasm).
func (m *Manifest) RuntimeValue() Runtime {
	if m == nil || m.Runtime == "" {
		return RuntimeWasm
	}
	return m.Runtime
}

// IOProtocolValue returns the manifest I/O protocol or the default (ExternRefJSON).
func (m *Manifest) IOProtocolValue() IOProtocol {
	if m == nil || m.IOProtocol == "" {
		return IOProtocolExternRefJSON
	}
	return m.IOProtocol
}

func (m *Manifest) applyDefaults() {
	if m.Kind == "" {
		m.Kind = ManifestKind
	}
	if m.Tenant == "" {
		m.Tenant = DefaultTenant
	}
	if m.HashAlgorithm == "" {
		m.HashAlgorithm = HashAlgorithm
	}
	if m.Runtime == "" {
		m.Runtime = RuntimeWasm
	}
	if m.IOProtocol == "" {
		m.IOProtocol = IOProtocolExternRefJSON
	}
}

// Marshal serializes the manifest to JSON.
func (m *Manifest) Marshal() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

// DecodeManifest deserializes a manifest from JSON.
func DecodeManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, m.Validate()
}

// ManifestKey returns the storage key for a manifest version.
func ManifestKey(tenant, processorID, version string) string {
	return fmt.Sprintf("%s.processor.%s.%s", normTenant(tenant), processorID, version)
}

// TagKey returns the storage key for a manifest tag.
func TagKey(tenant, processorID, tag string) string {
	return fmt.Sprintf("%s.tag.%s.%s", normTenant(tenant), processorID, tag)
}

// ChunkKey returns the storage key for a data chunk.
func ChunkKey(tenant, hash string) string {
	return fmt.Sprintf("%s.%s", normTenant(tenant), hash)
}

// RefKey returns the storage key for a reference counter.
func RefKey(tenant, hash string) string {
	return fmt.Sprintf("%s.ref.%s", normTenant(tenant), hash)
}

func normTenant(tenant string) string {
	if tenant == "" {
		return DefaultTenant
	}
	return tenant
}

// HasDirectCommand returns true when the runtime_config specifies a concrete
// command that is not the {artifact} placeholder. Such processors carry no
// stored payload — the command itself is the full entrypoint.
func HasDirectCommand(cfg map[string]any) bool {
	cmd, _ := cfg["cmd"].(string)
	return cmd != "" && cmd != "{artifact}"
}

// hasDirectCommand is the unexported alias used internally by Validate.
func hasDirectCommand(cfg map[string]any) bool { return HasDirectCommand(cfg) }
