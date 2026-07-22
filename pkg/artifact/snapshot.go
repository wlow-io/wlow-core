package artifact

import (
	"context"
	"errors"
	"time"
)

// SnapshotFiles contains local file paths for a microVM snapshot.
type SnapshotFiles struct {
	Config string
	State  string
	Memory string
	Rootfs string
}

const (
	// SnapshotConfigFile is the standard filename for microVM snapshot configuration.
	SnapshotConfigFile = "vm-config.json"
	// SnapshotStateFile is the standard filename for microVM CPU and device state.
	SnapshotStateFile = "vm-state.bin"
	// SnapshotMemoryFile is the standard filename for microVM memory dump.
	SnapshotMemoryFile = "vm-memory.bin"
	// SnapshotMemoryIndexFile is the standard filename for microVM memory page index.
	SnapshotMemoryIndexFile = "snapshot.memory.index"
	// SnapshotRootfsFile is the standard filename for microVM root filesystem image.
	SnapshotRootfsFile = "rootfs.img"
)

// PutSnapshotArtifact uploads a microVM snapshot to the artifact store.
func (s *Store) PutSnapshotArtifact(ctx context.Context, source *Manifest, version string, imageRef string, files SnapshotFiles, tags ...string) (*Manifest, error) {
	if s == nil {
		return nil, errors.New("artifact store required")
	}
	if source == nil {
		return nil, errors.New("source manifest required")
	}
	if version == "" {
		return nil, errors.New("snapshot version required")
	}
	if imageRef == "" {
		return nil, errors.New("snapshot image ref required")
	}
	objects, hash, size, err := PushSnapshotImage(ctx, imageRef, files)
	if err != nil {
		return nil, err
	}
	manifest := snapshotManifest(source, version, hash, size, objects)
	if err := s.PutArtifact(ctx, manifest, tags...); err != nil {
		return nil, err
	}
	return manifest, nil
}

func snapshotManifest(source *Manifest, version string, hash string, size int64, objects SnapshotObjects) *Manifest {
	cfg := copyRuntimeConfig(source.RuntimeConfig)
	cfg["snapshot_source_version"] = source.Version
	artifacts := snapshotArtifacts(objects)
	copySnapshotRuntimeArtifacts(artifacts, source.Artifacts)
	return &Manifest{
		Kind:          ManifestKind,
		Tenant:        source.Tenant,
		ProcessorID:   source.ProcessorID,
		Version:       version,
		Runtime:       RuntimeSnapshot,
		IOProtocol:    IOProtocolJSONVsockStream,
		RuntimeConfig: cfg,
		HashAlgorithm: HashAlgorithm,
		ArtifactHash:  hash,
		ArtifactSize:  size,
		Artifacts:     artifacts,
		Capabilities:  source.Capabilities,
		Deterministic: source.Deterministic,
		ResourceHints: source.ResourceHints,
		BuildProvenance: map[string]string{
			"source_runtime": string(source.RuntimeValue()),
		},
		Cache:     source.Cache,
		CreatedAt: time.Now().UTC(),
	}
}

func copySnapshotRuntimeArtifacts(dst map[string]Artifact, src map[string]Artifact) {
	for role, item := range src {
		if role == RoleOCIIndex {
			dst[role] = item
		}
	}
}

func snapshotArtifacts(objects SnapshotObjects) map[string]Artifact {
	return map[string]Artifact{
		RoleSnapshotConfig:      {Kind: KindRemoteObject, Remote: objects.Config},
		RoleSnapshotState:       {Kind: KindRemoteObject, Remote: objects.State},
		RoleSnapshotMemoryIndex: {Kind: KindRemoteObject, Remote: objects.MemoryIndex},
		RoleSnapshotRootfs:      {Kind: KindRemoteObject, Remote: objects.Rootfs},
	}
}

// SnapshotObjectFiles maps microVM snapshot objects to their external remote references.
func SnapshotObjectFiles(objects SnapshotObjects) map[string]*RemoteRef {
	return map[string]*RemoteRef{
		SnapshotConfigFile:      objects.Config,
		SnapshotStateFile:       objects.State,
		SnapshotMemoryIndexFile: objects.MemoryIndex,
		SnapshotRootfsFile:      objects.Rootfs,
	}
}

func copyRuntimeConfig(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+1)
	for key, value := range in {
		out[key] = value
	}
	return out
}
