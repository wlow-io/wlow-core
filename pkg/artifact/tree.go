package artifact

import "errors"

const maxManifestRefs = 1 << 20

// ManifestPrimaryChunks returns the primary data chunks for a manifest.
func ManifestPrimaryChunks(m *Manifest) []ChunkRef {
	if m == nil {
		return nil
	}
	if len(m.Chunks) > 0 {
		return append([]ChunkRef(nil), m.Chunks...)
	}
	if a, ok := m.Artifacts["program"]; ok && a.Blob != nil {
		return append([]ChunkRef(nil), a.Blob.Chunks...)
	}
	return nil
}

// ManifestChunks returns all unique data chunks referenced by a manifest.
func ManifestChunks(m *Manifest) []ChunkRef {
	if m == nil {
		return nil
	}
	out := make([]ChunkRef, 0, len(m.Chunks))
	out = append(out, m.Chunks...)
	for _, artifact := range m.Artifacts {
		out = appendArtifactChunks(out, artifact)
		if len(out) > maxManifestRefs {
			return out[:maxManifestRefs]
		}
	}
	return dedupeChunks(out)
}

// ManifestCompositeRefs returns all sub-artifact references in a composite manifest.
func ManifestCompositeRefs(m *Manifest) []CompositeRef {
	if m == nil {
		return nil
	}
	out := make([]CompositeRef, 0, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		out = append(out, artifact.Linked...)
		if len(out) > maxManifestRefs {
			return out[:maxManifestRefs]
		}
	}
	return out
}

// ManifestOCIDescriptors returns all OCI descriptors referenced by a manifest.
func ManifestOCIDescriptors(m *Manifest) []OCIDescriptor {
	if m == nil {
		return nil
	}
	out := make([]OCIDescriptor, 0, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		if artifact.OCI != nil {
			out = append(out, *artifact.OCI)
			if len(out) > maxManifestRefs {
				return out[:maxManifestRefs]
			}
		}
	}
	return out
}

// ManifestObjectRefs returns all object references in a manifest.
func ManifestObjectRefs(m *Manifest) []*ObjectRef {
	if m == nil {
		return nil
	}
	out := make([]*ObjectRef, 0, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		if artifact.Object != nil {
			out = append(out, artifact.Object)
			if len(out) > maxManifestRefs {
				return out[:maxManifestRefs]
			}
		}
	}
	return out
}

// ValidateArtifacts checks a map of artifacts for required fields and consistency.
func ValidateArtifacts(artifacts map[string]Artifact) error {
	for role, artifact := range artifacts {
		if role == "" {
			return errors.New("artifact role required")
		}
		if artifact.Kind == "" {
			return errors.New("artifact_kind required")
		}
		if artifact.Kind == KindBlob && artifact.Blob == nil {
			return errors.New("blob artifact requires blob ref")
		}
		if artifact.Kind == KindObject && artifact.Object == nil {
			return errors.New("object artifact requires object ref")
		}
		if artifact.Kind == KindFileTree && artifact.Tree == nil {
			return errors.New("file-tree artifact requires tree")
		}
		if artifact.Kind == KindOCIDescriptor && artifact.OCI == nil {
			return errors.New("oci-descriptor artifact requires descriptor")
		}
		if artifact.Kind == KindRemoteObject && artifact.Remote == nil {
			return errors.New("remote-object artifact requires remote ref")
		}
	}
	return nil
}

// ManifestObject retrieves a single object reference from a manifest by role.
func ManifestObject(m *Manifest, role string) *ObjectRef {
	if m == nil || role == "" {
		return nil
	}
	artifact, ok := m.Artifacts[role]
	if !ok {
		return nil
	}
	return artifact.Object
}

// ManifestRemote retrieves a single remote reference from a manifest by role.
func ManifestRemote(m *Manifest, role string) *RemoteRef {
	if m == nil || role == "" {
		return nil
	}
	artifact, ok := m.Artifacts[role]
	if !ok {
		return nil
	}
	return artifact.Remote
}

// SnapshotObjects contains remote references to microVM snapshot components.
type SnapshotObjects struct {
	Config      *RemoteRef
	State       *RemoteRef
	MemoryIndex *RemoteRef
	Rootfs      *RemoteRef
}

// ManifestSnapshotObjects retrieves all snapshot components from a manifest.
func ManifestSnapshotObjects(m *Manifest) SnapshotObjects {
	return SnapshotObjects{
		Config:      ManifestRemote(m, RoleSnapshotConfig),
		State:       ManifestRemote(m, RoleSnapshotState),
		MemoryIndex: ManifestRemote(m, RoleSnapshotMemoryIndex),
		Rootfs:      ManifestRemote(m, RoleSnapshotRootfs),
	}
}

// ManifestPlacementKeys returns unique keys for identifying and placing a manifest's workload.
func ManifestPlacementKeys(m *Manifest) []string {
	if m == nil {
		return nil
	}
	keys := []string{"runtime:" + string(m.RuntimeValue()), "artifact:" + m.ArtifactHash}
	if ref, ok := m.RuntimeConfig["image_ref"].(string); ok && ref != "" {
		keys = append(keys, "image:"+ref)
	}
	for _, desc := range ManifestOCIDescriptors(m) {
		if desc.Digest != "" {
			keys = append(keys, "oci:"+desc.Digest)
		}
	}
	for _, remote := range ManifestRemoteRefs(m) {
		if remote.Digest != "" {
			keys = append(keys, "remote:"+remote.Digest)
		}
	}
	return keys
}

// ManifestRemoteRefs returns all remote references in a manifest.
func ManifestRemoteRefs(m *Manifest) []RemoteRef {
	if m == nil {
		return nil
	}
	out := make([]RemoteRef, 0, len(m.Artifacts))
	for _, item := range m.Artifacts {
		if item.Remote != nil {
			out = append(out, *item.Remote)
		}
	}
	return out
}

func appendArtifactChunks(out []ChunkRef, artifact Artifact) []ChunkRef {
	if artifact.Blob != nil {
		out = append(out, artifact.Blob.Chunks...)
	}
	if artifact.Tree == nil {
		return out
	}
	for _, file := range artifact.Tree.Files {
		out = append(out, file.Chunks...)
	}
	return out
}

func dedupeChunks(in []ChunkRef) []ChunkRef {
	seen := make(map[string]struct{}, len(in))
	out := make([]ChunkRef, 0, len(in))
	for _, ref := range in {
		if ref.Hash == "" {
			continue
		}
		if _, ok := seen[ref.Hash]; ok {
			continue
		}
		seen[ref.Hash] = struct{}{}
		out = append(out, ref)
	}
	return out
}
