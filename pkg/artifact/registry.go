package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/zeebo/blake3"
)

const (
	// MediaTypeSnapshotConfig is the OCI media type for microVM snapshot configuration.
	MediaTypeSnapshotConfig = "application/vnd.wlow.snapshot.config.v1+json"
	// MediaTypeSnapshotState is the OCI media type for microVM execution state.
	MediaTypeSnapshotState = "application/vnd.wlow.snapshot.state.v1+json"
	// MediaTypeSnapshotMemory is the OCI media type for microVM memory dump.
	MediaTypeSnapshotMemory = "application/vnd.wlow.snapshot.memory.v1"
	// MediaTypeSnapshotMemoryIndex is the OCI media type for microVM memory page index.
	MediaTypeSnapshotMemoryIndex = "application/vnd.wlow.snapshot.memory.index.v1+json"
	// MediaTypeSnapshotMemoryChunk is the OCI media type for a compressed memory chunk.
	MediaTypeSnapshotMemoryChunk = "application/vnd.wlow.snapshot.memory.chunk.v1+gzip"
	// MediaTypeSnapshotRootfs is the OCI media type for microVM root filesystem.
	MediaTypeSnapshotRootfs = "application/vnd.wlow.snapshot.rootfs.v1"
	// MediaTypeRootfsEROFS is the OCI media type for an EROFS root filesystem image.
	MediaTypeRootfsEROFS = "application/vnd.wlow.rootfs.erofs.v1"
)

// RemoteImageDescriptor associates an OCI descriptor with its role in a manifest.
type RemoteImageDescriptor struct {
	Descriptor OCIDescriptor
	Role       string
}

// InspectRemoteOCI retrieves metadata for an OCI image and converts it to a manifest.
func InspectRemoteOCI(ctx context.Context, imageRef string, opts PushOptions) (*Manifest, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, err
	}
	desc, err := remote.Get(ref, remoteOptions(ctx)...)
	if err != nil {
		return nil, err
	}
	descs, err := remoteDescriptors(ctx, ref, desc)
	if err != nil {
		return nil, err
	}
	hash := desc.Digest.String()
	size := int64(0)
	for idx := 0; idx < len(descs) && idx < maxOCIDescriptors; idx++ {
		size += descs[idx].Descriptor.Size
	}
	manifest := remoteOCIManifest(opts, imageRef, hash, size, descs)
	return manifest, manifest.Validate()
}

func remoteDescriptors(ctx context.Context, ref name.Reference, desc *remote.Descriptor) ([]RemoteImageDescriptor, error) {
	out := []RemoteImageDescriptor{{
		Role: RoleOCIIndex,
		Descriptor: OCIDescriptor{
			Digest:    desc.Digest.String(),
			MediaType: string(desc.MediaType),
			Size:      desc.Size,
		},
	}}
	if desc.MediaType.IsIndex() {
		var index v1.IndexManifest
		if err := json.Unmarshal(desc.Manifest, &index); err != nil {
			return nil, err
		}
		for idx := 0; idx < len(index.Manifests) && idx < maxOCIDescriptors; idx++ {
			child, err := remote.Get(ref.Context().Digest(index.Manifests[idx].Digest.String()), remoteOptions(ctx)...)
			if err != nil {
				return nil, err
			}
			items, err := remoteManifestDescriptors(child)
			if err != nil {
				return nil, err
			}
			out = append(out, items...)
		}
		return dedupeRemoteDescriptors(out), nil
	}
	items, err := remoteManifestDescriptors(desc)
	if err != nil {
		return nil, err
	}
	out = append(out, items...)
	return dedupeRemoteDescriptors(out), nil
}

func remoteManifestDescriptors(desc *remote.Descriptor) ([]RemoteImageDescriptor, error) {
	var manifest v1.Manifest
	if err := json.Unmarshal(desc.Manifest, &manifest); err != nil {
		return nil, err
	}
	out := []RemoteImageDescriptor{{
		Role: descriptorRole(OCIDescriptor{MediaType: string(desc.MediaType), Digest: desc.Digest.String()}, 1),
		Descriptor: OCIDescriptor{
			Digest:    desc.Digest.String(),
			MediaType: string(desc.MediaType),
			Size:      desc.Size,
		},
	}, {
		Role: descriptorRole(OCIDescriptor{MediaType: string(manifest.Config.MediaType), Digest: manifest.Config.Digest.String()}, 2),
		Descriptor: OCIDescriptor{
			Digest:    manifest.Config.Digest.String(),
			MediaType: string(manifest.Config.MediaType),
			Size:      manifest.Config.Size,
		},
	}}
	for idx := 0; idx < len(manifest.Layers) && idx < maxOCIDescriptors; idx++ {
		layer := manifest.Layers[idx]
		desc := OCIDescriptor{
			Digest:      layer.Digest.String(),
			MediaType:   string(layer.MediaType),
			Size:        layer.Size,
			Annotations: layer.Annotations,
		}
		out = append(out, RemoteImageDescriptor{Role: descriptorRole(desc, idx+3), Descriptor: desc})
	}
	return out, nil
}

func dedupeRemoteDescriptors(in []RemoteImageDescriptor) []RemoteImageDescriptor {
	seen := make(map[string]struct{}, len(in))
	out := make([]RemoteImageDescriptor, 0, len(in))
	for idx := 0; idx < len(in) && idx < maxOCIDescriptors; idx++ {
		digest := in[idx].Descriptor.Digest
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, in[idx])
	}
	return out
}

func remoteOCIManifest(opts PushOptions, imageRef, hash string, size int64, descs []RemoteImageDescriptor) *Manifest {
	tenant := opts.Tenant
	if tenant == "" {
		tenant = DefaultTenant
	}
	cfg := copyRuntimeConfig(opts.Manifest.RuntimeConfig)
	cfg["image_ref"] = imageRef
	m := &Manifest{
		Kind:             ManifestKind,
		Tenant:           tenant,
		ProcessorID:      opts.ProcessorID,
		Version:          opts.Version,
		Runtime:          opts.Manifest.Runtime,
		IOProtocol:       opts.Manifest.IOProtocol,
		RuntimeConfig:    cfg,
		HashAlgorithm:    HashAlgorithmOCI,
		ArtifactHash:     hash,
		ArtifactSize:     size,
		Artifacts:        map[string]Artifact{},
		WITWorld:         opts.Manifest.WITWorld,
		Capabilities:     opts.Manifest.Capabilities,
		Deterministic:    opts.Manifest.Deterministic,
		ResourceHints:    opts.Manifest.ResourceHints,
		CompiledArtifact: opts.Manifest.CompiledArtifact,
		BuildProvenance:  opts.Manifest.BuildProvenance,
		Cache:            opts.Manifest.Cache,
		CreatedAt:        time.Now().UTC(),
	}
	for idx := 0; idx < len(descs) && idx < maxOCIDescriptors; idx++ {
		item := descs[idx]
		desc := item.Descriptor
		m.Artifacts[item.Role] = Artifact{Kind: KindOCIDescriptor, OCI: &desc}
	}
	return m
}

// PushSnapshotImage uploads a microVM snapshot as an OCI image.
func PushSnapshotImage(ctx context.Context, imageRef string, files SnapshotFiles) (SnapshotObjects, string, int64, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return SnapshotObjects{}, "", 0, err
	}
	layers, refs, chunkSizeTotal, err := snapshotLayers(ref.Context(), files)
	if err != nil {
		return SnapshotObjects{}, "", 0, err
	}
	img, err := mutate.AppendLayers(empty.Image, layers...)
	if err != nil {
		return SnapshotObjects{}, "", 0, err
	}
	if err := remote.Write(ref, img, remoteOptions(ctx)...); err != nil {
		return SnapshotObjects{}, "", 0, err
	}
	attachSnapshotRepo(&refs, ref.Context())
	hash, err := img.Digest()
	if err != nil {
		return SnapshotObjects{}, "", 0, err
	}
	size := refs.Config.Size + refs.State.Size + refs.MemoryIndex.Size + refs.Rootfs.Size + chunkSizeTotal
	return refs, hash.String(), size, nil
}

// PushRootfsImage uploads an EROFS rootfs image as an OCI image.
func PushRootfsImage(ctx context.Context, imageRef string, rootfsPath string) (*RemoteRef, string, int64, error) {
	if imageRef == "" {
		return nil, "", 0, errors.New("rootfs image ref required")
	}
	if rootfsPath == "" {
		return nil, "", 0, errors.New("rootfs path required")
	}
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return nil, "", 0, err
	}
	data, err := os.ReadFile(rootfsPath)
	if err != nil {
		return nil, "", 0, err
	}
	if len(data) == 0 {
		return nil, "", 0, errors.New("rootfs is empty")
	}
	layer := static.NewLayer(data, MediaTypeRootfsEROFS)
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return nil, "", 0, err
	}
	if err := remote.Write(ref, img, remoteOptions(ctx)...); err != nil {
		return nil, "", 0, err
	}
	digest, err := layer.Digest()
	if err != nil {
		return nil, "", 0, err
	}
	size, err := layer.Size()
	if err != nil {
		return nil, "", 0, err
	}
	imageDigest, err := img.Digest()
	if err != nil {
		return nil, "", 0, err
	}
	remote := &RemoteRef{
		Ref:       ref.Context().Digest(digest.String()).Name(),
		Digest:    digest.String(),
		Size:      size,
		MediaType: string(MediaTypeRootfsEROFS),
	}
	return remote, imageDigest.String(), size, nil
}

func attachSnapshotRepo(objects *SnapshotObjects, repo name.Repository) {
	for _, ref := range []*RemoteRef{objects.Config, objects.State, objects.MemoryIndex, objects.Rootfs} {
		if ref != nil {
			ref.Ref = repo.Digest(ref.Digest).Name()
		}
	}
}

// PullRemoteFile downloads a file from an OCI registry based on a RemoteRef.
func PullRemoteFile(ctx context.Context, remoteRef *RemoteRef, dest string) error {
	if remoteRef == nil {
		return errors.New("remote ref required")
	}
	ref, err := name.NewDigest(remoteRef.Ref)
	if err != nil {
		return err
	}
	layer, err := remote.Layer(ref, remoteOptions(ctx)...)
	if err != nil {
		return err
	}
	rc, err := layer.Compressed()
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			slog.Default().Error("close remote layer reader failed", "error", err)
		}
	}()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil {
			slog.Default().Error("close destination file failed", "error", err)
		}
	}()
	n, err := io.Copy(out, rc)
	if err != nil {
		return err
	}
	if remoteRef.Size >= 0 && n != remoteRef.Size {
		return fmt.Errorf("remote size mismatch: wrote %d want %d", n, remoteRef.Size)
	}
	return nil
}

// PullOCILayer downloads a specific OCI layer by digest.
func PullOCILayer(ctx context.Context, imageRef, digest, dest string) error {
	if imageRef == "" {
		return errors.New("image ref required")
	}
	if digest == "" {
		return errors.New("digest required")
	}
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return err
	}
	layerRef := ref.Context().Digest(digest)
	layer, err := remote.Layer(layerRef, remoteOptions(ctx)...)
	if err != nil {
		return err
	}
	rc, err := layer.Compressed()
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			slog.Default().Error("close remote layer reader failed", "error", err)
		}
	}()
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil {
			slog.Default().Error("close destination file failed", "error", err)
		}
	}()
	_, err = io.Copy(out, rc)
	return err
}

func remoteOptions(ctx context.Context) []remote.Option {
	options := []remote.Option{remote.WithContext(ctx)}
	auth := strings.TrimSpace(os.Getenv("IMAGE_PULL_AUTH"))
	if auth == "" {
		return append(options, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}
	decoded, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return append(options, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}
	user, pass, ok := strings.Cut(string(decoded), ":")
	if !ok || user == "" || pass == "" {
		return append(options, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}
	return append(options, remote.WithAuth(authn.FromConfig(authn.AuthConfig{
		Username: user,
		Password: pass,
	})))
}

func snapshotLayers(repo name.Repository, files SnapshotFiles) ([]v1.Layer, SnapshotObjects, int64, error) {
	items := []struct {
		role      string
		path      string
		mediaType types.MediaType
		assign    func(*SnapshotObjects, *RemoteRef)
	}{
		{RoleSnapshotConfig, files.Config, MediaTypeSnapshotConfig, func(o *SnapshotObjects, r *RemoteRef) { o.Config = r }},
		{RoleSnapshotState, files.State, MediaTypeSnapshotState, func(o *SnapshotObjects, r *RemoteRef) { o.State = r }},
		{RoleSnapshotRootfs, files.Rootfs, MediaTypeSnapshotRootfs, func(o *SnapshotObjects, r *RemoteRef) { o.Rootfs = r }},
	}
	layers := make([]v1.Layer, 0, len(items))
	objects := SnapshotObjects{}
	var chunkCompressedTotal int64
	for idx := 0; idx < len(items); idx++ {
		data, err := os.ReadFile(items[idx].path)
		if err != nil {
			return nil, objects, 0, err
		}
		layer := static.NewLayer(data, items[idx].mediaType)
		digest, err := layer.Digest()
		if err != nil {
			return nil, objects, 0, err
		}
		size, err := layer.Size()
		if err != nil {
			return nil, objects, 0, err
		}
		items[idx].assign(&objects, &RemoteRef{Digest: digest.String(), Size: size, MediaType: string(items[idx].mediaType)})
		layers = append(layers, layer)
	}
	chunkLayers, indexRef, chunkTotal, err := snapshotMemoryLayers(repo, files.Memory)
	if err != nil {
		return nil, objects, 0, err
	}
	objects.MemoryIndex = indexRef
	chunkCompressedTotal = chunkTotal
	layers = append(layers, chunkLayers...)
	return layers, objects, chunkCompressedTotal, nil
}

func snapshotMemoryLayers(repo name.Repository, memoryPath string) ([]v1.Layer, *RemoteRef, int64, error) {
	file, err := os.Open(memoryPath)
	if err != nil {
		return nil, nil, 0, err
	}
	defer file.Close()
	buffer := make([]byte, DefaultSnapshotChunkSize)
	chunks := make([]SnapshotMemoryChunk, 0, 128)
	layers := make([]v1.Layer, 0, 128)
	var offset int64
	var compressedTotal int64
	for chunkID := 0; chunkID < MaxSnapshotChunks; chunkID++ {
		n, readErr := file.Read(buffer)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, nil, 0, readErr
		}
		if n <= 0 {
			break
		}
		raw := append([]byte(nil), buffer[:n]...)
		layer := static.NewLayer(raw, MediaTypeSnapshotMemoryChunk)
		digest, err := layer.Digest()
		if err != nil {
			return nil, nil, 0, err
		}
		size, err := layer.Size()
		if err != nil {
			return nil, nil, 0, err
		}
		sha := sha256.Sum256(raw)
		b3 := blake3.Sum256(raw)
		chunks = append(chunks, SnapshotMemoryChunk{
			Offset:           offset,
			Ref:              repo.Digest(digest.String()).Name(),
			Digest:           digest.String(),
			CompressedSize:   size,
			UncompressedSize: int64(n),
			Compression:      "gzip",
			SHA256:           hex.EncodeToString(sha[:]),
			BLAKE3:           hex.EncodeToString(b3[:]),
		})
		layers = append(layers, layer)
		offset += int64(n)
		compressedTotal += size
		if offset > MaxSnapshotMemoryBytes {
			return nil, nil, 0, fmt.Errorf("snapshot memory too large: %d", offset)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	if len(chunks) == 0 {
		return nil, nil, 0, errors.New("snapshot memory file is empty")
	}
	index := SnapshotMemoryIndex{
		Version:     SnapshotMemoryIndexVersionV1,
		ChunkSize:   DefaultSnapshotChunkSize,
		MemoryBytes: offset,
		Chunks:      chunks,
		Layout:      "fixed",
	}
	if err := index.Validate(); err != nil {
		return nil, nil, 0, err
	}
	indexPayload, err := json.Marshal(index)
	if err != nil {
		return nil, nil, 0, err
	}
	indexLayer := static.NewLayer(indexPayload, MediaTypeSnapshotMemoryIndex)
	indexDigest, err := indexLayer.Digest()
	if err != nil {
		return nil, nil, 0, err
	}
	indexSize, err := indexLayer.Size()
	if err != nil {
		return nil, nil, 0, err
	}
	indexRef := &RemoteRef{
		Digest:    indexDigest.String(),
		Size:      indexSize,
		MediaType: string(MediaTypeSnapshotMemoryIndex),
	}
	layers = append(layers, indexLayer)
	return layers, indexRef, compressedTotal, nil
}
