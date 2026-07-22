package artifact

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	maxRefCASAttempts = 8
	tenantKeySize     = 32 // bytes, for tenant-scoped BLAKE3 keys
)

// Store manages the persistence and retrieval of manifests and artifacts.
type Store struct {
	manifests  jetstream.KeyValue
	refs       jetstream.KeyValue
	tenantKeys jetstream.KeyValue
	quotas     jetstream.KeyValue
	blobs      jetstream.KeyValue
}

// StoreConfig configures the storage buckets used by the artifact store.
type StoreConfig struct {
	BlobBucket      string
	ManifestBucket  string
	RefBucket       string
	TenantKeyBucket string
	QuotaBucket     string
	MaxBytes        int64
}

// TenantQuota defines resource limits for a specific tenant.
type TenantQuota struct {
	MaxChunkBytes    int64 `json:"max_chunk_bytes,omitempty"`
	MaxSnapshotBytes int64 `json:"max_snapshot_bytes,omitempty"`
	MaxProcessors    int64 `json:"max_processors,omitempty"`
	MaxTemplates     int64 `json:"max_templates,omitempty"`
}

// NewStore creates a new artifact store using the provided JetStream context.
func NewStore(ctx context.Context, js jetstream.JetStream, cfg StoreConfig) (*Store, error) {
	if js == nil {
		return nil, errors.New("jetstream required")
	}
	manifests, err := keyValue(ctx, js, bucket(cfg.ManifestBucket, ManifestBucket))
	if err != nil {
		return nil, err
	}
	refs, err := keyValue(ctx, js, bucket(cfg.RefBucket, RefBucket))
	if err != nil {
		return nil, err
	}
	keys, err := keyValue(ctx, js, bucket(cfg.TenantKeyBucket, TenantKeyBucket))
	if err != nil {
		return nil, err
	}
	quotas, err := keyValue(ctx, js, bucket(cfg.QuotaBucket, QuotaBucket))
	if err != nil {
		return nil, err
	}
	blobs, err := keyValue(ctx, js, bucket(cfg.BlobBucket, BlobBucket))
	if err != nil {
		return nil, err
	}
	return &Store{manifests: manifests, refs: refs, tenantKeys: keys, quotas: quotas, blobs: blobs}, nil
}

// PutBlobData stores a small binary artifact directly in NATS KV.
// Used for process scripts and WASM binaries.
func (s *Store) PutBlobData(ctx context.Context, tenant, hash string, data []byte) error {
	if s == nil || s.blobs == nil {
		return errors.New("blob store required")
	}
	if hash == "" || len(data) == 0 {
		return errors.New("hash and data required")
	}
	key := normTenant(tenant) + ".blob." + hash
	_, err := s.blobs.Put(ctx, key, data)
	return err
}

// GetBlobData retrieves a small binary artifact from NATS KV by hash.
func (s *Store) GetBlobData(ctx context.Context, tenant, hash string) ([]byte, error) {
	if s == nil || s.blobs == nil {
		return nil, errors.New("blob store required")
	}
	if hash == "" {
		return nil, errors.New("hash required")
	}
	key := normTenant(tenant) + ".blob." + hash
	entry, err := s.blobs.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

// PutArtifact stores a manifest and updates reference counts for its components.
func (s *Store) PutArtifact(ctx context.Context, m *Manifest, tags ...string) error {
	if s == nil {
		return errors.New("artifact store required")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	if _, err := s.manifests.Put(ctx, ManifestKey(m.Tenant, m.ProcessorID, m.Version), data); err != nil {
		return err
	}
	for _, c := range ManifestChunks(m) {
		if err := s.incrementRef(ctx, m.Tenant, c.Hash); err != nil {
			return err
		}
	}
	if err := s.incrementCompositeRefs(ctx, m); err != nil {
		return err
	}
	if err := s.incrementOCIRefs(ctx, m); err != nil {
		return err
	}
	return s.putTags(ctx, m, tags)
}

// GetManifest retrieves a manifest by its version.
func (s *Store) GetManifest(ctx context.Context, tenant, processorID, version string) (*Manifest, error) {
	e, err := s.manifests.Get(ctx, ManifestKey(tenant, processorID, version))
	if err != nil {
		return nil, err
	}
	return DecodeManifest(e.Value())
}

// Resolve retrieves a manifest by version or tag.
func (s *Store) Resolve(ctx context.Context, tenant, processorID, ref string) (*Manifest, error) {
	if ref == "" {
		ref = "latest"
	}
	if m, err := s.GetManifest(ctx, tenant, processorID, ref); err == nil {
		return m, nil
	}
	e, err := s.manifests.Get(ctx, TagKey(tenant, processorID, ref))
	if err != nil {
		return nil, err
	}
	return s.GetManifest(ctx, tenant, processorID, string(e.Value()))
}

// ResolveRuntime returns the runtime declared by the resolved manifest. Used
// by the workflow engine to route sandboxed tasks onto per-runtime subjects.
func (s *Store) ResolveRuntime(ctx context.Context, tenant, processorID, ref string) (Runtime, error) {
	m, err := s.Resolve(ctx, tenant, processorID, ref)
	if err != nil {
		return "", err
	}
	return m.RuntimeValue(), nil
}

// ResolvePlacement resolves a manifest and returns its placement keys for scheduling.
func (s *Store) ResolvePlacement(ctx context.Context, tenant, processorID, ref string) (Runtime, []string, error) {
	m, err := s.Resolve(ctx, tenant, processorID, ref)
	if err != nil {
		return "", nil, err
	}
	return m.RuntimeValue(), ManifestPlacementKeys(m), nil
}

// FetchArtifact retrieves the binary payload of a process or WASM artifact
// from the NATS KV blob store. MicroVM artifacts are OCI-backed; call PullRemoteFile instead.
func (s *Store) FetchArtifact(ctx context.Context, m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, errors.New("manifest required")
	}
	if m.ArtifactHash == "" {
		return nil, errors.New("manifest artifact hash required")
	}
	return s.GetBlobData(ctx, m.Tenant, m.ArtifactHash)
}

// DeleteVersion deletes a specific manifest version and decrements its reference counts.
func (s *Store) DeleteVersion(ctx context.Context, tenant, processorID, version string) error {
	m, err := s.GetManifest(ctx, tenant, processorID, version)
	if err != nil {
		return err
	}
	for _, c := range ManifestChunks(m) {
		if err := s.decrementRef(ctx, m.Tenant, c.Hash); err != nil {
			return err
		}
	}
	if err := s.decrementCompositeRefs(ctx, m); err != nil {
		return err
	}
	if err := s.decrementOCIRefs(ctx, m); err != nil {
		return err
	}
	return s.manifests.Delete(ctx, ManifestKey(tenant, processorID, version))
}

// EnsureTenantKey ensures a stable encryption/signing key exists for a tenant.
func (s *Store) EnsureTenantKey(ctx context.Context, tenant string) ([]byte, error) {
	key := normTenant(tenant)
	entry, err := s.tenantKeys.Get(ctx, key)
	if err == nil {
		return entry.Value(), validateTenantKey(entry.Value())
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	buf := make([]byte, tenantKeySize)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	_, err = s.tenantKeys.Create(ctx, key, buf)
	if err == nil {
		return buf, nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return nil, err
	}
	entry, err = s.tenantKeys.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), validateTenantKey(entry.Value())
}

// PutQuota sets resource limits for a tenant.
func (s *Store) PutQuota(ctx context.Context, tenant string, quota TenantQuota) error {
	data, err := json.Marshal(quota)
	if err != nil {
		return err
	}
	_, err = s.quotas.Put(ctx, normTenant(tenant), data)
	return err
}

// GetQuota retrieves resource limits for a tenant.
func (s *Store) GetQuota(ctx context.Context, tenant string) (TenantQuota, error) {
	entry, err := s.quotas.Get(ctx, normTenant(tenant))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return TenantQuota{}, nil
	}
	if err != nil {
		return TenantQuota{}, err
	}
	var quota TenantQuota
	return quota, json.Unmarshal(entry.Value(), &quota)
}

// SweepTenant performs garbage collection for a tenant (stub).
func (s *Store) SweepTenant(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (s *Store) putTags(ctx context.Context, m *Manifest, tags []string) error {
	for _, tag := range tags {
		if !safeName.MatchString(tag) {
			return fmt.Errorf("invalid tag: %s", tag)
		}
		if _, err := s.manifests.Put(ctx, TagKey(m.Tenant, m.ProcessorID, tag), []byte(m.Version)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) tenantChunkBytes(_ context.Context, _ string) (int64, error) {
	return 0, nil
}

func (s *Store) incrementCompositeRefs(ctx context.Context, m *Manifest) error {
	for _, ref := range ManifestCompositeRefs(m) {
		if err := s.incrementRef(ctx, m.Tenant, "artifact."+ref.Hash); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) decrementCompositeRefs(ctx context.Context, m *Manifest) error {
	for _, ref := range ManifestCompositeRefs(m) {
		if err := s.decrementRefOnly(ctx, m.Tenant, "artifact."+ref.Hash); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) incrementOCIRefs(ctx context.Context, m *Manifest) error {
	for _, desc := range ManifestOCIDescriptors(m) {
		if err := s.incrementRef(ctx, m.Tenant, DescriptorRefKeyPart(desc.Digest)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) decrementOCIRefs(ctx context.Context, m *Manifest) error {
	for _, desc := range ManifestOCIDescriptors(m) {
		if err := s.decrementOCIRef(ctx, m.Tenant, desc.Digest); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) decrementOCIRef(ctx context.Context, tenant, digest string) error {
	key := RefKey(tenant, DescriptorRefKeyPart(digest))
	for attempt := 0; attempt < maxRefCASAttempts; attempt++ {
		count, rev, err := s.refCount(ctx, key)
		if err != nil {
			return err
		}
		if count <= 1 {
			_ = s.refs.Delete(ctx, key)
			return nil
		}
		_, err = s.refs.Update(ctx, key, []byte(strconv.FormatInt(count-1, 10)), rev)
		if err == nil {
			return nil
		}
		if !isCASConflict(err) {
			return err
		}
	}
	return errors.New("refcount update contention")
}

func (s *Store) incrementRef(ctx context.Context, tenant, hash string) error {
	key := RefKey(tenant, hash)
	for attempt := 0; attempt < maxRefCASAttempts; attempt++ {
		count, rev, err := s.refCount(ctx, key)
		if err != nil {
			return err
		}
		data := []byte(strconv.FormatInt(count+1, 10))
		if rev == 0 {
			_, err = s.refs.Create(ctx, key, data)
		} else {
			_, err = s.refs.Update(ctx, key, data, rev)
		}
		if err == nil {
			return nil
		}
		if !isCASConflict(err) {
			return err
		}
	}
	return errors.New("refcount update contention")
}

func (s *Store) decrementRef(ctx context.Context, tenant, hash string) error {
	key := RefKey(tenant, hash)
	for attempt := 0; attempt < maxRefCASAttempts; attempt++ {
		count, rev, err := s.refCount(ctx, key)
		if err != nil {
			return err
		}
		if count <= 1 {
			_ = s.refs.Delete(ctx, key)
			return nil
		}
		_, err = s.refs.Update(ctx, key, []byte(strconv.FormatInt(count-1, 10)), rev)
		if err == nil {
			return nil
		}
		if !isCASConflict(err) {
			return err
		}
	}
	return errors.New("refcount update contention")
}

func (s *Store) decrementRefOnly(ctx context.Context, tenant, hash string) error {
	key := RefKey(tenant, hash)
	for attempt := 0; attempt < maxRefCASAttempts; attempt++ {
		count, rev, err := s.refCount(ctx, key)
		if err != nil {
			return err
		}
		if count <= 1 {
			_ = s.refs.Delete(ctx, key)
			return nil
		}
		_, err = s.refs.Update(ctx, key, []byte(strconv.FormatInt(count-1, 10)), rev)
		if err == nil {
			return nil
		}
		if !isCASConflict(err) {
			return err
		}
	}
	return errors.New("refcount update contention")
}

func validateTenantKey(key []byte) error {
	if len(key) != tenantKeySize {
		return errors.New("invalid tenant key")
	}
	return nil
}

func isCASConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	return strings.Contains(err.Error(), "wrong last sequence")
}

func (s *Store) refCount(ctx context.Context, key string) (int64, uint64, error) {
	e, err := s.refs.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	n, err := strconv.ParseInt(string(e.Value()), 10, 64)
	return n, e.Revision(), err
}

func keyValue(ctx context.Context, js jetstream.JetStream, name string) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, name)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name, History: 64})
	}
	return kv, err
}

func bucket(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// MarshalJSON is a helper for JSON serialization.
func MarshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}
