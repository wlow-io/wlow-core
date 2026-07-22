package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client is a NATS-based artifact service client.
type Client struct {
	nc      *nats.Conn
	timeout time.Duration
}

// PushOptions configures the creation and upload of an artifact.
type PushOptions struct {
	Tenant      string
	ProcessorID string
	Version     string
	Tags        []string
	Manifest    Manifest
}

// NewClient creates a new artifact service client.
func NewClient(nc *nats.Conn, timeout time.Duration) (*Client, error) {
	if nc == nil {
		return nil, errors.New("nats connection required")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{nc: nc, timeout: timeout}, nil
}

// Push stores data in the NATS KV blob store and registers a manifest.
// Used for process scripts and WASM binaries (small inline artifacts).
// MicroVM rootfs images must use PutManifest after pushing to an OCI registry.
func (c *Client) Push(ctx context.Context, data []byte, opts PushOptions) (*Manifest, error) {
	if len(data) == 0 {
		return nil, errors.New("artifact data required")
	}
	tenant := opts.Tenant
	if tenant == "" {
		tenant = DefaultTenant
	}

	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	js, err := jetstream.New(c.nc)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(ctx, js, StoreConfig{})
	if err != nil {
		return nil, err
	}
	if err := store.PutBlobData(ctx, tenant, hash, data); err != nil {
		return nil, fmt.Errorf("store artifact blob: %w", err)
	}

	m := blobManifest(tenant, hash, int64(len(data)), opts)
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, c.PutManifest(ctx, m, opts.Tags)
}

// PutManifest stores a manifest in NATS KV via the artifact server.
func (c *Client) PutManifest(ctx context.Context, manifest *Manifest, tags []string) error {
	if manifest == nil {
		return errors.New("manifest required")
	}
	var res ManifestPutResponse
	return c.request(ctx, SubjectManifestPut, ManifestPutRequest{Manifest: *manifest, Tags: tags}, &res)
}

// TenantKey returns the encryption key for a tenant, creating one if needed.
func (c *Client) TenantKey(ctx context.Context, tenant string) ([]byte, error) {
	var res TenantKeyResponse
	err := c.request(ctx, SubjectTenantKey, TenantKeyRequest{Tenant: tenant}, &res)
	return res.Key, err
}

func (c *Client) request(ctx context.Context, subject string, req any, out any) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	timeout := c.timeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	msg, err := c.nc.Request(subject, data, timeout)
	if err != nil {
		return err
	}
	var failure ErrorResponse
	if err := json.Unmarshal(msg.Data, &failure); err == nil && failure.Error != "" {
		return errors.New(failure.Error)
	}
	if err := json.Unmarshal(msg.Data, out); err != nil {
		return fmt.Errorf("decode %s response: %w", subject, err)
	}
	return nil
}

func blobManifest(tenant, hash string, size int64, opts PushOptions) *Manifest {
	m := &Manifest{
		Kind:          ManifestKind,
		Tenant:        tenant,
		ProcessorID:   opts.ProcessorID,
		Version:       opts.Version,
		Runtime:       opts.Manifest.Runtime,
		IOProtocol:    opts.Manifest.IOProtocol,
		RuntimeConfig: opts.Manifest.RuntimeConfig,
		Entrypoint:    opts.Manifest.Entrypoint,
		HashAlgorithm: HashAlgorithmOCI,
		ArtifactHash:  hash,
		ArtifactSize:  size,
		Artifacts: map[string]Artifact{
			"program": {Kind: KindBlob, Blob: &BlobRef{Size: size}},
		},
		WITWorld:         opts.Manifest.WITWorld,
		Capabilities:     opts.Manifest.Capabilities,
		Deterministic:    opts.Manifest.Deterministic,
		ResourceHints:    opts.Manifest.ResourceHints,
		CompiledArtifact: opts.Manifest.CompiledArtifact,
		BuildProvenance:  opts.Manifest.BuildProvenance,
		Cache:            opts.Manifest.Cache,
		CreatedAt:        time.Now().UTC(),
	}
	m.applyDefaults()
	return m
}
