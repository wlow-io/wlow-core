package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/wlow/wlow/pkg/artifact"
)

// SnapshotPrepareConfig configures the preparation of a microVM snapshot.
type SnapshotPrepareConfig struct {
	Store       *artifact.Store
	DataDir     string
	Tenant      string
	ProcessorID string
	SourceRef   string
	Version     string
	Tags        []string
	SnapshotRef string
	NATSURL     string
}

// SnapshotPreparer handles the end-to-end flow of creating a microVM snapshot.
type SnapshotPreparer struct {
	cfg SnapshotPrepareConfig
}

// NewSnapshotPreparer creates a new snapshot preparer.
func NewSnapshotPreparer(cfg SnapshotPrepareConfig) (*SnapshotPreparer, error) {
	if cfg.Store == nil {
		return nil, errors.New("artifact store required")
	}
	if cfg.ProcessorID == "" || cfg.Version == "" {
		return nil, errors.New("processor id and snapshot version required")
	}
	if cfg.SourceRef == "" {
		cfg.SourceRef = "latest"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/wlow"
	}
	if cfg.SnapshotRef == "" {
		cfg.SnapshotRef = defaultSnapshotRef(cfg)
	}
	return &SnapshotPreparer{cfg: cfg}, nil
}

// Prepare executes the snapshot preparation process and uploads the result.
func (p *SnapshotPreparer) Prepare(ctx context.Context) (*artifact.Manifest, error) {
	if ctx == nil {
		return nil, errors.New("context required")
	}
	if p == nil {
		return nil, errors.New("snapshot preparer required")
	}
	source, err := p.cfg.Store.Resolve(ctx, p.cfg.Tenant, p.cfg.ProcessorID, p.cfg.SourceRef)
	if err != nil {
		return nil, fmt.Errorf("resolve source manifest: %w", err)
	}
	if source.RuntimeValue() != artifact.RuntimeMicroVM {
		return nil, fmt.Errorf("source runtime must be %s", artifact.RuntimeMicroVM)
	}
	files, err := p.runRustPreparer(ctx)
	if err != nil {
		return nil, err
	}
	return p.cfg.Store.PutSnapshotArtifact(ctx, source, p.cfg.Version, p.cfg.SnapshotRef, files, p.cfg.Tags...)
}

func (p *SnapshotPreparer) runRustPreparer(ctx context.Context) (artifact.SnapshotFiles, error) {
	if p == nil {
		return artifact.SnapshotFiles{}, errors.New("snapshot preparer required")
	}
	natsURL := p.cfg.NATSURL
	if natsURL == "" {
		natsURL = os.Getenv("NATS_URL")
	}
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	binary := os.Getenv("WLOW_RUNNER_BINARY")
	if binary == "" {
		binary = "/usr/local/bin/wlow-runner"
	}
	args := []string{
		"--nats", natsURL,
		"--data-dir", p.cfg.DataDir,
		"local-snapshot-cycle",
		"--tenant", p.cfg.Tenant,
		"--id", p.cfg.ProcessorID,
		"--from", p.cfg.SourceRef,
		"--version", p.cfg.Version,
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return artifact.SnapshotFiles{}, fmt.Errorf("runner-rs snapshot prepare failed: %w: %s", err, string(output))
	}
	return snapshotFiles(p.cfg), nil
}

func snapshotFiles(cfg SnapshotPrepareConfig) artifact.SnapshotFiles {
	base := filepath.Join(cfg.DataDir, "snapshots", tenantOrDefault(cfg.Tenant), cfg.ProcessorID, cfg.Version)
	return artifact.SnapshotFiles{
		Config: filepath.Join(base, artifact.SnapshotConfigFile),
		State:  filepath.Join(base, artifact.SnapshotStateFile),
		Memory: filepath.Join(base, artifact.SnapshotMemoryFile),
		Rootfs: filepath.Join(base, artifact.SnapshotRootfsFile),
	}
}

func tenantOrDefault(tenant string) string {
	if tenant == "" {
		return artifact.DefaultTenant
	}
	return tenant
}

func defaultSnapshotRef(cfg SnapshotPrepareConfig) string {
	registry := strings.TrimRight(os.Getenv("WLOW_SNAPSHOT_REGISTRY"), "/")
	if registry == "" {
		registry = strings.TrimRight(os.Getenv("WLOW_REGISTRY"), "/")
	}
	if registry == "" || cfg.ProcessorID == "" || cfg.Version == "" {
		return ""
	}
	return registry + "/" + cfg.ProcessorID + "-snapshot:" + cfg.Version
}
