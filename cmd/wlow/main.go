package main

import (
	"archive/tar"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/artifact"
	"github.com/wlow/wlow/pkg/build"
	"github.com/wlow/wlow/pkg/sandbox"
)

const maxListItems = 256

// Version and BuildTime are injected by GoReleaser at build time via ldflags.
var (
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const usage = `wlow — workflow orchestrator CLI

Usage:
  wlow <command> [flags]

Commands:
  start [--control-plane]  Start the control plane, or serve process/WASM tasks
  new <name>               Scaffold a new processor project
  push                     Build and register a processor artifact
  prepare-snapshot         Prepare snapshot artifacts (run from a KVM host)
  benchmark                Submit timing workflows
  version                  Print version

Control plane:
  wlow start --control-plane          start the workflow orchestrator (needs NATS)
  wlow start --control-plane --nats … override NATS URL

Processor runtimes:
  wlow start                          serve process + WASM tasks (needs NATS)
  wlow start --runtimes wasm          WASM only
  cold-microvm-rootfs                 Firecracker — deploy the wlow runner image on a KVM host
  snapshot-fork-microvm               Firecracker from snapshot — same

Run "wlow <command> --help" for per-command flags.
`

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	switch args[0] {
	case "version", "--version", "-version":
		fmt.Printf("wlow %s (built %s)\n", Version, BuildTime)
		return nil
	case "new":
		return newProcessor(ctx, args[1:])
	case "start":
		return startRunner(ctx, args[1:])
	case "push":
		return push(ctx, args[1:])
	case "prepare-snapshot":
		return prepareSnapshot(ctx, args[1:])
	case "benchmark":
		return benchmark(ctx, args[1:])
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func push(ctx context.Context, args []string) error {
	cfg, err := parsePushConfig(args)
	if err != nil {
		return err
	}
	spec, cleanup, err := buildProcessor(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		return err
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Printf("close nats: %v", err)
		}
	}()
	client, err := artifact.NewClient(nc, 30*time.Second)
	if err != nil {
		return err
	}
	manifest, err := pushSpec(ctx, client, cfg, spec)
	if err != nil {
		return err
	}
	fmt.Printf("%s:%s %s\n", manifest.ProcessorID, manifest.Version, manifest.ArtifactHash)
	if !cfg.Snapshot {
		return nil
	}
	snapshot, err := snapshotAfterPush(ctx, nc, cfg, manifest)
	if err != nil {
		return err
	}
	fmt.Printf("%s:%s %s\n", snapshot.ProcessorID, snapshot.Version, snapshot.ArtifactHash)
	return nil
}

func prepareSnapshot(ctx context.Context, args []string) error {
	cfg, err := parsePrepareSnapshotConfig(args)
	if err != nil {
		return err
	}
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	store, err := artifactStore(ctx, nc)
	if err != nil {
		return err
	}
	manifest, err := prepareSnapshotWithStore(ctx, store, sandbox.SnapshotPrepareConfig{
		Store:       store,
		DataDir:     cfg.DataDir,
		NATSURL:     cfg.NATSURL,
		SnapshotRef: cfg.SnapshotRef,
		Tenant:      cfg.Tenant,
		ProcessorID: cfg.ProcessorID,
		SourceRef:   cfg.SourceRef,
		Version:     cfg.Version,
		Tags:        cfg.Tags,
	})
	if err != nil {
		return err
	}
	fmt.Printf("%s:%s %s\n", manifest.ProcessorID, manifest.Version, manifest.ArtifactHash)
	return nil
}

func snapshotAfterPush(ctx context.Context, nc *nats.Conn, cfg *pushConfig, source *artifact.Manifest) (*artifact.Manifest, error) {
	if cfg.Runtime != artifact.RuntimeMicroVM {
		return nil, errors.New("--snapshot requires --runtime cold-microvm-rootfs")
	}
	store, err := artifactStore(ctx, nc)
	if err != nil {
		return nil, err
	}
	return prepareSnapshotWithStore(ctx, store, sandbox.SnapshotPrepareConfig{
		Store:       store,
		DataDir:     cfg.DataDir,
		NATSURL:     cfg.NATSURL,
		SnapshotRef: cfg.SnapshotImageRef,
		Tenant:      source.Tenant,
		ProcessorID: source.ProcessorID,
		SourceRef:   source.Version,
		Version:     cfg.SnapshotVersion,
		Tags:        cfg.SnapshotTags,
	})
}

func artifactStore(ctx context.Context, nc *nats.Conn) (*artifact.Store, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return artifact.NewStore(ctx, js, artifact.StoreConfig{})
}

func prepareSnapshotWithStore(ctx context.Context, store *artifact.Store, cfg sandbox.SnapshotPrepareConfig) (*artifact.Manifest, error) {
	cfg.Store = store
	preparer, err := sandbox.NewSnapshotPreparer(cfg)
	if err != nil {
		return nil, err
	}
	return preparer.Prepare(ctx)
}

type prepareSnapshotConfig struct {
	NATSURL     string
	Tenant      string
	ProcessorID string
	SourceRef   string
	Version     string
	Tags        []string
	DataDir     string
	SnapshotRef string
}

func parsePrepareSnapshotConfig(args []string) (*prepareSnapshotConfig, error) {
	fs := flag.NewFlagSet("prepare-snapshot", flag.ContinueOnError)
	natsURL := fs.String("nats", "nats://localhost:4222", "NATS URL")
	tenant := fs.String("tenant", artifact.DefaultTenant, "tenant")
	id := fs.String("id", "", "processor id")
	from := fs.String("from", "latest", "source processor ref")
	version := fs.String("version", "snapshot-v1", "snapshot version")
	tags := fs.String("tags", "latest", "comma-separated tags")
	dataDir := fs.String("data-dir", envOr("WLOW_DATA_DIR", "/var/lib/wlow"), "snapshot preparation data dir")
	snapshotRef := fs.String("snapshot-ref", "", "OCI image ref for snapshot data")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return &prepareSnapshotConfig{
		NATSURL:     *natsURL,
		Tenant:      *tenant,
		ProcessorID: *id,
		SourceRef:   *from,
		Version:     *version,
		Tags:        splitList(*tags),
		DataDir:     *dataDir,
		SnapshotRef: *snapshotRef,
	}, nil
}

type pushConfig struct {
	NATSURL          string
	Tenant           string
	ProcessorID      string
	Version          string
	Runtime          artifact.Runtime
	Source           build.SourceKind
	Path             string
	Tags             []string
	Entrypoint       []string
	Deterministic    bool
	Platform         string
	Snapshot         bool
	SnapshotVersion  string
	SnapshotTags     []string
	DataDir          string
	Registry         string
	ImageRef         string
	SnapshotImageRef string
}

func parsePushConfig(args []string) (*pushConfig, error) {
	fs := flag.NewFlagSet("push", flag.ContinueOnError)
	natsURL := fs.String("nats", "nats://localhost:4222", "NATS URL")
	tenant := fs.String("tenant", artifact.DefaultTenant, "tenant")
	id := fs.String("id", "", "processor id")
	version := fs.String("version", "v1", "processor version")
	runtime := fs.String("runtime", "wasm", "runtime")
	source := fs.String("source", "", "source kind: wasm,dockerfile,tarball,binary")
	path := fs.String("path", "", "source path")
	tags := fs.String("tags", "latest", "comma-separated tags")
	entrypoint := fs.String("entrypoint", "", "comma-separated entrypoint")
	deterministic := fs.Bool("deterministic", true, "use deterministic WIT world")
	platform := fs.String("platform", "", "Docker build platform")
	snapshot := fs.Bool("snapshot", false, "prepare and publish a snapshot-backed processor after pushing")
	snapshotVersion := fs.String("snapshot-version", "snapshot-v1", "snapshot processor version")
	snapshotTags := fs.String("snapshot-tags", "latest", "comma-separated tags for the snapshot-backed version")
	dataDir := fs.String("data-dir", envOr("WLOW_DATA_DIR", "/var/lib/wlow"), "data dir for snapshot preparation")
	registry := fs.String("registry", envOr("WLOW_REGISTRY", ""), "OCI registry/repository prefix for microVM images")
	imageRef := fs.String("image-ref", "", "full OCI image ref override")
	snapshotRef := fs.String("snapshot-ref", "", "OCI image ref for snapshot data")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return &pushConfig{
		NATSURL:          *natsURL,
		Tenant:           *tenant,
		ProcessorID:      *id,
		Version:          *version,
		Runtime:          artifact.Runtime(*runtime),
		Source:           inferKind(*runtime, *source),
		Path:             *path,
		Tags:             splitList(*tags),
		Entrypoint:       splitList(*entrypoint),
		Deterministic:    *deterministic,
		Platform:         *platform,
		Snapshot:         *snapshot,
		SnapshotVersion:  *snapshotVersion,
		SnapshotTags:     splitList(*snapshotTags),
		DataDir:          *dataDir,
		Registry:         *registry,
		ImageRef:         *imageRef,
		SnapshotImageRef: *snapshotRef,
	}, nil
}

func buildProcessor(ctx context.Context, cfg *pushConfig) (*build.Spec, func(), error) {
	if cfg == nil {
		return nil, nil, errors.New("push config required")
	}
	cleanup := func() {}
	outputPath := ""
	imageRef := ""
	if usesRegistryLayout(cfg) {
		ref, err := processorImageRef(cfg)
		if err != nil {
			return nil, nil, err
		}
		imageRef = ref
		if cfg.Runtime == artifact.RuntimeMicroVM {
			dir, err := os.MkdirTemp("", "wlow-push-*")
			if err != nil {
				return nil, nil, fmt.Errorf("create temp dir: %w", err)
			}
			cleanup = func() {
				if err := os.RemoveAll(dir); err != nil {
					log.Printf("remove temp dir: %v", err)
				}
			}
			outputPath = filepath.Join(dir, "rootfs.tar")
		}
	} else if usesObjectUpload(cfg) {
		dir, err := os.MkdirTemp("", "wlow-push-*")
		if err != nil {
			return nil, nil, fmt.Errorf("create temp dir: %w", err)
		}
		cleanup = func() {
			if err := os.RemoveAll(dir); err != nil {
				log.Printf("remove temp dir: %v", err)
			}
		}
		outputPath = filepath.Join(dir, "rootfs.tar")
	}
	spec, err := build.Build(ctx, build.Options{
		Kind:          cfg.Source,
		Path:          cfg.Path,
		Runtime:       cfg.Runtime,
		ProcessorID:   cfg.ProcessorID,
		Version:       cfg.Version,
		Tags:          cfg.Tags,
		Entrypoint:    cfg.Entrypoint,
		Deterministic: cfg.Deterministic,
		Platform:      cfg.Platform,
		OutputPath:    outputPath,
		ImageRef:      imageRef,
	})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return spec, cleanup, nil
}

func usesObjectUpload(_ *pushConfig) bool {
	return false
}

func usesRegistryLayout(cfg *pushConfig) bool {
	return cfg.Source == build.SourceDockerfile &&
		(cfg.Runtime == artifact.RuntimeMicroVM)
}

func processorImageRef(cfg *pushConfig) (string, error) {
	if cfg.ImageRef != "" {
		return cfg.ImageRef, nil
	}
	if cfg.Registry == "" {
		return "", errors.New("dockerfile microvm push requires --image-ref or --registry/WLOW_REGISTRY")
	}
	return strings.TrimRight(cfg.Registry, "/") + "/" + cfg.ProcessorID + ":" + cfg.Version, nil
}

func pushSpec(ctx context.Context, client *artifact.Client, cfg *pushConfig, spec *build.Spec) (*artifact.Manifest, error) {
	opts := artifact.PushOptions{
		Tenant:      cfg.Tenant,
		ProcessorID: cfg.ProcessorID,
		Version:     cfg.Version,
		Tags:        spec.Tags,
		Manifest:    spec.Manifest,
	}
	if spec.Path != "" && cfg.Runtime == artifact.RuntimeMicroVM {
		return pushEROFSRootfs(ctx, client, cfg, spec)
	}
	if cfg.Runtime == artifact.RuntimeMicroVM {
		ref, ok := spec.Manifest.RuntimeConfig["image_ref"].(string)
		if !ok || ref == "" {
			return nil, errors.New("microvm image_ref missing after build")
		}
		manifest, err := artifact.InspectRemoteOCI(ctx, ref, opts)
		if err != nil {
			return nil, err
		}
		if err := client.PutManifest(ctx, manifest, opts.Tags); err != nil {
			return nil, err
		}
		return manifest, nil
	}
	return client.Push(ctx, spec.Data, opts)
}

func pushEROFSRootfs(ctx context.Context, client *artifact.Client, cfg *pushConfig, spec *build.Spec) (*artifact.Manifest, error) {
	if client == nil {
		return nil, errors.New("artifact client required")
	}
	if cfg == nil || spec == nil {
		return nil, errors.New("push config and spec required")
	}
	ref, err := processorImageRef(cfg)
	if err != nil {
		return nil, err
	}
	rootfs, cleanup, err := prepareEROFSRootfs(ctx, cfg, spec.Path)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	remote, hash, size, err := artifact.PushRootfsImage(ctx, ref, rootfs)
	if err != nil {
		return nil, err
	}
	manifest := rootfsManifest(cfg, spec, remote, hash, size)
	if err := client.PutManifest(ctx, manifest, spec.Tags); err != nil {
		return nil, err
	}
	return manifest, nil
}

func prepareEROFSRootfs(ctx context.Context, cfg *pushConfig, tarPath string) (string, func(), error) {
	if ctx == nil {
		return "", func() {}, errors.New("context required")
	}
	if cfg == nil || tarPath == "" {
		return "", func() {}, errors.New("push config and rootfs tar required")
	}
	dir, err := os.MkdirTemp("", "wlow-erofs-*")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("remove erofs dir: %v", err)
		}
	}
	staging := filepath.Join(dir, "staging")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		cleanup()
		return "", func() {}, err
	}
	if err := unpackTar(tarPath, staging); err != nil {
		cleanup()
		return "", func() {}, err
	}
	if err := injectAgent(staging); err != nil {
		cleanup()
		return "", func() {}, err
	}
	rootfs := filepath.Join(dir, "rootfs.erofs")
	if err := makeEROFS(ctx, staging, rootfs); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return rootfs, cleanup, nil
}

func rootfsManifest(cfg *pushConfig, spec *build.Spec, remote *artifact.RemoteRef, hash string, size int64) *artifact.Manifest {
	runtimeCfg := copyRuntimeConfig(spec.Manifest.RuntimeConfig)
	runtimeCfg["rootfs_format"] = "erofs"
	runtimeCfg["rootfs_ref"] = remote.Ref
	m := &artifact.Manifest{
		Kind:            artifact.ManifestKind,
		Tenant:          cfg.Tenant,
		ProcessorID:     cfg.ProcessorID,
		Version:         cfg.Version,
		Runtime:         artifact.RuntimeMicroVM,
		IOProtocol:      spec.Manifest.IOProtocol,
		RuntimeConfig:   runtimeCfg,
		HashAlgorithm:   artifact.HashAlgorithmOCI,
		ArtifactHash:    hash,
		ArtifactSize:    size,
		Artifacts:       map[string]artifact.Artifact{artifact.RoleRootfs: {Kind: artifact.KindRemoteObject, Remote: remote}},
		Deterministic:   spec.Manifest.Deterministic,
		ResourceHints:   spec.Manifest.ResourceHints,
		BuildProvenance: buildProvenance(spec.Manifest.BuildProvenance, map[string]string{"rootfs_format": "erofs"}),
		Cache:           spec.Manifest.Cache,
		CreatedAt:       time.Now().UTC(),
	}
	return m
}

func unpackTar(tarPath string, dest string) error {
	if tarPath == "" || dest == "" {
		return errors.New("tar path and destination required")
	}
	file, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := file.Close(); err != nil {
			slog.Default().Error("close tar file failed", "error", err)
		}
	}()
	tr := tar.NewReader(file)
	const maxEntries = 1 << 18
	for count := 0; count < maxEntries; count++ {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := writeTarEntry(dest, header, tr); err != nil {
			return err
		}
	}
	return errors.New("rootfs tar entry limit exceeded")
}

func writeTarEntry(dest string, header *tar.Header, tr *tar.Reader) error {
	if header == nil || tr == nil {
		return errors.New("tar entry required")
	}
	target, err := safeArchivePath(dest, header.Name)
	if err != nil {
		return err
	}
	switch header.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(target, os.FileMode(header.Mode)&0o777)
	case tar.TypeReg:
		return writeTarFile(target, header, tr)
	case tar.TypeSymlink:
		return writeTarSymlink(dest, target, header.Linkname)
	default:
		return nil
	}
}

func writeTarFile(target string, header *tar.Header, tr *tar.Reader) error {
	if target == "" || header == nil || tr == nil {
		return errors.New("tar file target required")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode)&0o777)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, tr)
	return err
}

func writeTarSymlink(base string, target string, link string) error {
	if base == "" || target == "" {
		return errors.New("symlink target required")
	}
	if link == "" {
		return errors.New("symlink link required")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	return os.Symlink(link, target)
}

func injectAgent(staging string) error {
	agent := envOr("WLOW_AGENT_BINARY", "/usr/local/bin/wlow-agent")
	if staging == "" || agent == "" {
		return errors.New("staging and agent required")
	}
	sbin := filepath.Join(staging, "sbin")
	if err := os.MkdirAll(sbin, 0o755); err != nil {
		return err
	}
	dest := filepath.Join(sbin, "wlow-agent")
	if err := copyFile(agent, dest, 0o755); err != nil {
		return fmt.Errorf("inject agent: %w", err)
	}
	return nil
}

func makeEROFS(ctx context.Context, staging string, output string) error {
	if ctx == nil {
		return errors.New("context required")
	}
	if staging == "" || output == "" {
		return errors.New("staging and output required")
	}
	mkfs := envOr("WLOW_MKFS_EROFS", "mkfs.erofs")
	cmd := exec.CommandContext(ctx, mkfs, "--quiet", "-x-1", "-E", "^inline_data", output, staging)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func copyFile(src string, dest string, mode os.FileMode) error {
	if src == "" || dest == "" {
		return errors.New("source and destination required")
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := in.Close(); err != nil {
			slog.Default().Error("close source file failed", "error", err)
		}
	}()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil {
			slog.Default().Error("close destination file failed", "error", err)
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return os.Chmod(dest, mode)
}

func safeArchivePath(base, rel string) (string, error) {
	if base == "" || rel == "" {
		return "", errors.New("archive base and path required")
	}
	clean := filepath.Clean("/" + rel)
	joined := filepath.Join(base, clean)
	out, err := filepath.Rel(base, joined)
	if err != nil {
		return "", err
	}
	if out == ".." || strings.HasPrefix(out, "../") {
		return "", fmt.Errorf("archive entry escapes output: %s", rel)
	}
	return joined, nil
}

func copyRuntimeConfig(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+2)
	for key, value := range in {
		out[key] = value
	}
	return out
}

func buildProvenance(existing map[string]string, extra map[string]string) map[string]string {
	out := make(map[string]string, len(existing)+len(extra))
	for key, value := range existing {
		out[key] = value
	}
	for key, value := range extra {
		out[key] = value
	}
	return out
}

func inferKind(runtime, source string) build.SourceKind {
	if source != "" {
		return build.SourceKind(source)
	}
	switch runtime {
	case string(artifact.RuntimeMicroVM), string(artifact.RuntimeSnapshot):
		return build.SourceTarball
	case string(artifact.RuntimeProcess):
		return build.SourceBinary
	default:
		return build.SourceWasm
	}
}

func splitList(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for idx := 0; idx < len(parts) && idx < maxListItems; idx++ {
		part := strings.TrimSpace(parts[idx])
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
