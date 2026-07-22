package build

import (
	"context"
	"errors"

	"github.com/wlow/wlow/pkg/artifact"
)

// SourceKind defines the type of source code or artifact being built.
type SourceKind string

const (
	// SourceWasm indicates a WebAssembly component source.
	SourceWasm SourceKind = "wasm"
	// SourceDockerfile indicates a Dockerfile source for MicroVM rootfs.
	SourceDockerfile SourceKind = "dockerfile"
	// SourceTarball indicates a rootfs tarball source.
	SourceTarball SourceKind = "tarball"
	// SourceBinary indicates a native binary executable source.
	SourceBinary SourceKind = "binary"
)

// Options contains parameters for the build process.
type Options struct {
	Kind          SourceKind
	Path          string
	Runtime       artifact.Runtime
	ProcessorID   string
	Version       string
	Tags          []string
	Entrypoint    []string
	WorkDir       string
	Env           map[string]string
	Deterministic bool
	BuildSecrets  []string
	Platform      string
	OutputPath    string
	ImageRef      string
}

// Spec contains the result of a build, including the manifest and artifact data.
type Spec struct {
	Data     []byte
	Path     string
	Manifest artifact.Manifest
	Tags     []string
}

// Adapter is the interface for different build implementations.
type Adapter interface {
	Build(ctx context.Context, opts Options) (*Spec, error)
}

// Build executes a build based on the provided options.
func Build(ctx context.Context, opts Options) (*Spec, error) {
	if opts.ProcessorID == "" || opts.Version == "" {
		return nil, errors.New("processor id and version required")
	}
	switch opts.Kind {
	case SourceWasm:
		return WasmAdapter{}.Build(ctx, opts)
	case SourceDockerfile:
		return DockerfileAdapter{}.Build(ctx, opts)
	case SourceTarball:
		return TarballAdapter{}.Build(ctx, opts)
	case SourceBinary:
		return BinaryAdapter{}.Build(ctx, opts)
	default:
		return nil, errors.New("unsupported build source")
	}
}
