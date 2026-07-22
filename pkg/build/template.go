package build

import (
	"context"
	"errors"
	"time"

	"github.com/wlow/wlow/pkg/artifact"
)

// WarmTemplateConfig defines the configuration for building a microVM warm template.
type WarmTemplateConfig struct {
	RootfsHash     string
	Warmup         []string
	ReadyText      string
	Timeout        time.Duration
	BeforeSnapshot []string
	AfterRestore   []string
}

// WarmTemplate is a stub for building a microVM warm template.
// Deprecated: Use runner-rs for template builds.
func WarmTemplate(ctx context.Context, _ any, _ string, cfg WarmTemplateConfig) (*artifact.Manifest, error) {
	if ctx == nil || cfg.RootfsHash == "" {
		return nil, errors.New("context and rootfs hash required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return nil, errors.New("go cloud hypervisor warm template build removed; use runner-rs")
}
