package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/wlow/wlow/pkg/artifact"
)

const artifactArg = "{artifact}"

// ProcessExecutor executes tasks as local operating system processes.
type ProcessExecutor struct{}

// ProcessConfig defines the configuration for a process-based task.
type ProcessConfig struct {
	Command string            `json:"cmd"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	WorkDir string            `json:"workdir,omitempty"`
}

// NewProcessExecutor creates a new process executor.
func NewProcessExecutor() *ProcessExecutor {
	return &ProcessExecutor{}
}

// Runtime returns the process runtime identifier.
func (e *ProcessExecutor) Runtime() artifact.Runtime {
	return artifact.RuntimeProcess
}

// Execute runs the process and returns the results.
func (e *ProcessExecutor) Execute(ctx context.Context, req ExecuteRequest) (*ExecuteResult, error) {
	if req.Manifest == nil {
		return nil, errors.New("manifest required")
	}
	if req.Manifest.IOProtocolValue() != artifact.IOProtocolJSONStdio {
		return nil, errors.New("unsupported process io protocol: " + string(req.Manifest.IOProtocolValue()))
	}
	cfg, err := decodeProcessConfig(req.Manifest.RuntimeConfig)
	if err != nil {
		return nil, err
	}
	if len(req.Bytes) == 0 {
		// Direct-command processor: no artifact to write, command is fully
		// specified in runtime_config. Pass an empty artifact path.
		return runProcess(ctx, req, cfg, "")
	}
	dir, path, err := writeArtifact(req.Bytes)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	return runProcess(ctx, req, cfg, path)
}

func decodeProcessConfig(raw map[string]any) (*ProcessConfig, error) {
	if len(raw) == 0 {
		return nil, errors.New("process runtime_config required")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var cfg ProcessConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Command == "" {
		return nil, errors.New("process cmd required")
	}
	return &cfg, nil
}

func writeArtifact(data []byte) (string, string, error) {
	if len(data) == 0 {
		return "", "", errors.New("artifact bytes required")
	}
	dir, err := os.MkdirTemp("", "wlow-process-*")
	if err != nil {
		return "", "", err
	}
	path := filepath.Join(dir, "artifact")
	if err := os.WriteFile(path, data, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return "", "", err
	}
	return dir, path, nil
}

func runProcess(ctx context.Context, req ExecuteRequest, cfg *ProcessConfig, artifactPath string) (*ExecuteResult, error) {
	ctx, cancel := processContext(ctx, req.Manifest.ResourceHints.Timeout)
	defer cancel()
	args := expandArgs(cfg.Args, artifactPath)
	command := cfg.Command
	if command == artifactArg {
		command = artifactPath
	}
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = cfg.WorkDir
	cmd.Env = processEnv(cfg.Env, artifactPath)
	input, err := json.Marshal(req.Input)
	if err != nil {
		return nil, err
	}
	cmd.Stdin = bytes.NewReader(input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, processError(ctx, err, stderr.String())
	}
	return decodeProcessOutput(stdout.Bytes())
}

func processContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func expandArgs(args []string, artifactPath string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == artifactArg {
			out = append(out, artifactPath)
			continue
		}
		out = append(out, arg)
	}
	return out
}

func processEnv(env map[string]string, artifactPath string) []string {
	out := os.Environ()
	if artifactPath != "" {
		out = append(out, "WLOW_ARTIFACT="+artifactPath)
	}
	for key, value := range env {
		out = append(out, key+"="+value)
	}
	return out
}

func processError(ctx context.Context, err error, stderr string) error {
	if ctx.Err() == context.DeadlineExceeded {
		return errors.New("process timeout")
	}
	if stderr != "" {
		return fmt.Errorf("%w: %s", err, stderr)
	}
	return err
}

func decodeProcessOutput(data []byte) (*ExecuteResult, error) {
	var out map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &out); err != nil {
		return nil, fmt.Errorf("decode process stdout: %w", err)
	}
	return &ExecuteResult{Output: out}, nil
}
