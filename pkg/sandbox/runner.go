package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/artifact"
	wlownats "github.com/wlow/wlow/pkg/nats"
	"github.com/wlow/wlow/pkg/workflow"
)

const (
	// DefaultStreamName is the default NATS JetStream name for processor tasks.
	DefaultStreamName = "WLOW_PROCESSOR"
	// DefaultSubjectPrefix is the default NATS subject prefix for processor tasks.
	DefaultSubjectPrefix = "wlow.processor"
	// DefaultFilterSubject is the default NATS subject filter for sandbox tasks.
	DefaultFilterSubject = DefaultSubjectPrefix + ".sandbox.>"
)

// RunnerConfig contains the configuration for a sandbox Runner.
type RunnerConfig struct {
	Client                 *wlownats.Client
	Store                  workflow.Store
	Artifacts              *artifact.Store
	Executors              *ExecutorRegistry
	OutputCache            workflow.OutputCache
	Locality               *workflow.LocalityScheduler
	NodeID                 string
	Runtimes               []artifact.Runtime
	DataDir                string
	Concurrency            int
	StreamName             string
	ConsumerName           string
	FilterSubject          string
	ProcessorSubjectPrefix string
	MaxRetries             int
	AckTimeout             time.Duration
	HeartbeatInterval      time.Duration
	WorkflowSubjectPrefix  string
	Logger                 *slog.Logger
}

// Runner listens for and executes sandboxed tasks.
type Runner struct {
	cfg RunnerConfig
	log *slog.Logger
}

type runnerConsumeContext struct {
	jetstream.ConsumeContext
	cancel context.CancelFunc
}

// NewRunner creates a new sandbox Runner.
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	if cfg.Client == nil {
		return nil, errors.New("nats client required")
	}
	if cfg.Store == nil {
		return nil, errors.New("workflow store required")
	}
	if cfg.Artifacts == nil {
		return nil, errors.New("artifact store required")
	}
	if cfg.Executors == nil {
		if len(cfg.Runtimes) == 0 {
			return nil, errors.New("runtimes required when executors not provided")
		}
		executors, err := RegistryFor("", cfg.Runtimes...)
		if err != nil {
			return nil, err
		}
		cfg.Executors = executors
	}
	cfg = cfg.withDefaults()
	return &Runner{cfg: cfg, log: cfg.Logger}, nil
}

// Start begins consuming tasks from NATS.
func (r *Runner) Start(ctx context.Context) (jetstream.ConsumeContext, error) {
	stream, err := r.cfg.Client.CreateStream(ctx, streamConfig(r.cfg))
	if err != nil {
		return nil, err
	}
	consumer, err := r.cfg.Client.CreateConsumer(ctx, consumerConfig(r.cfg, stream))
	if err != nil {
		return nil, err
	}
	if r.cfg.Locality != nil && r.cfg.NodeID != "" {
		go r.heartbeat(ctx)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	taskCh := make(chan jetstream.Msg, r.cfg.Concurrency)
	r.startWorkers(workerCtx, taskCh)
	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		select {
		case taskCh <- msg:
		case <-workerCtx.Done():
			_ = msg.Nak()
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return runnerConsumeContext{ConsumeContext: consumeCtx, cancel: cancel}, nil
}

func (c runnerConsumeContext) Stop() {
	c.cancel()
	c.ConsumeContext.Stop()
}

func (r *Runner) startWorkers(ctx context.Context, taskCh <-chan jetstream.Msg) {
	for idx := 0; idx < r.cfg.Concurrency; idx++ {
		go r.worker(ctx, taskCh)
	}
}

func (r *Runner) worker(ctx context.Context, taskCh <-chan jetstream.Msg) {
	const maxTasks = 1 << 30
	for count := 0; count < maxTasks; count++ {
		select {
		case <-ctx.Done():
			return
		case msg := <-taskCh:
			r.HandleTask(msg)
		}
	}
}

func (r *Runner) heartbeat(ctx context.Context) {
	interval := r.cfg.HeartbeatInterval
	t := time.NewTicker(interval)
	defer t.Stop()
	const maxTicks = 1 << 30
	for tick := 0; tick < maxTicks; tick++ {
		if err := r.publishInventory(ctx); err != nil {
			r.log.Warn("locality heartbeat failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Runner) publishInventory(ctx context.Context) error {
	warm := make([]string, 0, len(r.cfg.Runtimes))
	for idx := 0; idx < len(r.cfg.Runtimes); idx++ {
		warm = append(warm, "runtime:"+string(r.cfg.Runtimes[idx]))
	}
	warm = append(warm, r.cachedPlacementKeys()...)
	return r.cfg.Locality.PutInventory(ctx, workflow.NodeInventory{
		NodeID:      r.cfg.NodeID,
		Warm:        warm,
		Concurrency: r.cfg.Concurrency,
	})
}

func (r *Runner) cachedPlacementKeys() []string {
	roots := []string{
		filepath.Join(r.dataDir(), "oci-cache"),
		filepath.Join(r.dataDir(), "snapshots"),
	}
	keys := make([]string, 0, 64)
	for idx := 0; idx < len(roots); idx++ {
		keys = append(keys, cacheKeysInDir(roots[idx])...)
	}
	return keys
}

func cacheKeysInDir(root string) []string {
	keys := make([]string, 0, 64)
	stack := []string{root}
	const maxEntries = 4096
	for count := 0; count < maxEntries && len(stack) > 0; count++ {
		dir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for idx := 0; idx < len(entries) && idx < maxEntries; idx++ {
			path := filepath.Join(dir, entries[idx].Name())
			if entries[idx].IsDir() {
				stack = append(stack, path)
				continue
			}
			digest := digestFromCacheName(entries[idx].Name())
			if digest != "" {
				keys = append(keys, "oci:"+digest, "remote:"+digest)
			}
		}
	}
	return keys
}

func digestFromCacheName(name string) string {
	value := strings.TrimPrefix(name, "sha256:")
	value = strings.TrimPrefix(value, "sha256-")
	if len(value) != 64 {
		return ""
	}
	return "sha256:" + value
}

// HandleTask processes a single NATS message as a workflow task.
func (r *Runner) HandleTask(msg jetstream.Msg) {
	var t workflow.Task
	if err := json.Unmarshal(msg.Data(), &t); err != nil {
		r.log.Error("sandbox unmarshal failed", "error", err)
		_ = msg.Nak()
		return
	}
	result := r.execute(context.Background(), &t)
	if shouldRetry(msg, result, r.cfg.MaxRetries) {
		_ = msg.Nak()
		return
	}
	_ = r.publishResult(context.Background(), result)
	_ = msg.Ack()
}

func (r *Runner) execute(ctx context.Context, t *workflow.Task) *workflow.TaskResult {
	processorID, ref := t.ProcessorRef()
	result := &workflow.TaskResult{WorkflowID: t.WorkflowID, TaskID: t.ID, ProcessorID: processorID}
	_ = r.cfg.Store.StoreTaskState(ctx, t.WorkflowID, t.ID, runningResult(result))
	manifest, err := r.cfg.Artifacts.Resolve(ctx, t.TenantID(), processorID, ref)
	if err != nil {
		return r.storeFailure(ctx, result, err)
	}
	cacheKey, cached, err := r.cachedResult(ctx, t, manifest, processorID, ref)
	if err != nil {
		return r.storeFailure(ctx, result, err)
	}
	if cached != nil {
		return cached
	}
	req, cleanup, err := r.executeRequest(ctx, manifest, t)
	if err != nil {
		return r.storeFailure(ctx, result, err)
	}
	defer cleanup()
	executor, err := r.cfg.Executors.Get(manifest.RuntimeValue())
	if err != nil {
		return r.storeFailure(ctx, result, err)
	}
	out, err := executor.Execute(ctx, req)
	if err != nil {
		return r.storeFailure(ctx, result, err)
	}
	result.Status = workflow.StatusCompleted
	result.Output = out.Output
	if cacheKey != "" && r.cfg.OutputCache != nil {
		_ = r.cfg.OutputCache.Put(ctx, cacheKey, result)
	}
	_ = r.cfg.Store.StoreTaskState(ctx, t.WorkflowID, t.ID, result)
	return result
}

func (r *Runner) executeRequest(ctx context.Context, manifest *artifact.Manifest, t *workflow.Task) (ExecuteRequest, func(), error) {
	req := ExecuteRequest{Manifest: manifest, Input: t.Input}
	if manifest.RuntimeValue() == artifact.RuntimeSnapshot {
		return r.snapshotExecuteRequest(ctx, manifest, req)
	}
	// Direct-command process processors store their entrypoint in runtime_config.
	// No artifact bytes exist or are needed — the process executor reads cmd directly.
	if manifest.RuntimeValue() == artifact.RuntimeProcess && artifact.HasDirectCommand(manifest.RuntimeConfig) {
		return req, func() {}, nil
	}
	bytes, err := r.cfg.Artifacts.FetchArtifact(ctx, manifest)
	if err != nil {
		return req, func() {}, err
	}
	req.Bytes = bytes
	return req, func() {}, nil
}

func (r *Runner) snapshotExecuteRequest(ctx context.Context, manifest *artifact.Manifest, req ExecuteRequest) (ExecuteRequest, func(), error) {
	dir, err := os.MkdirTemp("", "wlow-snapshot-*")
	if err != nil {
		return req, func() {}, err
	}
	snapshotDir := filepath.Join(dir, "snapshot")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		_ = os.RemoveAll(dir)
		return req, func() {}, err
	}
	if err := r.materializeSnapshot(ctx, manifest, snapshotDir); err != nil {
		_ = os.RemoveAll(dir)
		return req, func() {}, err
	}
	req.Snapshot = &SnapshotRootfs{Dir: snapshotDir, RootfsPath: filepath.Join(snapshotDir, artifact.SnapshotRootfsFile)}
	return req, func() { _ = os.RemoveAll(dir) }, err
}

func (r *Runner) materializeSnapshot(ctx context.Context, manifest *artifact.Manifest, dir string) error {
	objects := artifact.ManifestSnapshotObjects(manifest)
	files := artifact.SnapshotObjectFiles(objects)
	for name, ref := range files {
		if ref == nil {
			return fmt.Errorf("snapshot remote object %s required", name)
		}
		dest := filepath.Join(dir, name)
		if err := artifact.PullRemoteFile(ctx, ref, dest); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) dataDir() string {
	if r.cfg.DataDir != "" {
		return r.cfg.DataDir
	}
	return "/var/lib/wlow"
}

func (r *Runner) cachedResult(ctx context.Context, t *workflow.Task, m *artifact.Manifest, processorID, ref string) (string, *workflow.TaskResult, error) {
	if r.cfg.OutputCache == nil || !m.Deterministic {
		return "", nil, nil
	}
	key, err := workflow.OutputCacheKey(processorID, ref, t.Input)
	if err != nil {
		return "", nil, err
	}
	cached, ok, err := r.cfg.OutputCache.Get(ctx, key)
	if err != nil || !ok {
		return key, nil, err
	}
	cached.WorkflowID = t.WorkflowID
	cached.TaskID = t.ID
	cached.ProcessorID = processorID
	_ = r.cfg.Store.StoreTaskState(ctx, t.WorkflowID, t.ID, cached)
	return key, cached, nil
}

func (r *Runner) storeFailure(ctx context.Context, result *workflow.TaskResult, err error) *workflow.TaskResult {
	failed := failedResult(result, err)
	_ = r.cfg.Store.StoreTaskState(ctx, result.WorkflowID, result.TaskID, failed)
	return failed
}

func (r *Runner) publishResult(ctx context.Context, result *workflow.TaskResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = r.cfg.Client.Publish(ctx, workflow.ResultSubject(r.cfg.WorkflowSubjectPrefix, result.TaskID), data)
	return err
}

func (cfg RunnerConfig) withDefaults() RunnerConfig {
	if cfg.StreamName == "" {
		cfg.StreamName = DefaultStreamName
	}
	if cfg.ProcessorSubjectPrefix == "" {
		cfg.ProcessorSubjectPrefix = DefaultSubjectPrefix
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = "sandbox-" + cfg.NodeID
		if cfg.NodeID == "" {
			cfg.ConsumerName = "sandbox-runtime"
		}
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 5 * time.Minute
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 15 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default().With("component", "sandbox")
	}
	return cfg
}

func streamConfig(cfg RunnerConfig) wlownats.StreamConfig {
	return wlownats.StreamConfig{
		Name:      cfg.StreamName,
		Subjects:  []string{cfg.StreamName + ".>"},
		Retention: jetstream.WorkQueuePolicy,
	}
}

func consumerConfig(cfg RunnerConfig, stream jetstream.Stream) wlownats.ConsumerConfig {
	out := wlownats.ConsumerConfig{
		Name:          cfg.ConsumerName,
		Stream:        stream.CachedInfo().Config.Name,
		MaxDeliver:    cfg.MaxRetries,
		MaxAckPending: cfg.Concurrency,
		AckWait:       cfg.AckTimeout,
	}
	if len(cfg.Runtimes) > 0 {
		const maxRuntimes = 64
		count := len(cfg.Runtimes)
		if count > maxRuntimes {
			count = maxRuntimes
		}
		subjects := make([]string, 0, count*2)
		for idx := 0; idx < count; idx++ {
			subjects = append(subjects, fmt.Sprintf("%s.sandbox.%s.>", cfg.ProcessorSubjectPrefix, cfg.Runtimes[idx]))
			if cfg.NodeID != "" {
				subjects = append(subjects, fmt.Sprintf("%s.node.%s.sandbox.%s.>", cfg.ProcessorSubjectPrefix, cfg.NodeID, cfg.Runtimes[idx]))
			}
		}
		out.FilterSubjects = subjects
		return out
	}
	if cfg.FilterSubject == "" {
		out.FilterSubject = DefaultFilterSubject
	} else {
		out.FilterSubject = cfg.FilterSubject
	}
	return out
}

func runningResult(result *workflow.TaskResult) *workflow.TaskResult {
	cp := *result
	cp.Status = workflow.StatusRunning
	return &cp
}

func failedResult(result *workflow.TaskResult, err error) *workflow.TaskResult {
	result.Status = workflow.StatusFailed
	result.ErrorMessage = err.Error()
	return result
}

func shouldRetry(msg jetstream.Msg, result *workflow.TaskResult, maxRetries int) bool {
	if result.Status != workflow.StatusFailed && result.Status != workflow.StatusTimedOut {
		return false
	}
	meta, err := msg.Metadata()
	return err == nil && meta != nil && int(meta.NumDelivered) < maxRetries
}
