package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/nats"
	"github.com/wlow/wlow/pkg/workflow"
)

// RunnerConfig contains the configuration for a processor SDK runner.
type RunnerConfig struct {
	NATSUrl     string
	ProcessorID string
	Subjects    []string
	StreamName  string
	Concurrency int
	AckTimeout  time.Duration
	MaxRetries  int
	StoreBucket string
	Logger      *slog.Logger
}

// DefaultRunnerConfig returns the default configuration for a runner.
func DefaultRunnerConfig() RunnerConfig {
	return RunnerConfig{
		NATSUrl:     "nats://localhost:4222",
		StreamName:  "PROCESSOR",
		Concurrency: 2,
		AckTimeout:  5 * time.Minute,
		MaxRetries:  3,
		StoreBucket: "workflow-state",
		Logger:      slog.Default(),
	}
}

// Runner manages the execution of a processor by listening to NATS.
type Runner struct {
	cfg      RunnerConfig
	handler  Handler
	client   *nats.Client
	store    *nats.Store
	log      *slog.Logger
	active   map[string]context.CancelFunc
	mu       sync.RWMutex
	inFlight chan struct{}
	stop     chan struct{}
}

// NewRunner creates a runner from a Handler.
func NewRunner(cfg RunnerConfig, h Handler) (*Runner, error) {
	if cfg.ProcessorID == "" {
		return nil, fmt.Errorf("processor id required")
	}
	if len(cfg.Subjects) == 0 {
		return nil, fmt.Errorf("subjects required")
	}

	d := DefaultRunnerConfig()
	if cfg.Logger == nil {
		cfg.Logger = d.Logger
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = d.Concurrency
	}
	if cfg.StoreBucket == "" {
		cfg.StoreBucket = d.StoreBucket
	}
	if cfg.StreamName == "" {
		cfg.StreamName = d.StreamName
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = d.AckTimeout
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = d.MaxRetries
	}
	if cfg.NATSUrl == "" {
		cfg.NATSUrl = d.NATSUrl
	}

	return &Runner{
		cfg:      cfg,
		handler:  h,
		log:      cfg.Logger.With("processor", cfg.ProcessorID),
		active:   make(map[string]context.CancelFunc),
		inFlight: make(chan struct{}, cfg.Concurrency),
		stop:     make(chan struct{}),
	}, nil
}

// NewRunnerFor creates a runner for a typed processor.
func NewRunnerFor[In, Out any](cfg RunnerConfig, p Processor[In, Out]) (*Runner, error) {
	return NewRunner(cfg, Wrap(p))
}

// Run starts the runner and blocks until it is stopped or a signal is received.
func (r *Runner) Run(ctx context.Context) error {
	client, err := nats.NewClient(nats.Config{URL: r.cfg.NATSUrl})
	if err != nil {
		return err
	}
	r.client = client
	defer client.Close()

	store, err := nats.NewStore(ctx, client, nats.StoreConfig{Bucket: r.cfg.StoreBucket})
	if err != nil {
		return err
	}
	r.store = store

	stream, err := client.CreateStream(ctx, nats.StreamConfig{
		Name:       r.cfg.StreamName,
		Subjects:   []string{r.cfg.StreamName + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		MaxBytes:   1 << 30,
		Duplicates: 20 * time.Minute,
	})
	if err != nil {
		return err
	}

	var consumers []jetstream.Consumer
	for _, subj := range r.cfg.Subjects {
		name := fmt.Sprintf("%s-%s", r.cfg.ProcessorID, sanitize(subj))
		c, err := client.CreateConsumer(ctx, nats.ConsumerConfig{
			Name:          name,
			Stream:        stream.CachedInfo().Config.Name,
			FilterSubject: subj,
			MaxDeliver:    r.cfg.MaxRetries,
			AckWait:       r.cfg.AckTimeout,
		})
		if err != nil {
			return err
		}
		consumers = append(consumers, c)
	}

	r.log.Info("started", "subjects", r.cfg.Subjects, "concurrency", r.cfg.Concurrency)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ctx.Done():
			r.shutdown()
			return nil
		case <-sig:
			r.shutdown()
			return nil
		case <-r.stop:
			r.shutdown()
			return nil
		default:
			if len(r.inFlight) >= cap(r.inFlight) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			for _, c := range consumers {
				msgs, _ := c.FetchNoWait(1)
				for msg := range msgs.Messages() {
					r.inFlight <- struct{}{}
					go func(m jetstream.Msg) {
						defer func() { <-r.inFlight }()
						r.handle(m)
					}(msg)
				}
			}
		}
	}
}

// Stop sends a signal to stop the runner.
func (r *Runner) Stop() { close(r.stop) }

func (r *Runner) shutdown() {
	r.log.Info("shutting down")
	for len(r.inFlight) > 0 {
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *Runner) handle(msg jetstream.Msg) {
	var t workflow.Task
	if err := json.Unmarshal(msg.Data(), &t); err != nil {
		r.log.Error("unmarshal", "error", err)
		_ = msg.Nak()
		return
	}

	log := r.log.With("wf", t.WorkflowID, "task", t.ID)

	if st, _ := r.store.GetTaskState(context.Background(), t.WorkflowID, t.ID); st != nil && st.Status == workflow.StatusCancelled {
		_ = msg.Ack()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.AckTimeout)
	defer cancel()

	r.mu.Lock()
	r.active[t.ID] = cancel
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, t.ID)
		r.mu.Unlock()
	}()

	cancelSub, _ := r.client.SubscribeSync(fmt.Sprintf("workflow.cancel.%s", t.WorkflowID))
	if cancelSub != nil {
		defer func() { _ = cancelSub.Unsubscribe() }()
		go r.watchCancel(ctx, cancelSub, t.ID)
	}

	_ = r.store.StoreTaskState(context.Background(), t.WorkflowID, t.ID, &workflow.TaskResult{
		WorkflowID:  t.WorkflowID,
		TaskID:      t.ID,
		ProcessorID: r.cfg.ProcessorID,
		Status:      workflow.StatusRunning,
	})

	result := r.execute(ctx, &t)
	_ = r.store.StoreTaskState(context.Background(), result.WorkflowID, result.TaskID, result)

	data, _ := json.Marshal(result)

	if result.Status == workflow.StatusFailed || result.Status == workflow.StatusTimedOut {
		if meta, _ := msg.Metadata(); meta != nil && int(meta.NumDelivered) < r.cfg.MaxRetries {
			_ = msg.Nak()
			return
		}
	}

	_, _ = r.client.Publish(context.Background(), fmt.Sprintf("workflow.result.%s", t.ID), data)
	log.Info("done", "status", result.Status)
	_ = msg.Ack()
}

func (r *Runner) execute(ctx context.Context, t *workflow.Task) *workflow.TaskResult {
	result := &workflow.TaskResult{
		WorkflowID:  t.WorkflowID,
		TaskID:      t.ID,
		ProcessorID: r.cfg.ProcessorID,
	}

	done := make(chan struct{})
	var out Dynamic
	var err error

	go func() {
		defer close(done)
		out, err = r.handler.Handle(ctx, t.Input)
	}()

	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			result.Status = workflow.StatusTimedOut
			result.ErrorMessage = "timeout"
		} else {
			result.Status = workflow.StatusCancelled
		}
	case <-done:
		if err != nil {
			result.Status = workflow.StatusFailed
			result.ErrorMessage = err.Error()
		} else {
			result.Status = workflow.StatusCompleted
			result.Output = out
		}
	}
	return result
}

func (r *Runner) watchCancel(ctx context.Context, sub *gonats.Subscription, taskID string) {
	for ctx.Err() == nil {
		if _, err := sub.NextMsg(time.Second); err == nil {
			r.mu.RLock()
			if cancel, ok := r.active[taskID]; ok {
				cancel()
			}
			r.mu.RUnlock()
			return
		}
	}
}

// ReportProgress updates the progress of the current workflow in the store.
func (r *Runner) ReportProgress(ctx context.Context, wfID string, _ int) error {
	return r.store.UpdateProgress(ctx, wfID)
}

func sanitize(s string) string {
	out := make([]byte, len(s))
	for i, c := range s {
		if c == '.' || c == '>' || c == '*' {
			out[i] = '-'
		} else {
			out[i] = byte(c)
		}
	}
	return string(out)
}
