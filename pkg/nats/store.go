package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/workflow"
)

// Store implements workflow.Store using NATS KeyValue.
type Store struct {
	kv jetstream.KeyValue
}

// StoreConfig provides configuration for the NATS KeyValue store.
type StoreConfig struct {
	Bucket  string
	History uint8
	TTL     time.Duration
}

// NewStore creates a new NATS-backed workflow store.
func NewStore(ctx context.Context, c *Client, cfg StoreConfig) (*Store, error) {
	if cfg.Bucket == "" {
		cfg.Bucket = "workflow-state"
	}
	if cfg.History == 0 {
		cfg.History = 64
	}
	if cfg.TTL == 0 {
		cfg.TTL = 24 * time.Hour
	}

	kv, err := c.js.KeyValue(ctx, cfg.Bucket)
	if err == jetstream.ErrBucketNotFound {
		kv, err = c.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:  cfg.Bucket,
			History: cfg.History,
			TTL:     cfg.TTL,
		})
	}
	if err != nil {
		return nil, err
	}
	return &Store{kv: kv}, nil
}

// KV returns the underlying NATS KeyValue bucket.
func (s *Store) KV() jetstream.KeyValue { return s.kv }

func (s *Store) put(ctx context.Context, k string, v []byte) error {
	_, err := s.kv.Put(ctx, k, v)
	return err
}

func (s *Store) get(ctx context.Context, k string) ([]byte, error) {
	e, err := s.kv.Get(ctx, k)
	if err != nil {
		return nil, err
	}
	return e.Value(), nil
}

func wfKey(id string) string       { return fmt.Sprintf("workflow.%s.data", id) }
func taskKey(wf, t string) string  { return fmt.Sprintf("workflow.%s.task.%s.state", wf, t) }
func progressKey(id string) string { return fmt.Sprintf("workflow.%s.progress", id) }

// InitWorkflow initializes the workflow state in the store.
func (s *Store) InitWorkflow(ctx context.Context, wf *workflow.Workflow) error {
	wf.StartedAt = time.Now()
	for id := range wf.Tasks {
		if err := s.StoreTaskState(ctx, wf.ID, id, &workflow.TaskResult{
			WorkflowID: wf.ID,
			TaskID:     id,
			Status:     workflow.StatusPending,
		}); err != nil {
			return err
		}
	}
	data, _ := json.Marshal(wf)
	return s.put(ctx, wfKey(wf.ID), data)
}

// GetWorkflow retrieves a workflow definition by ID.
func (s *Store) GetWorkflow(ctx context.Context, id string) (*workflow.Workflow, error) {
	data, err := s.get(ctx, wfKey(id))
	if err != nil {
		return nil, err
	}
	var wf workflow.Workflow
	return &wf, json.Unmarshal(data, &wf)
}

// StoreTaskState updates the state of a single task in the store.
func (s *Store) StoreTaskState(ctx context.Context, wfID, taskID string, st *workflow.TaskResult) error {
	data, _ := json.Marshal(st)
	return s.put(ctx, taskKey(wfID, taskID), data)
}

// GetTaskState retrieves the current state of a task.
func (s *Store) GetTaskState(ctx context.Context, wfID, taskID string) (*workflow.TaskResult, error) {
	data, err := s.get(ctx, taskKey(wfID, taskID))
	if err != nil {
		return nil, err
	}
	var st workflow.TaskResult
	return &st, json.Unmarshal(data, &st)
}

// IsWorkflowCompleted checks if all tasks in a workflow have finished.
func (s *Store) IsWorkflowCompleted(ctx context.Context, wfID string) (bool, error) {
	wf, err := s.GetWorkflow(ctx, wfID)
	if err != nil {
		return false, err
	}

	running := false
	for id := range wf.Tasks {
		st, err := s.GetTaskState(ctx, wfID, id)
		if err != nil {
			return false, err
		}
		switch st.Status {
		case workflow.StatusFailed, workflow.StatusTimedOut:
			return true, nil
		case workflow.StatusPending, workflow.StatusQueued, workflow.StatusRunning:
			running = true
		}
	}
	return !running, nil
}

// CancelWorkflow marks all pending/queued tasks in a workflow as cancelled.
func (s *Store) CancelWorkflow(ctx context.Context, wfID string) error {
	wf, err := s.GetWorkflow(ctx, wfID)
	if err != nil {
		return err
	}
	for id := range wf.Tasks {
		st, _ := s.GetTaskState(ctx, wfID, id)
		if st != nil && (st.Status == workflow.StatusQueued || st.Status == workflow.StatusPending) {
			st.Status = workflow.StatusCancelled
			_ = s.StoreTaskState(ctx, wfID, id, st)
		}
	}
	return nil
}

// AggregateResult collects results from all tasks into a single Result object.
func (s *Store) AggregateResult(ctx context.Context, wfID string) (*workflow.Result, error) {
	wf, err := s.GetWorkflow(ctx, wfID)
	if err != nil {
		return nil, err
	}

	r := &workflow.Result{
		WorkflowID:  wfID,
		Status:      workflow.StatusCompleted,
		Metadata:    wf.Metadata,
		TaskResults: make([]workflow.TaskResult, 0, len(wf.Tasks)),
	}

	for id := range wf.Tasks {
		st, err := s.GetTaskState(ctx, wfID, id)
		if err != nil {
			return nil, err
		}
		r.TaskResults = append(r.TaskResults, *st)
		if st.Status != workflow.StatusCompleted {
			r.Status = workflow.StatusFailed
			if r.Error == nil && st.ErrorMessage != "" {
				r.Error = &workflow.Error{
					ProcessorID: st.ProcessorID,
					TaskID:      st.TaskID,
					Message:     st.ErrorMessage,
				}
			}
		}
	}
	return r, nil
}

// UpdateProgress updates the progress timestamp for a workflow.
func (s *Store) UpdateProgress(ctx context.Context, wfID string) error {
	return s.put(ctx, progressKey(wfID), []byte(time.Now().Format(time.RFC3339)))
}
