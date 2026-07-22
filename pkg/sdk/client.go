package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/nats"
	"github.com/wlow/wlow/pkg/workflow"
)

// Client is a high-level NATS client for interacting with the workflow engine.
type Client struct {
	nc *nats.Client
	js jetstream.JetStream
}

// ClientConfig contains configuration for the Client.
type ClientConfig struct {
	NATSUrl string
}

// NewClient creates a new workflow Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	nc, err := nats.NewClient(nats.Config{URL: cfg.NATSUrl})
	if err != nil {
		return nil, err
	}
	return &Client{nc: nc, js: nc.JetStream()}, nil
}

// Close closes the underlying NATS connection.
func (c *Client) Close() { c.nc.Close() }

// Submit sends a workflow to the engine for execution.
func (c *Client) Submit(ctx context.Context, wf *workflow.Workflow) error {
	data, _ := json.Marshal(wf)
	_, err := c.js.Publish(ctx, "workflow.submit", data)
	return err
}

// SubmitAndWait sends a workflow and waits for the result on the reply subject.
func (c *Client) SubmitAndWait(ctx context.Context, wf *workflow.Workflow) (*workflow.Result, error) {
	reply := fmt.Sprintf("workflow.reply.%s", wf.ID)
	wf.ReplySubject = reply

	sub, err := c.nc.SubscribeSync(reply)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			slog.Default().Error("failed to unsubscribe", slog.String("error", err.Error()))
		}
	}()

	if err := c.Submit(ctx, wf); err != nil {
		return nil, err
	}

	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}

	var r workflow.Result
	return &r, json.Unmarshal(msg.Data, &r)
}

// Cancel requests cancellation of a running workflow.
func (c *Client) Cancel(ctx context.Context, wfID string) error {
	data, _ := json.Marshal(workflow.Cancel{WorkflowID: wfID})
	_, err := c.js.Publish(ctx, "workflow.cancel", data)
	return err
}

// WorkflowBuilder provides a fluent API for constructing workflows.
type WorkflowBuilder struct {
	wf *workflow.Workflow
}

// NewWorkflow creates a new WorkflowBuilder.
func NewWorkflow(id string) *WorkflowBuilder {
	return &WorkflowBuilder{wf: &workflow.Workflow{
		ID:           id,
		Tasks:        make(map[string]workflow.Task),
		Dependencies: make(map[string][]string),
		Metadata:     make(map[string]any),
	}}
}

// Metadata adds metadata to the workflow.
func (b *WorkflowBuilder) Metadata(k string, v any) *WorkflowBuilder {
	b.wf.Metadata[k] = v
	return b
}

// ReplyTo sets the reply subject for the workflow result.
func (b *WorkflowBuilder) ReplyTo(subj string) *WorkflowBuilder {
	b.wf.ReplySubject = subj
	return b
}

// Task adds a task to the workflow.
func (b *WorkflowBuilder) Task(id, subject string, input map[string]any, deps ...string) *WorkflowBuilder {
	b.wf.Tasks[id] = workflow.Task{ID: id, Subject: subject, Input: input}
	if len(deps) > 0 {
		b.wf.Dependencies[id] = deps
	}
	return b
}

// Build constructs and validates the workflow.
func (b *WorkflowBuilder) Build() (*workflow.Workflow, error) {
	data, _ := json.Marshal(b.wf)
	return workflow.ParseWorkflow(data)
}

// MustBuild constructs the workflow or panics if validation fails.
func (b *WorkflowBuilder) MustBuild() *workflow.Workflow {
	wf, err := b.Build()
	if err != nil {
		panic(err)
	}
	return wf
}

// Subscribe subscribes to a NATS subject (for testing/convenience).
func (c *Client) Subscribe(subj string) (*gonats.Subscription, error) {
	return c.nc.Subscribe(subj, nil)
}
