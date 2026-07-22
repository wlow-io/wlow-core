package workflow

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ResultPublisher is the interface for publishing task results.
type ResultPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
	PublishWithID(ctx context.Context, msgID, subject string, data []byte) error
	PublishCancel(ctx context.Context, wfID string) error
}

// ResultHandlerConfig is the configuration for a ResultHandler.
type ResultHandlerConfig struct {
	Store                  Store
	Publisher              ResultPublisher
	Resolver               Resolver
	Locality               *LocalityScheduler
	ProcessorSubjectPrefix string
	Logger                 *slog.Logger
	Metrics                Metrics
}

// ResultHandler handles task results and drives workflow progress.
type ResultHandler struct {
	store                  Store
	pub                    ResultPublisher
	resolver               Resolver
	locality               *LocalityScheduler
	processorSubjectPrefix string
	log                    *slog.Logger
	metrics                Metrics
}

// NewResultHandler creates a new ResultHandler.
func NewResultHandler(cfg ResultHandlerConfig) *ResultHandler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = noopMetrics{}
	}
	return &ResultHandler{
		store:                  cfg.Store,
		pub:                    cfg.Publisher,
		resolver:               cfg.Resolver,
		locality:               cfg.Locality,
		processorSubjectPrefix: cfg.ProcessorSubjectPrefix,
		log:                    cfg.Logger,
		metrics:                cfg.Metrics,
	}
}

// HandleResult processes a task result message.
func (h *ResultHandler) HandleResult(msg jetstream.Msg) {
	var r TaskResult
	if err := json.Unmarshal(msg.Data(), &r); err != nil {
		h.log.Error("unmarshal failed", "error", err)
		_ = msg.Nak()
		return
	}

	log := h.log.With("workflow_id", r.WorkflowID, "task_id", r.TaskID)
	if r.WorkflowID == "" || r.TaskID == "" {
		log.Error("invalid result identifiers")
		_ = msg.Nak()
		return
	}
	if r.Status == "" {
		log.Error("invalid result status")
		_ = msg.Nak()
		return
	}

	wf, err := h.store.GetWorkflow(context.Background(), r.WorkflowID)
	if err != nil {
		log.Error("get workflow failed", "error", err)
		_ = msg.Nak()
		return
	}
	if err := h.store.StoreTaskState(context.Background(), r.WorkflowID, r.TaskID, &r); err != nil {
		log.Error("store result failed", "error", err)
		_ = msg.Nak()
		return
	}

	done, err := h.store.IsWorkflowCompleted(context.Background(), r.WorkflowID)
	if err != nil {
		log.Error("check complete failed", "error", err)
		_ = msg.Nak()
		return
	}

	if done {
		h.finalize(context.Background(), wf, &r, log)
		_ = msg.Ack()
		return
	}

	h.publishReady(context.Background(), wf, r.TaskID, log)
	_ = msg.Ack()
}

func (h *ResultHandler) finalize(ctx context.Context, wf *Workflow, r *TaskResult, log *slog.Logger) {
	if r.Status == StatusFailed || r.Status == StatusTimedOut {
		if err := h.pub.PublishCancel(ctx, r.WorkflowID); err != nil {
			log.Error("publish cancel failed", "error", err)
		}
	}

	result, err := h.store.AggregateResult(ctx, r.WorkflowID)
	if err != nil {
		log.Error("aggregate failed", "error", err)
		return
	}

	if result.Status == StatusFailed {
		h.metrics.WorkflowFailed(r.WorkflowID)
		if r.ErrorMessage != "" {
			result.Error = &Error{
				ProcessorID: r.ProcessorID,
				TaskID:      r.TaskID,
				Message:     r.ErrorMessage,
			}
		}
	} else {
		h.metrics.WorkflowCompleted(r.WorkflowID, time.Since(wf.StartedAt).Seconds())
		result.Status = StatusCompleted
	}

	data, err := json.Marshal(result)
	if err != nil {
		log.Error("marshal workflow reply failed", "error", err)
		return
	}
	if err := h.pub.PublishWithID(ctx, r.WorkflowID, wf.ReplySubject, data); err != nil {
		log.Error("publish workflow reply failed", "reply_subject", wf.ReplySubject, "error", err)
	}
}

func (h *ResultHandler) publishReady(ctx context.Context, wf *Workflow, doneTask string, log *slog.Logger) {
	for id, deps := range wf.Dependencies {
		depends := false
		for _, d := range deps {
			if d == doneTask {
				depends = true
				break
			}
		}
		if !depends {
			continue
		}

		ready := true
		for _, d := range deps {
			s, err := h.store.GetTaskState(ctx, wf.ID, d)
			if err != nil || s.Status != StatusCompleted {
				ready = false
				break
			}
		}

		if ready {
			t := wf.Tasks[id]
			t.ID = id
			t.WorkflowID = wf.ID
			h.publishTask(ctx, &t, log)
		}
	}
}

func (h *ResultHandler) publishTask(ctx context.Context, t *Task, log *slog.Logger) {
	data, err := json.Marshal(t)
	if err != nil {
		log.Error("marshal failed", "task", t.ID, "error", err)
		return
	}
	subject, err := RouteTaskWithProcessorPrefix(ctx, h.resolver, h.locality, h.processorSubjectPrefix, t)
	if err != nil {
		log.Error("route failed", "task", t.ID, "error", err)
		return
	}
	if err := h.pub.Publish(ctx, subject, data); err != nil {
		log.Error("publish failed", "task", t.ID, "error", err)
		return
	}
	h.metrics.TaskQueued(subject)
	if err := h.store.StoreTaskState(ctx, t.WorkflowID, t.ID, &TaskResult{
		WorkflowID: t.WorkflowID,
		TaskID:     t.ID,
		Status:     StatusQueued,
	}); err != nil {
		log.Error("store state failed", "task", t.ID, "error", err)
	}
}

// NATSPublisher implements ResultPublisher using NATS and JetStream.
type NATSPublisher struct {
	js                    jetstream.JetStream
	nc                    *nats.Conn
	workflowSubjectPrefix string
}

// NewNATSResultPublisher creates a new NATSPublisher.
func NewNATSResultPublisher(nc *nats.Conn, js jetstream.JetStream, workflowSubjectPrefix string) *NATSPublisher {
	return &NATSPublisher{js: js, nc: nc, workflowSubjectPrefix: workflowSubjectPrefix}
}

// Publish sends a message to a NATS subject.
func (p *NATSPublisher) Publish(ctx context.Context, subj string, data []byte) error {
	_, err := p.js.Publish(ctx, subj, data)
	return err
}

// PublishWithID sends a message with a specific NATS Message ID for deduplication.
func (p *NATSPublisher) PublishWithID(ctx context.Context, id, subj string, data []byte) error {
	_, err := p.js.PublishMsg(ctx, &nats.Msg{Subject: subj, Data: data}, jetstream.WithMsgID(id))
	if err == nil {
		return nil
	}
	if p.nc == nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if publishErr := p.nc.Publish(subj, data); publishErr != nil {
		return publishErr
	}
	return p.nc.Flush()
}

// PublishCancel sends a cancellation message for a workflow.
func (p *NATSPublisher) PublishCancel(ctx context.Context, wfID string) error {
	_, err := p.js.Publish(ctx, CancelSubject(p.workflowSubjectPrefix, wfID), nil)
	return err
}
