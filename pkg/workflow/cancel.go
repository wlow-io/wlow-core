package workflow

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// CancelHandlerConfig is the configuration for a CancelHandler.
type CancelHandlerConfig struct {
	Store                 Store
	Publisher             Publisher
	WorkflowSubjectPrefix string
	Logger                *slog.Logger
}

// CancelHandler handles workflow cancellation requests.
type CancelHandler struct {
	store                 Store
	pub                   Publisher
	workflowSubjectPrefix string
	log                   *slog.Logger
}

// NewCancelHandler creates a new CancelHandler.
func NewCancelHandler(cfg CancelHandlerConfig) *CancelHandler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &CancelHandler{
		store:                 cfg.Store,
		pub:                   cfg.Publisher,
		workflowSubjectPrefix: cfg.WorkflowSubjectPrefix,
		log:                   cfg.Logger,
	}
}

// HandleCancel processes a workflow cancellation message.
func (h *CancelHandler) HandleCancel(msg jetstream.Msg) {
	var c Cancel
	if err := json.Unmarshal(msg.Data(), &c); err != nil {
		h.log.Error("unmarshal failed", "error", err)
		_ = msg.Nak()
		return
	}

	log := h.log.With("workflow_id", c.WorkflowID)

	if err := h.store.CancelWorkflow(context.Background(), c.WorkflowID); err != nil {
		log.Error("cancel failed", "error", err)
		_ = msg.Nak()
		return
	}

	_ = h.pub.Publish(context.Background(), CancelSubject(h.workflowSubjectPrefix, c.WorkflowID), nil)
	log.Info("cancelled")
	_ = msg.Ack()
}
