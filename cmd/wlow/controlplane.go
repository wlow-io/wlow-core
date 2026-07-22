package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/artifact"
	"github.com/wlow/wlow/pkg/config"
	wlownats "github.com/wlow/wlow/pkg/nats"
	"github.com/wlow/wlow/pkg/workflow"
)

type controlPlaneFlags struct {
	natsURL                string
	storeBucket            string
	workflowSubjectPrefix  string
	processorSubjectPrefix string
}

// startControlPlane runs the wlow control plane inline. It creates NATS streams,
// wires the workflow engine, result handler, and cancel handler, and blocks until
// ctx is cancelled. Returns an error on any fatal startup failure.
func startControlPlane(ctx context.Context, flags controlPlaneFlags) error {
	cfg := config.Load()

	// Apply flag overrides on top of env-based config.
	if flags.natsURL != "" {
		cfg.NATSUrl = flags.natsURL
	}
	if flags.storeBucket != "" {
		cfg.StoreBucket = flags.storeBucket
	}
	if flags.workflowSubjectPrefix != "" {
		cfg.WorkflowSubjectPrefix = flags.workflowSubjectPrefix
	}
	if flags.processorSubjectPrefix != "" {
		cfg.ProcessorSubjectPrefix = flags.processorSubjectPrefix
	}

	slog.Info("wlow control plane starting",
		"nats", cfg.NATSUrl,
		"workflow_prefix", cfg.WorkflowSubjectPrefix,
		"processor_prefix", cfg.ProcessorSubjectPrefix,
	)

	client, err := wlownats.NewClient(wlownats.Config{URL: cfg.NATSUrl})
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer client.Close()

	store, err := wlownats.NewStore(ctx, client, wlownats.StoreConfig{
		Bucket: cfg.StoreBucket,
		TTL:    24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}

	artStore, err := artifact.NewStore(ctx, client.JetStream(), artifact.StoreConfig{})
	if err != nil {
		return fmt.Errorf("init artifact store: %w", err)
	}

	artServer, err := artifact.NewServer(artifact.ServerConfig{
		Store:  artStore,
		Logger: slog.Default().With(slog.String("component", "artifact")),
	})
	if err != nil {
		return fmt.Errorf("init artifact server: %w", err)
	}
	artSubs, err := artServer.Subscribe(client.Connection())
	if err != nil {
		return fmt.Errorf("subscribe artifact protocol: %w", err)
	}
	defer func() {
		for _, sub := range artSubs {
			_ = sub.Unsubscribe()
		}
	}()

	wfStream, err := client.CreateStream(ctx, wlownats.StreamConfig{
		Name:       cfg.WorkflowStream,
		Subjects:   []string{cfg.WorkflowSubjectPrefix + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		MaxBytes:   cfg.StreamMaxBytes,
		Duplicates: 20 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create workflow stream: %w", err)
	}

	if _, err := client.CreateStream(ctx, wlownats.StreamConfig{
		Name:       cfg.ProcessorStream,
		Subjects:   []string{cfg.ProcessorSubjectPrefix + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		MaxBytes:   cfg.StreamMaxBytes,
		Duplicates: 20 * time.Minute,
	}); err != nil {
		return fmt.Errorf("create processor stream: %w", err)
	}

	publisher := workflow.PublisherFunc(func(ctx context.Context, subject string, data []byte) error {
		_, err := client.Publish(ctx, subject, data)
		return err
	})

	locality, err := workflow.NewLocalityScheduler(ctx, client.JetStream())
	if err != nil {
		return fmt.Errorf("init locality: %w", err)
	}

	engine := workflow.NewEngine(workflow.EngineConfig{
		Store:                  store,
		Publisher:              publisher,
		Resolver:               artStore,
		Locality:               locality,
		ProcessorSubjectPrefix: cfg.ProcessorSubjectPrefix,
		Logger:                 slog.Default().With(slog.String("component", "engine")),
	})

	resultPub := workflow.NewNATSResultPublisher(client.Connection(), client.JetStream(), cfg.WorkflowSubjectPrefix)
	resultHandler := workflow.NewResultHandler(workflow.ResultHandlerConfig{
		Store:                  store,
		Publisher:              resultPub,
		Resolver:               artStore,
		Locality:               locality,
		ProcessorSubjectPrefix: cfg.ProcessorSubjectPrefix,
		Logger:                 slog.Default().With(slog.String("component", "result")),
	})

	cancelHandler := workflow.NewCancelHandler(workflow.CancelHandlerConfig{
		Store:                 store,
		Publisher:             publisher,
		WorkflowSubjectPrefix: cfg.WorkflowSubjectPrefix,
		Logger:                slog.Default().With(slog.String("component", "cancel")),
	})

	streamName := wfStream.CachedInfo().Config.Name

	submitConsumer, err := client.CreateConsumer(ctx, wlownats.ConsumerConfig{
		Name:          "workflow-consumer",
		Stream:        streamName,
		FilterSubject: workflow.SubmitSubject(cfg.WorkflowSubjectPrefix),
		MaxDeliver:    cfg.MaxRetries,
		AckWait:       cfg.AckTimeout,
	})
	if err != nil {
		return fmt.Errorf("create submit consumer: %w", err)
	}
	submitCtx, err := submitConsumer.Consume(engine.HandleWorkflow)
	if err != nil {
		return fmt.Errorf("attach submit handler: %w", err)
	}
	defer submitCtx.Stop()

	resultConsumer, err := client.CreateConsumer(ctx, wlownats.ConsumerConfig{
		Name:          "result-consumer",
		Stream:        streamName,
		FilterSubject: workflow.ResultFilterSubject(cfg.WorkflowSubjectPrefix),
		MaxDeliver:    cfg.MaxRetries,
		AckWait:       cfg.AckTimeout,
	})
	if err != nil {
		return fmt.Errorf("create result consumer: %w", err)
	}
	resultCtx, err := resultConsumer.Consume(resultHandler.HandleResult)
	if err != nil {
		return fmt.Errorf("attach result handler: %w", err)
	}
	defer resultCtx.Stop()

	cancelConsumer, err := client.CreateConsumer(ctx, wlownats.ConsumerConfig{
		Name:          "cancel-consumer",
		Stream:        streamName,
		FilterSubject: workflow.CancelRequestSubject(cfg.WorkflowSubjectPrefix),
		MaxDeliver:    cfg.MaxRetries,
		AckWait:       cfg.AckTimeout,
	})
	if err != nil {
		return fmt.Errorf("create cancel consumer: %w", err)
	}
	cancelCtx, err := cancelConsumer.Consume(cancelHandler.HandleCancel)
	if err != nil {
		return fmt.Errorf("attach cancel handler: %w", err)
	}
	defer cancelCtx.Stop()

	if _, err := workflow.NewNATSOutputCache(ctx, client.JetStream(), 24*time.Hour); err != nil {
		return fmt.Errorf("init output cache: %w", err)
	}

	slog.Info("wlow control plane ready")
	<-ctx.Done()
	slog.Info("control plane shutting down")
	return nil
}
