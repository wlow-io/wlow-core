// Package main is the wlow control-plane entrypoint for the runtime image.
// The canonical user-facing way to start the control plane is:
//
//	wlow start --control-plane
//
// This binary exists for the container image entrypoint where a separate
// orchestrator process is convenient. It is not part of the public CLI API.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/artifact"
	"github.com/wlow/wlow/pkg/config"
	"github.com/wlow/wlow/pkg/nats"
	"github.com/wlow/wlow/pkg/workflow"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(context.Background(), log); err != nil {
		log.Error("control plane failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg := config.Load()

	log.Info("wlow control plane starting",
		"nats", cfg.NATSUrl,
		"workflow_prefix", cfg.WorkflowSubjectPrefix,
		"processor_prefix", cfg.ProcessorSubjectPrefix,
	)

	client, err := nats.NewClient(nats.Config{URL: cfg.NATSUrl})
	if err != nil {
		return err
	}
	defer client.Close()

	store, err := nats.NewStore(ctx, client, nats.StoreConfig{Bucket: cfg.StoreBucket, TTL: 24 * time.Hour})
	if err != nil {
		return err
	}

	artStore, err := artifact.NewStore(ctx, client.JetStream(), artifact.StoreConfig{})
	if err != nil {
		return err
	}

	artServer, err := artifact.NewServer(artifact.ServerConfig{
		Store:  artStore,
		Logger: log.With(slog.String("component", "artifact")),
	})
	if err != nil {
		return err
	}
	artSubs, err := artServer.Subscribe(client.Connection())
	if err != nil {
		return err
	}
	defer func() {
		for _, sub := range artSubs {
			_ = sub.Unsubscribe()
		}
	}()

	wfStream, err := client.CreateStream(ctx, nats.StreamConfig{
		Name:       cfg.WorkflowStream,
		Subjects:   []string{cfg.WorkflowSubjectPrefix + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		MaxBytes:   cfg.StreamMaxBytes,
		Duplicates: 20 * time.Minute,
	})
	if err != nil {
		return err
	}
	if _, err := client.CreateStream(ctx, nats.StreamConfig{
		Name:       cfg.ProcessorStream,
		Subjects:   []string{cfg.ProcessorSubjectPrefix + ".>"},
		Retention:  jetstream.WorkQueuePolicy,
		MaxBytes:   cfg.StreamMaxBytes,
		Duplicates: 20 * time.Minute,
	}); err != nil {
		return err
	}

	publisher := workflow.PublisherFunc(func(ctx context.Context, subject string, data []byte) error {
		_, err := client.Publish(ctx, subject, data)
		return err
	})

	locality, err := workflow.NewLocalityScheduler(ctx, client.JetStream())
	if err != nil {
		return err
	}

	engine := workflow.NewEngine(workflow.EngineConfig{
		Store:                  store,
		Publisher:              publisher,
		Resolver:               artStore,
		Locality:               locality,
		ProcessorSubjectPrefix: cfg.ProcessorSubjectPrefix,
		Logger:                 log.With(slog.String("component", "engine")),
	})

	resultPub := workflow.NewNATSResultPublisher(client.Connection(), client.JetStream(), cfg.WorkflowSubjectPrefix)
	resultHandler := workflow.NewResultHandler(workflow.ResultHandlerConfig{
		Store:                  store,
		Publisher:              resultPub,
		Resolver:               artStore,
		Locality:               locality,
		ProcessorSubjectPrefix: cfg.ProcessorSubjectPrefix,
		Logger:                 log.With(slog.String("component", "result")),
	})
	cancelHandler := workflow.NewCancelHandler(workflow.CancelHandlerConfig{
		Store:                 store,
		Publisher:             publisher,
		WorkflowSubjectPrefix: cfg.WorkflowSubjectPrefix,
		Logger:                log.With(slog.String("component", "cancel")),
	})

	streamName := wfStream.CachedInfo().Config.Name

	submitConsumer, err := client.CreateConsumer(ctx, nats.ConsumerConfig{
		Name:          "workflow-consumer",
		Stream:        streamName,
		FilterSubject: workflow.SubmitSubject(cfg.WorkflowSubjectPrefix),
		MaxDeliver:    cfg.MaxRetries,
		AckWait:       cfg.AckTimeout,
	})
	if err != nil {
		return err
	}
	submitCtx, err := submitConsumer.Consume(engine.HandleWorkflow)
	if err != nil {
		return err
	}
	defer submitCtx.Stop()

	resultConsumer, err := client.CreateConsumer(ctx, nats.ConsumerConfig{
		Name:          "result-consumer",
		Stream:        streamName,
		FilterSubject: workflow.ResultFilterSubject(cfg.WorkflowSubjectPrefix),
		MaxDeliver:    cfg.MaxRetries,
		AckWait:       cfg.AckTimeout,
	})
	if err != nil {
		return err
	}
	resultCtx, err := resultConsumer.Consume(resultHandler.HandleResult)
	if err != nil {
		return err
	}
	defer resultCtx.Stop()

	cancelConsumer, err := client.CreateConsumer(ctx, nats.ConsumerConfig{
		Name:          "cancel-consumer",
		Stream:        streamName,
		FilterSubject: workflow.CancelRequestSubject(cfg.WorkflowSubjectPrefix),
		MaxDeliver:    cfg.MaxRetries,
		AckWait:       cfg.AckTimeout,
	})
	if err != nil {
		return err
	}
	cancelCtx, err := cancelConsumer.Consume(cancelHandler.HandleCancel)
	if err != nil {
		return err
	}
	defer cancelCtx.Stop()

	if _, err := workflow.NewNATSOutputCache(ctx, client.JetStream(), 24*time.Hour); err != nil {
		return err
	}

	log.Info("wlow control plane ready")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
	return nil
}
