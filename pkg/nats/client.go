package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client wraps NATS and JetStream connections.
type Client struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// Config provides configuration for the NATS client.
type Config struct {
	URL            string
	MaxReconnects  int
	ReconnectWait  time.Duration
	ConnectTimeout time.Duration
}

// NewClient creates a new NATS client.
func NewClient(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		cfg.URL = "nats://localhost:4222"
	}
	if cfg.MaxReconnects == 0 {
		cfg.MaxReconnects = -1
	}
	if cfg.ReconnectWait == 0 {
		cfg.ReconnectWait = 2 * time.Second
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	nc, err := nats.Connect(cfg.URL,
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.Timeout(cfg.ConnectTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	return &Client{nc: nc, js: js}, nil
}

// Close closes the NATS connection.
func (c *Client) Close() { c.nc.Close() }

// Connection returns the underlying NATS connection.
func (c *Client) Connection() *nats.Conn { return c.nc }

// JetStream returns the underlying JetStream context.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// StreamConfig provides configuration for a NATS JetStream stream.
type StreamConfig struct {
	Name       string
	Subjects   []string
	Retention  jetstream.RetentionPolicy
	MaxMsgs    int64
	MaxBytes   int64
	MaxAge     time.Duration
	Duplicates time.Duration
}

// CreateStream creates a NATS JetStream stream if it doesn't exist.
func (c *Client) CreateStream(ctx context.Context, cfg StreamConfig) (jetstream.Stream, error) {
	s, err := c.js.Stream(ctx, cfg.Name)
	if err == jetstream.ErrStreamNotFound {
		return c.js.CreateStream(ctx, jetstream.StreamConfig{
			Name:       cfg.Name,
			Subjects:   cfg.Subjects,
			Retention:  cfg.Retention,
			MaxMsgs:    cfg.MaxMsgs,
			MaxBytes:   cfg.MaxBytes,
			MaxAge:     cfg.MaxAge,
			Duplicates: cfg.Duplicates,
		})
	}
	return s, err
}

// ConsumerConfig provides configuration for a NATS JetStream consumer.
type ConsumerConfig struct {
	Name           string
	Stream         string
	FilterSubject  string
	FilterSubjects []string
	MaxDeliver     int
	MaxAckPending  int
	AckWait        time.Duration
}

// CreateConsumer creates or updates a NATS JetStream consumer.
func (c *Client) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (jetstream.Consumer, error) {
	if cfg.Stream == "" {
		return nil, fmt.Errorf("consumer stream required")
	}
	if cfg.Name == "" {
		return nil, fmt.Errorf("consumer name required")
	}
	if cfg.FilterSubject != "" && len(cfg.FilterSubjects) > 0 {
		return nil, fmt.Errorf("consumer filter_subject and filter_subjects are mutually exclusive")
	}
	jcfg := jetstream.ConsumerConfig{
		Name:          cfg.Name,
		Durable:       cfg.Name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    cfg.MaxDeliver,
		MaxAckPending: cfg.MaxAckPending,
		AckWait:       cfg.AckWait,
	}
	if len(cfg.FilterSubjects) > 0 {
		jcfg.FilterSubjects = cfg.FilterSubjects
	} else {
		jcfg.FilterSubject = cfg.FilterSubject
	}
	return c.js.CreateOrUpdateConsumer(ctx, cfg.Stream, jcfg)
}

// Publish sends a message to a NATS subject using JetStream.
func (c *Client) Publish(ctx context.Context, subj string, data []byte) (*jetstream.PubAck, error) {
	return c.js.Publish(ctx, subj, data)
}

// Subscribe creates a NATS subscription for the given subject.
func (c *Client) Subscribe(subj string, h nats.MsgHandler) (*nats.Subscription, error) {
	return c.nc.Subscribe(subj, h)
}

// SubscribeSync creates a synchronous NATS subscription for the given subject.
func (c *Client) SubscribeSync(subj string) (*nats.Subscription, error) {
	return c.nc.SubscribeSync(subj)
}
