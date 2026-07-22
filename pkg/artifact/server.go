package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// Server implements the artifact protocol over NATS.
type Server struct {
	store *Store
	log   *slog.Logger
}

// ServerConfig configures an artifact server.
type ServerConfig struct {
	Store  *Store
	Logger *slog.Logger
}

// NewServer creates a new artifact server.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Store == nil {
		return nil, errors.New("artifact store required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{store: cfg.Store, log: cfg.Logger}, nil
}

// Subscribe registers artifact protocol handlers on the provided NATS connection.
func (s *Server) Subscribe(nc *nats.Conn) ([]*nats.Subscription, error) {
	if nc == nil {
		return nil, errors.New("nats connection required")
	}
	subs := make([]*nats.Subscription, 0, 2)
	for subject, handler := range map[string]nats.MsgHandler{
		SubjectManifestPut: s.handleManifestPut,
		SubjectTenantKey:   s.handleTenantKey,
	} {
		sub, err := nc.Subscribe(subject, handler)
		if err != nil {
			return subs, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (s *Server) handleManifestPut(msg *nats.Msg) {
	var req ManifestPutRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		s.respond(msg, err)
		return
	}
	if err := validateManifestForStorage(context.Background(), &req.Manifest); err != nil {
		s.respond(msg, err)
		return
	}
	if err := s.store.PutArtifact(context.Background(), &req.Manifest, req.Tags...); err != nil {
		s.respond(msg, err)
		return
	}
	s.respond(msg, ManifestPutResponse{
		ProcessorID:  req.Manifest.ProcessorID,
		Version:      req.Manifest.Version,
		ArtifactHash: req.Manifest.ArtifactHash,
	})
}

func (s *Server) handleTenantKey(msg *nats.Msg) {
	var req TenantKeyRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		s.respond(msg, err)
		return
	}
	key, err := s.store.EnsureTenantKey(context.Background(), req.Tenant)
	if err != nil {
		s.respond(msg, err)
		return
	}
	s.respond(msg, TenantKeyResponse{Tenant: normTenant(req.Tenant), Key: key})
}

// validateManifestForStorage checks that the manifest doesn't reference
// storage backends that no longer exist (NATS object store).
func validateManifestForStorage(_ context.Context, m *Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	for _, ref := range ManifestObjectRefs(m) {
		if ref.Name != "" {
			return errors.New("NATS object-store artifact refs are disabled; use OCI remote refs")
		}
	}
	return nil
}

func (s *Server) respond(msg *nats.Msg, value any) {
	if msg.Reply == "" {
		return
	}
	if err, ok := value.(error); ok {
		value = ErrorResponse{Error: err.Error()}
	}
	data, err := json.Marshal(value)
	if err != nil {
		s.log.Error("artifact response marshal failed", "error", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		s.log.Error("artifact response failed", "error", err)
	}
}
