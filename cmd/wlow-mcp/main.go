// wlow-mcp is an MCP (Model Context Protocol) server that exposes wlow
// workflow operations as tools for AI agents and IDE integrations.
//
// Listens on HTTP at /mcp for JSON-RPC 2.0 requests.
// Transport: HTTP POST (stateless). SSE not supported.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wlow/wlow/pkg/artifact"
	wlownats "github.com/wlow/wlow/pkg/nats"
	"github.com/wlow/wlow/pkg/sdk"
	"github.com/wlow/wlow/pkg/workflow"
)

const (
	mcpProtocolVersion    = "2024-11-05"
	defaultTimeoutSeconds = 120
	maxTimeoutSeconds     = 600
)

func main() {
	addr := flag.String("addr", envOr("WLOW_MCP_ADDR", ":8088"), "listen address")
	natsURL := flag.String("nats", envOr("NATS_URL", "nats://localhost:4222"), "NATS URL")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	client, err := sdk.NewClient(sdk.ClientConfig{NATSUrl: *natsURL})
	if err != nil {
		log.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	nc, err := wlownats.NewClient(wlownats.Config{URL: *natsURL})
	if err != nil {
		log.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	artStore, err := artifact.NewStore(context.Background(), nc.JetStream(), artifact.StoreConfig{})
	if err != nil {
		log.Error("artifact store init failed", "error", err)
		os.Exit(1)
	}

	srv := &mcpServer{client: client, artJS: nc.JetStream(), artStore: artStore}

	http.HandleFunc("/mcp", srv.handle)
	log.Info("wlow-mcp started", "addr", *addr, "nats", *natsURL)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
}

type mcpServer struct {
	client   *sdk.Client
	artJS    jetstream.JetStream
	artStore *artifact.Store
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

func (s *mcpServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeRPC(w, rpcResponse{JSONRPC: "2.0", Error: rpcErr(-32700, "parse error", err.Error())})
		return
	}
	result, err := s.dispatch(r.Context(), req)
	if err != nil {
		writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: rpcErr(-32603, "internal error", err.Error())})
		return
	}
	writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result})
}

func (s *mcpServer) dispatch(ctx context.Context, req rpcRequest) (any, error) {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": "wlow", "version": "0.1.0"},
		}, nil

	case "tools/list":
		return map[string]any{"tools": toolList()}, nil

	case "tools/call":
		return s.callTool(ctx, req.Params)

	default:
		return nil, fmt.Errorf("unsupported method: %s", req.Method)
	}
}

func (s *mcpServer) callTool(ctx context.Context, params json.RawMessage) (any, error) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &call); err != nil {
		return nil, err
	}
	switch call.Name {
	case "submit_workflow":
		return s.submitWorkflow(ctx, call.Arguments)
	case "list_processors":
		return s.listProcessors(ctx, call.Arguments)
	case "get_workflow_result", "push_processor":
		return nil, fmt.Errorf("tool %q: use 'wlow start' or the Go SDK instead", call.Name)
	default:
		return nil, fmt.Errorf("unknown tool: %s", call.Name)
	}
}

// submitWorkflow submits a workflow and waits for its result.
func (s *mcpServer) submitWorkflow(ctx context.Context, args json.RawMessage) (any, error) {
	var req struct {
		Workflow       json.RawMessage `json:"workflow"`
		TimeoutSeconds int             `json:"timeout_seconds"`
	}
	if err := json.Unmarshal(args, &req); err != nil {
		return nil, err
	}
	if len(req.Workflow) == 0 {
		return nil, errors.New("workflow required")
	}
	wf, err := workflow.ParseWorkflow(req.Workflow)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow: %w", err)
	}

	timeout := req.TimeoutSeconds
	if timeout <= 0 {
		timeout = defaultTimeoutSeconds
	}
	if timeout > maxTimeoutSeconds {
		timeout = maxTimeoutSeconds
	}

	tctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	result, err := s.client.SubmitAndWait(tctx, wf)
	if err != nil {
		return nil, fmt.Errorf("workflow failed: %w", err)
	}
	return map[string]any{
		"content": []map[string]any{{
			"type": "text",
			"text": fmt.Sprintf("workflow %s completed: status=%s", wf.ID, result.Status),
		}},
		"workflow_id":  wf.ID,
		"status":       result.Status,
		"task_results": result.TaskResults,
	}, nil
}

// listProcessors returns all registered processors from NATS KV.
func (s *mcpServer) listProcessors(ctx context.Context, args json.RawMessage) (any, error) {
	var req struct {
		Tenant string `json:"tenant"`
	}
	_ = json.Unmarshal(args, &req)
	tenant := req.Tenant
	if tenant == "" {
		tenant = artifact.DefaultTenant
	}

	kv, err := s.artJS.KeyValue(ctx, artifact.ManifestBucket)
	if err != nil {
		return nil, fmt.Errorf("artifact store unavailable: %w", err)
	}

	watcher, err := kv.WatchAll(ctx, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, fmt.Errorf("list processors: %w", err)
	}

	defer func() {
		if err := watcher.Stop(); err != nil {
			slog.Default().Error("watcher.Stop() failed", "error", err)
		}
	}()

	seen := make(map[string]bool)
	var processors []map[string]string

	const maxProcessors = 512
	for count := 0; count < maxProcessors; count++ {
		select {
		case entry, ok := <-watcher.Updates():
			if !ok || entry == nil {
				goto done
			}
			key := entry.Key()
			// manifest key pattern: {tenant}.processor.{id}.{version}
			if !strings.Contains(key, ".processor.") {
				continue
			}
			parts := strings.SplitN(key, ".processor.", 2)
			if len(parts) != 2 || parts[0] != tenant {
				continue
			}
			idVersion := strings.SplitN(parts[1], ".", 2)
			if len(idVersion) != 2 {
				continue
			}
			procID := idVersion[0]
			if seen[procID] {
				continue
			}
			seen[procID] = true
			m, err := artifact.DecodeManifest(entry.Value())
			if err != nil {
				continue
			}
			processors = append(processors, map[string]string{
				"id":      m.ProcessorID,
				"version": m.Version,
				"runtime": string(m.Runtime),
			})
		case <-ctx.Done():
			goto done
		}
	}
done:
	return map[string]any{
		"content": []map[string]any{{
			"type": "text",
			"text": fmt.Sprintf("found %d processors", len(processors)),
		}},
		"processors": processors,
	}, nil
}

func toolList() []map[string]any {
	return []map[string]any{
		{
			"name":        "submit_workflow",
			"description": "Submit a wlow workflow and wait for its result. Tasks run in dependency order across process, WASM, or microVM processors.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"workflow": map[string]any{
						"type":        "object",
						"description": "Workflow definition: {id, tasks: {name: {processor_id, processor_version, input}}, dependencies: {name: [dep, ...]}}",
					},
					"timeout_seconds": map[string]any{
						"type":        "integer",
						"description": fmt.Sprintf("Max wait time in seconds (default %d, max %d)", defaultTimeoutSeconds, maxTimeoutSeconds),
					},
				},
				"required": []string{"workflow"},
			},
		},
		{
			"name":        "list_processors",
			"description": "List all registered processors available for use in workflows.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"tenant": map[string]any{
						"type":        "string",
						"description": "Tenant namespace (default: \"default\")",
					},
				},
			},
		},
		{
			"name":        "get_workflow_result",
			"description": "Not supported over MCP. Use submit_workflow which waits for the result inline.",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
		},
		{
			"name":        "push_processor",
			"description": "Not supported over MCP. Use the wlow CLI: wlow push --id <id> --runtime <runtime> ...",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}
}

func rpcErr(code int, message, data string) map[string]any {
	return map[string]any{"code": code, "message": message, "data": data}
}

func writeRPC(w http.ResponseWriter, res rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
