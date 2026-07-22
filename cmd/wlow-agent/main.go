// wlow-agent is the in-VM PID-1 process. It mounts the minimum
// pseudo-filesystems Python/process tooling expects, dials the host over
// vsock (or a unix socket for tests), and runs whatever command the host
// pushes through the length-prefixed JSON envelope.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	vmGenIDPath = "/sys/firmware/acpi/vmgenid"
	maxFrame    = 8 << 20
	maxRequests = 1 << 20
	maxRetries  = 1 << 20
)

type envelope struct {
	Command      []string          `json:"command,omitempty"`
	Control      string            `json:"control,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	WorkDir      string            `json:"workdir,omitempty"`
	Input        json.RawMessage   `json:"input,omitempty"`
	AfterRestore []string          `json:"after_restore,omitempty"`
}

type result struct {
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type taskFile struct {
	Entrypoint []string          `json:"entrypoint"`
	Env        map[string]string `json:"env,omitempty"`
	WorkDir    string            `json:"workdir,omitempty"`
}

func main() {
	if err := run(context.Background()); err != nil {
		_, _ = os.Stderr.WriteString("wlow-agent: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if isPID1() {
		mountPseudoFS()
	}
	ensureDefaultPath()
	go watchVMGenID(ctx)
	for attempt := 0; attempt < maxRetries; attempt++ {
		conn, err := dialHost(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := waitRetry(ctx, attempt); err != nil {
				return err
			}
			continue
		}
		err = serve(ctx, conn)
		_ = conn.Close()
		if err != nil && !isRecoverableConnErr(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitRetry(ctx, 0); err != nil {
			return err
		}
	}
	return errors.New("agent reconnect limit reached")
}

func ensureDefaultPath() {
	if os.Getenv("PATH") != "" {
		return
	}
	_ = os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func isPID1() bool {
	return os.Getpid() == 1
}

// dialHost prefers vsock when running inside a microVM, falls back to a unix
// socket for local tests. The host CID is always 2 from the guest's PoV.
func dialHost(ctx context.Context) (net.Conn, error) {
	if path := configValue("WLOW_VSOCK_PATH"); path != "" {
		return dialUnix(ctx, path)
	}
	port := configUint32("WLOW_VSOCK_PORT", 1024)
	cid := configUint32("WLOW_VSOCK_CID", 2)
	return dialVsock(ctx, cid, port)
}

func dialUnix(ctx context.Context, path string) (net.Conn, error) {
	const maxTries = 60
	for tries := 0; tries < maxTries; tries++ {
		c, err := net.Dial("unix", path)
		if err == nil {
			return c, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("dial unix %s: %w", path, err)
		}
		if waitErr := waitRetry(ctx, tries); waitErr != nil {
			return nil, fmt.Errorf("dial unix %s: %w", path, waitErr)
		}
	}
	return nil, fmt.Errorf("dial unix %s: max tries reached", path)
}

func waitRetry(ctx context.Context, attempt int) error {
	if ctx == nil {
		return errors.New("context required")
	}
	delay := retryDelay(attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryDelay(attempt int) time.Duration {
	if attempt < 20 {
		return 10 * time.Millisecond
	}
	if attempt < 100 {
		return 25 * time.Millisecond
	}
	return 100 * time.Millisecond
}

func configUint32(key string, fallback uint32) uint32 {
	v := configValue(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return fallback
	}
	return uint32(n)
}

func configValue(key string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	data, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return ""
	}
	for _, field := range strings.Fields(string(data)) {
		name, value, ok := strings.Cut(field, "=")
		if ok && name == key {
			return value
		}
	}
	return ""
}

func serve(ctx context.Context, conn net.Conn) error {
	reader := bufio.NewReader(conn)
	for count := 0; count < maxRequests; count++ {
		var req envelope
		if err := readFrame(reader, &req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if req.Control == "disconnect" {
			if err := writeFrame(conn, result{}); err != nil {
				return err
			}
			return nil
		}
		applyTaskFileFallback(&req)
		resp := execute(ctx, req)
		if err := writeFrame(conn, resp); err != nil {
			return err
		}
	}
	return errors.New("agent request limit reached")
}

// applyTaskFileFallback fills in command/env from /etc/wlow/task.json if
// the host did not specify them. This lets snapshot-restore paths re-run the
// same entrypoint without the host re-pushing the descriptor.
func applyTaskFileFallback(req *envelope) {
	if len(req.Command) > 0 {
		return
	}
	data, err := os.ReadFile("/etc/wlow/task.json")
	if err != nil {
		return
	}
	var tf taskFile
	if err := json.Unmarshal(data, &tf); err != nil {
		return
	}
	req.Command = tf.Entrypoint
	req.WorkDir = tf.WorkDir
	if req.Env == nil {
		req.Env = tf.Env
	}
}

func execute(ctx context.Context, req envelope) result {
	if len(req.Command) == 0 {
		return result{Error: "command required"}
	}
	root := configValue("WLOW_CHROOT")
	command, err := resolveCommandPath(req.Command[0], root)
	if err != nil {
		return result{Error: err.Error()}
	}
	cmd := exec.CommandContext(ctx, command, req.Command[1:]...)
	cmd.Env = os.Environ()
	if req.WorkDir != "" {
		cmd.Dir = req.WorkDir
	}
	configureCommandRoot(cmd, root)
	for key, value := range req.Env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.Stdin = strings.NewReader(string(req.Input))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return result{Error: commandError(err, stderr.Bytes())}
	}
	return result{Output: json.RawMessage(stdout.Bytes())}
}

func resolveCommandPath(command string, root string) (string, error) {
	if command == "" {
		return "", errors.New("command required")
	}
	if strings.Contains(command, "/") || root == "" {
		return command, nil
	}
	pathValue := configValue("PATH")
	if pathValue == "" {
		pathValue = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	parts := strings.Split(pathValue, ":")
	const maxPathEntries = 64
	for idx := 0; idx < len(parts) && idx < maxPathEntries; idx++ {
		if parts[idx] == "" {
			continue
		}
		candidate := filepath.Join(root, parts[idx], command)
		if isExecutable(candidate) {
			return filepath.Join(parts[idx], command), nil
		}
	}
	return "", fmt.Errorf("exec: %q: executable file not found in chroot PATH", command)
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode()&0o111 != 0
}

func commandError(err error, output []byte) string {
	const maxOutput = 4096
	if len(output) > maxOutput {
		output = output[len(output)-maxOutput:]
	}
	if len(output) == 0 {
		return err.Error()
	}
	return err.Error() + ": " + string(output)
}

func watchVMGenID(ctx context.Context) {
	last := readID()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for count := 0; count < maxRequests; count++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next := readID()
			if next != "" && next != last {
				last = next
				runHook(configValue("WLOW_AFTER_RESTORE"))
			}
		}
	}
}

func isRecoverableConnErr(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset") || strings.Contains(msg, "broken pipe")
}

func readID() string {
	data, err := os.ReadFile(vmGenIDPath)
	if err != nil {
		return ""
	}
	return string(data)
}

func runHook(command string) {
	if command == "" {
		return
	}
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}
	_ = exec.Command(parts[0], parts[1:]...).Run()
}

func readFrame(r io.Reader, out any) error {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return err
	}
	if size == 0 || size > maxFrame {
		return errors.New("invalid frame size")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func writeFrame(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}
