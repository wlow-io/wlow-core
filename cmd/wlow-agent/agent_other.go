//go:build !linux

package main

import (
	"context"
	"errors"
	"net"
	"os/exec"
)

// mountPseudoFS is a no-op on non-Linux hosts; the agent only ever runs as
// PID 1 inside a Linux microVM.
func mountPseudoFS() {}

func dialVsock(_ context.Context, _, _ uint32) (net.Conn, error) {
	return nil, errors.New("vsock not supported on this platform; set WLOW_VSOCK_PATH for unix-socket fallback")
}

func configureCommandRoot(_ *exec.Cmd, _ string) {}
