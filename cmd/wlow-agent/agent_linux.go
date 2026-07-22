//go:build linux

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"

	"github.com/mdlayher/vsock"
)

func mountPseudoFS() {
	mounts := []struct {
		source string
		target string
		fstype string
		flags  uintptr
	}{
		{"proc", "/proc", "proc", 0},
		{"sysfs", "/sys", "sysfs", 0},
		{"devtmpfs", "/dev", "devtmpfs", 0},
		{"tmpfs", "/tmp", "tmpfs", 0},
	}
	const maxMounts = 16
	for idx := 0; idx < len(mounts) && idx < maxMounts; idx++ {
		m := mounts[idx]
		_ = os.MkdirAll(m.target, 0o755)
		if err := syscall.Mount(m.source, m.target, m.fstype, m.flags, ""); err != nil {
			_, _ = os.Stderr.WriteString("wlow-agent: mount " + m.target + " failed: " + err.Error() + "\n")
		}
	}
}

func dialVsock(_ context.Context, cid, port uint32) (net.Conn, error) {
	c, err := vsock.Dial(cid, port, nil)
	if err != nil {
		return nil, fmt.Errorf("vsock dial cid=%d port=%d: %w", cid, port, err)
	}
	return c, nil
}

func configureCommandRoot(cmd *exec.Cmd, root string) {
	if root == "" {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: root}
	if cmd.Dir == "" {
		cmd.Dir = "/"
	}
}
