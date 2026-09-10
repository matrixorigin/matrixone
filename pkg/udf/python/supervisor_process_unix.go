//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package python

import (
	"errors"
	"os/exec"
	"syscall"
)

func prepareSupervisorCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killSupervisorProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return cmd.Process.Kill()
		}
		return err
	}
	return nil
}
