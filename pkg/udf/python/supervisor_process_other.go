//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package python

import "os/exec"

func prepareSupervisorCommand(*exec.Cmd) {}

func killSupervisorProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
