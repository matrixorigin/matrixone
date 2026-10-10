// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package python

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/udf/udferr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var errSupervisorClosing = udferr.New("python udf worker is shutting down")

type Supervisor struct {
	cfg     Config
	mu      sync.Mutex
	cmd     *exec.Cmd
	done    chan struct{}
	lastErr error
	closing bool
}

func NewSupervisor(cfg Config) (*Supervisor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Supervisor{cfg: cfg}, nil
}

func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return errSupervisorClosing
	}
	if s.cmd != nil {
		return nil
	}
	workerPath := s.cfg.Path
	if !filepath.IsAbs(workerPath) {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		workerPath = filepath.Join(cwd, workerPath)
	}
	workerPath = filepath.Join(workerPath, "worker.py")
	if _, err := os.Stat(workerPath); err != nil {
		return err
	}
	executable := s.cfg.Python
	if executable == "" {
		executable = "python"
	}
	cmd := exec.Command(executable, "-u", workerPath, "--address="+s.cfg.Address)
	prepareSupervisorCommand(cmd)
	// The MatrixOne service owns process logging. Inheriting stderr keeps
	// worker diagnostics in the service log and avoids an unbounded, per-start
	// file beside the binary.
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	// Keep the notification channel after Wait clears cmd.  A very short-lived
	// worker can exit between cmd.Start and the caller's Done call; dropping the
	// channel in that window would make the service role miss an unexpected
	// worker exit entirely.  The next successful Start replaces it with a fresh
	// channel.
	s.cmd, s.done = cmd, done
	s.lastErr = nil
	go s.wait(cmd, done)
	logutil.Infof("started Python UDF worker: %s", cmd.String())
	return nil
}

func (s *Supervisor) wait(cmd *exec.Cmd, done chan struct{}) {
	err := cmd.Wait()
	unplanned := false
	s.mu.Lock()
	if s.cmd == cmd {
		// Keep the terminal state transition and Done close under the same lock.
		// Otherwise Start can observe cmd == nil in the small interval after
		// Wait clears it but before the old Done channel is closed, and launch a
		// replacement while a concurrent Close is still waiting for the old
		// worker.
		s.cmd = nil
		s.lastErr = err
		// Close marks the supervisor as closing before killing the process
		// group. Keep the exit error available through Err(), but do not report
		// an intentional SIGKILL as an unexpected worker failure.
		unplanned = !s.closing
	}
	close(done)
	s.mu.Unlock()
	if err != nil && unplanned {
		logutil.Errorf("Python UDF worker exited: %v", err)
	}
}

// Done returns the exit notification for the currently running worker. The
// channel is captured after Start and is closed exactly once by wait. A caller
// that owns the service role can therefore observe an unexpected worker exit
// independently of the normal shutdown context.
func (s *Supervisor) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// Err returns the exit error for the most recently completed worker. It must
// be read after the channel returned by Done has closed. A nil error means the
// process exited with status zero; the service layer still treats that as an
// unexpected exit while the role is running.
func (s *Supervisor) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

func (s *Supervisor) Close() error {
	s.mu.Lock()
	cmd, done := s.cmd, s.done
	if cmd != nil || done != nil {
		s.closing = true
	}
	s.mu.Unlock()
	var closeErr error
	if cmd != nil {
		if err := killSupervisorProcess(cmd); err != nil && !errors.Is(err, os.ErrProcessDone) {
			closeErr = errutil.Wrapf(err, "stop Python UDF worker")
		}
	}
	if done != nil {
		<-done
	}
	// Keep Start fenced until the worker observed by this Close has published
	// its terminal notification.  This also clears the fence when Close is
	// called after an unexpected exit, where cmd is already nil.
	s.mu.Lock()
	if s.cmd == nil && s.done == done {
		s.closing = false
	}
	s.mu.Unlock()
	return closeErr
}
