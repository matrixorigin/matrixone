// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

package python

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var errSupervisorClosing = errors.New("python udf worker is shutting down")

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
	s.mu.Lock()
	if s.cmd == cmd {
		// cmd is cleared so a later Start can launch a new worker, while done is
		// retained as the completed notification that Start's caller captured.
		s.cmd = nil
		s.lastErr = err
		s.closing = false
	}
	s.mu.Unlock()
	close(done)
	if err != nil {
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
	if cmd != nil {
		s.closing = true
	}
	s.mu.Unlock()
	var closeErr error
	if cmd != nil {
		if err := killSupervisorProcess(cmd); err != nil && !errors.Is(err, os.ErrProcessDone) {
			closeErr = fmt.Errorf("stop Python UDF worker: %w", err)
		}
	}
	if done != nil {
		<-done
	}
	return closeErr
}
