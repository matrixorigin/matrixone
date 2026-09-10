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
	s.cmd, s.done = cmd, done
	go s.wait(cmd, done)
	logutil.Infof("started Python UDF worker: %s", cmd.String())
	return nil
}

func (s *Supervisor) wait(cmd *exec.Cmd, done chan struct{}) {
	err := cmd.Wait()
	s.mu.Lock()
	if s.cmd == cmd {
		s.cmd, s.done = nil, nil
		s.closing = false
	}
	s.mu.Unlock()
	close(done)
	if err != nil {
		logutil.Errorf("Python UDF worker exited: %v", err)
	}
}

func (s *Supervisor) Close() error {
	s.mu.Lock()
	cmd, done := s.cmd, s.done
	if cmd != nil {
		s.closing = true
	}
	s.mu.Unlock()
	if cmd == nil {
		return nil
	}
	var closeErr error
	if err := killSupervisorProcess(cmd); err != nil && !errors.Is(err, os.ErrProcessDone) {
		closeErr = fmt.Errorf("stop Python UDF worker: %w", err)
	}
	if done != nil {
		<-done
	}
	return closeErr
}
