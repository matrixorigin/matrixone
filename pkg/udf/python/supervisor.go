// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

package python

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type Supervisor struct {
	cfg Config
	mu  sync.Mutex
	cmd *exec.Cmd
	log io.WriteCloser
}

var supervisorNumber atomic.Int32

func NewSupervisor(cfg Config) (*Supervisor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Supervisor{cfg: cfg}, nil
}

func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	exePath, err := os.Executable()
	if err != nil {
		return err
	}
	logPath := filepath.Join(filepath.Dir(exePath), "python-runtime-"+strconv.Itoa(int(supervisorNumber.Add(1))+0)+".log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	cmd := exec.Command(executable, "-u", workerPath, "--address="+s.cfg.Address)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	s.cmd, s.log = cmd, logFile
	go s.wait(cmd, logFile)
	logutil.Infof("started Python runtime worker: %s", cmd.String())
	return nil
}

func (s *Supervisor) wait(cmd *exec.Cmd, log io.WriteCloser) {
	err := cmd.Wait()
	_ = log.Close()
	s.mu.Lock()
	if s.cmd == cmd {
		s.cmd, s.log = nil, nil
	}
	s.mu.Unlock()
	if err != nil {
		logutil.Errorf("Python runtime worker exited: %v", err)
	}
}

func (s *Supervisor) Close() error {
	s.mu.Lock()
	cmd, log := s.cmd, s.log
	s.cmd, s.log = nil, nil
	s.mu.Unlock()
	if cmd == nil {
		if log != nil {
			return log.Close()
		}
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("stop Python runtime worker: %w", err)
	}
	if log != nil {
		return log.Close()
	}
	return nil
}
