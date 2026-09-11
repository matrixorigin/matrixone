// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSupervisorCloseStopsTheCurrentWorkerBeforeReturning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("worker supervisor test uses a POSIX executable script")
	}
	dir := t.TempDir()
	workerPath := filepath.Join(dir, "worker.py")
	require.NoError(t, os.WriteFile(workerPath, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o755))

	supervisor, err := NewSupervisor(Config{Address: "127.0.0.1:0", Path: dir, Python: "/bin/sh"})
	require.NoError(t, err)
	require.NoError(t, supervisor.Start())
	require.Eventually(t, func() bool {
		supervisor.mu.Lock()
		defer supervisor.mu.Unlock()
		return supervisor.cmd != nil
	}, time.Second, time.Millisecond)
	require.NoError(t, supervisor.Close())
	require.Eventually(t, func() bool {
		supervisor.mu.Lock()
		defer supervisor.mu.Unlock()
		return supervisor.cmd == nil && supervisor.done != nil
	}, time.Second, time.Millisecond)
	select {
	case <-supervisor.Done():
	default:
		t.Fatal("closed worker did not publish a completed notification")
	}

	// A second start is only safe after the first wait has completed. This
	// catches a Close implementation that clears cmd and returns before Wait.
	require.NoError(t, supervisor.Start())
	require.NoError(t, supervisor.Close())
}

func TestSupervisorPublishesUnexpectedWorkerExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("worker supervisor test uses a POSIX executable script")
	}
	dir := t.TempDir()
	workerPath := filepath.Join(dir, "worker.py")
	require.NoError(t, os.WriteFile(workerPath, []byte("#!/bin/sh\nexit 17\n"), 0o755))

	supervisor, err := NewSupervisor(Config{Address: "127.0.0.1:0", Path: dir, Python: "/bin/sh"})
	require.NoError(t, err)
	require.NoError(t, supervisor.Start())
	done := supervisor.Done()
	require.NotNil(t, done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker exit was not published")
	}
	require.EqualError(t, supervisor.Err(), "exit status 17")
	require.NoError(t, supervisor.Close())
}

func TestSupervisorKeepsImmediateExitObservable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("worker supervisor test uses a POSIX executable script")
	}
	dir := t.TempDir()
	workerDir := filepath.Join(dir, "worker")
	require.NoError(t, os.Mkdir(workerDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(workerDir, "worker.py"), []byte(""), 0o644))

	script := filepath.Join(dir, "python")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nexit 17\n"), 0o755))
	s, err := NewSupervisor(Config{
		Path:    workerDir,
		Python:  script,
		Address: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, s.Start())
	done := s.Done()
	require.NotNil(t, done, "Start must publish an observable exit channel even for an immediate failure")
	// Close must wait even when Wait has already cleared cmd but has not yet
	// published the terminal notification.
	require.NoError(t, s.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("immediate worker exit was not published")
	}
	require.Error(t, s.Err(), "a close that races an immediate worker exit must still publish the terminal process result")
}
