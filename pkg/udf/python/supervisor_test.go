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
		return supervisor.cmd == nil && supervisor.done == nil
	}, time.Second, time.Millisecond)

	// A second start is only safe after the first wait has completed. This
	// catches a Close implementation that clears cmd and returns before Wait.
	require.NoError(t, supervisor.Start())
	require.NoError(t, supervisor.Close())
}
