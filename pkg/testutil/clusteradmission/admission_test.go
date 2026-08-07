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

package clusteradmission

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	helperModeEnv = "MO_CLUSTER_ADMISSION_HELPER_MODE"
	helperPathEnv = "MO_CLUSTER_ADMISSION_HELPER_PATH"
)

func TestAdmissionRejectsImplicitReentrancyAndAllowsExplicitConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.lock")
	owner := newManager(path, time.Millisecond)
	contender := newManager(path, time.Millisecond)

	first, err := owner.acquire(context.Background(), Exclusive)
	require.NoError(t, err)
	_, err = owner.acquire(context.Background(), Exclusive)
	require.ErrorContains(t, err, "another complete test cluster")
	second, err := owner.acquire(context.Background(), AllowConcurrent)
	require.NoError(t, err)

	tryContender := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := contender.acquire(ctx, Exclusive)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	tryContender()
	require.NoError(t, first.Release())
	tryContender()
	require.NoError(t, second.Release())

	next, err := contender.acquire(context.Background(), Exclusive)
	require.NoError(t, err)
	require.NoError(t, next.Release())
	require.NoError(t, next.Release())
}

func TestAcquireRejectsInvalidContexts(t *testing.T) {
	manager := newManager(filepath.Join(t.TempDir(), "cluster.lock"), time.Millisecond)

	_, err := manager.acquire(nil, Exclusive)
	require.ErrorContains(t, err, "requires a context")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = manager.acquire(ctx, Exclusive)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestAdmissionIsExclusiveAcrossProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.lock")
	owner := newManager(path, time.Millisecond)
	lease, err := owner.acquire(context.Background(), Exclusive)
	require.NoError(t, err)

	runAdmissionHelper(t, path, "blocked")
	require.NoError(t, lease.Release())
	runAdmissionHelper(t, path, "acquired")
}

func TestAdmissionSubprocessHelper(t *testing.T) {
	mode := os.Getenv(helperModeEnv)
	if mode == "" {
		return
	}
	manager := newManager(os.Getenv(helperPathEnv), time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	lease, err := manager.acquire(ctx, Exclusive)

	switch mode {
	case "blocked":
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case "acquired":
		require.NoError(t, err)
		require.NoError(t, lease.Release())
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
}

func runAdmissionHelper(t *testing.T, path, mode string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAdmissionSubprocessHelper$")
	cmd.Env = append(os.Environ(),
		helperModeEnv+"="+mode,
		helperPathEnv+"="+path,
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}
