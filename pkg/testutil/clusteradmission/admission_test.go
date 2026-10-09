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
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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

func TestAdmissionLeaseReportsTiming(t *testing.T) {
	manager := newManager(filepath.Join(t.TempDir(), "cluster.lock"), time.Millisecond)
	lease, err := manager.acquire(context.Background(), Exclusive)
	require.NoError(t, err)

	beforeRelease := lease.Timing()
	require.False(t, beforeRelease.RequestedAt.IsZero())
	require.False(t, beforeRelease.AcquiredAt.IsZero())
	require.GreaterOrEqual(t, beforeRelease.WaitDuration, time.Duration(0))
	require.GreaterOrEqual(t, beforeRelease.HoldDuration, time.Duration(0))
	require.True(t, beforeRelease.ReleasedAt.IsZero())

	require.NoError(t, lease.Release())
	afterRelease := lease.Timing()
	require.False(t, afterRelease.ReleasedAt.IsZero())
	require.GreaterOrEqual(t, afterRelease.WaitDuration, time.Duration(0))
	require.GreaterOrEqual(t, afterRelease.HoldDuration, beforeRelease.HoldDuration)
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

func TestAdmissionProcessPoolIsBoundedAndExcludesExclusive(t *testing.T) {
	t.Setenv(ProcessPoolSizeEnv, "2")
	path := filepath.Join(t.TempDir(), "cluster.lock")
	firstManager := newManager(path, time.Millisecond)
	secondManager := newManager(path, time.Millisecond)
	thirdManager := newManager(path, time.Millisecond)

	first, err := firstManager.acquire(context.Background(), AllowConcurrentProcesses)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Release()) })
	second, err := secondManager.acquire(context.Background(), AllowConcurrentProcesses)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Release()) })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = thirdManager.acquire(ctx, AllowConcurrentProcesses)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	exclusiveManager := newManager(path, time.Millisecond)
	exclusiveCtx, exclusiveCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer exclusiveCancel()
	_, err = exclusiveManager.acquire(exclusiveCtx, Exclusive)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, first.Release())
	next, err := thirdManager.acquire(context.Background(), AllowConcurrentProcesses)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, next.Release()) })
	require.NoError(t, next.Release())
	require.NoError(t, second.Release())
}

func TestAdmissionProcessPoolWorksAcrossProcesses(t *testing.T) {
	t.Setenv(ProcessPoolSizeEnv, "2")
	path := filepath.Join(t.TempDir(), "cluster.lock")
	owner := newManager(path, time.Millisecond)
	lease, err := owner.acquire(context.Background(), AllowConcurrentProcesses)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lease.Release()) })

	// The subprocess must take the second slot rather than wait for the
	// owner's shared gate. This exercises the same file-lock boundary used by
	// two concurrent race-UT batch processes.
	runAdmissionHelper(t, path, "pooled-acquired")
}

func TestAdmissionSubprocessHelper(t *testing.T) {
	mode := os.Getenv(helperModeEnv)
	if mode == "" {
		return
	}
	manager := newManager(os.Getenv(helperPathEnv), time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	modeValue := Exclusive
	if mode == "pooled-acquired" {
		modeValue = AllowConcurrentProcesses
	}
	lease, err := manager.acquire(ctx, modeValue)

	switch mode {
	case "blocked":
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case "hold":
		require.NoError(t, err)
		fmt.Println("ready")
		var buffer [1]byte
		_, _ = os.Stdin.Read(buffer[:])
		require.NoError(t, lease.Release())
	case "acquired":
		require.NoError(t, err)
		require.NoError(t, lease.Release())
	case "pooled-acquired":
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

func TestProcessDeathReleasesAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.lock")
	readyRead, readyWrite, err := os.Pipe()
	require.NoError(t, err)
	defer readyRead.Close()
	defer readyWrite.Close()
	inputRead, inputWrite, err := os.Pipe()
	require.NoError(t, err)
	defer inputRead.Close()
	defer inputWrite.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAdmissionSubprocessHelper$")
	cmd.Env = append(os.Environ(), helperModeEnv+"=hold", helperPathEnv+"="+path)
	cmd.Stdin, cmd.Stdout = inputRead, readyWrite
	require.NoError(t, cmd.Start())
	var stopped sync.Once
	stop := func() { stopped.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) }
	t.Cleanup(stop)
	require.NoError(t, readyWrite.Close())
	require.NoError(t, readyRead.SetReadDeadline(time.Now().Add(5*time.Second)))
	message, err := bufio.NewReader(readyRead).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "ready\n", message)
	runAdmissionHelper(t, path, "blocked")
	stop()
	runAdmissionHelper(t, path, "acquired")
}

func TestAdmissionProcessPoolEnvironment(t *testing.T) {
	for _, value := range []string{"", "0", "1", "invalid", "2"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(ProcessPoolSizeEnv, value)
			m := newManager(filepath.Join(t.TempDir(), "cluster.lock"), time.Millisecond)
			lease, err := m.acquire(t.Context(), AllowConcurrentProcesses)
			if value != "2" {
				require.ErrorContains(t, err, ProcessPoolSizeEnv)
				require.Nil(t, lease)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, lease.Release()) })
			require.NoError(t, lease.Release())
			// An ordinary exclusive lease must work again after the pool releases.
			next, err := m.acquire(t.Context(), Exclusive)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, next.Release()) })
		})
	}
}

func TestAdmissionProcessPoolBlockedByExclusive(t *testing.T) {
	t.Setenv(ProcessPoolSizeEnv, "2")
	path := filepath.Join(t.TempDir(), "cluster.lock")
	owner := newManager(path, time.Millisecond)
	lease, err := owner.acquire(t.Context(), Exclusive)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lease.Release()) })
	contender := newManager(path, time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = contender.acquire(ctx, AllowConcurrentProcesses)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, contender.lock)
	require.Nil(t, contender.gate)
	require.NoError(t, lease.Release())
	next, err := contender.acquire(t.Context(), AllowConcurrentProcesses)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, next.Release()) })
}
