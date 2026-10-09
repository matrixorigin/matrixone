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

func TestAdmissionSubprocessHelper(t *testing.T) {
	mode := os.Getenv(helperModeEnv)
	if mode == "" {
		return
	}
	manager := newManager(os.Getenv(helperPathEnv), time.Millisecond)
	manager.participant = os.Getenv(ParticipantEnv) == "1"
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	lease, err := manager.acquire(ctx, Exclusive)

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

func TestParticipantCapacityPreservesExclusiveGateAndBorrowedLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.lock")
	first, second := newManager(path, time.Millisecond), newManager(path, time.Millisecond)
	first.participant, second.participant = true, true
	a, err := first.acquire(t.Context(), Exclusive)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, a.Release()) })
	b, err := second.acquire(t.Context(), Exclusive)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Release()) })
	borrowed, err := first.acquire(t.Context(), AllowConcurrent)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, borrowed.Release()) })
	// Both a normal process and a third participant must remain excluded.
	t.Setenv(ParticipantEnv, "")
	runAdmissionHelper(t, path, "blocked")
	t.Setenv(ParticipantEnv, "1")
	runAdmissionHelper(t, path, "blocked")
	require.NoError(t, a.Release())
	runAdmissionHelper(t, path, "blocked")
	require.NoError(t, borrowed.Release())
	runAdmissionHelper(t, path, "acquired")
	require.NoError(t, b.Release())
	t.Setenv(ParticipantEnv, "")
	runAdmissionHelper(t, path, "acquired")
}

func TestParticipantAcquireFailureReleasesGate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.lock")
	require.NoError(t, os.Mkdir(path+".slot-0", 0700))
	participant := newManager(path, time.Millisecond)
	participant.participant = true
	lease, err := participant.acquire(t.Context(), Exclusive)
	require.Error(t, err)
	require.Nil(t, lease)
	ordinary := newManager(path, time.Millisecond)
	lease, err = ordinary.acquire(t.Context(), Exclusive)
	require.NoError(t, err)
	require.NoError(t, lease.Release())
}

func TestParticipantIncompleteReleaseCannotBorrow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.lock")
	participant := newManager(path, time.Millisecond)
	participant.participant = true
	lease, err := participant.acquire(t.Context(), Exclusive)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lease.Release()) })
	// The state after slot release but before gate cleanup must retain its
	// cleanup owner, without admitting another cluster through that owner.
	require.NoError(t, participant.slot.Close())
	participant.slot = nil
	_, err = participant.acquire(t.Context(), AllowConcurrent)
	require.ErrorContains(t, err, "cleanup is incomplete")
	t.Setenv(ParticipantEnv, "")
	runAdmissionHelper(t, path, "blocked")
	require.NoError(t, lease.Release())
	runAdmissionHelper(t, path, "acquired")
}

func TestParticipantProcessDeathReleasesAdmission(t *testing.T) {
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
	cmd.Env = append(os.Environ(), helperModeEnv+"=hold", helperPathEnv+"="+path, ParticipantEnv+"=1")
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
	t.Setenv(ParticipantEnv, "")
	runAdmissionHelper(t, path, "blocked")
	stop()
	runAdmissionHelper(t, path, "acquired")
}

func TestParticipantConfigurationIsExplicit(t *testing.T) {
	for _, value := range []string{"", "1", "2", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(ParticipantEnv, value)
			manager := newProcessManager()
			require.Equal(t, value == "1", manager.participant)
			if value == "" || value == "1" {
				require.NoError(t, manager.configErr)
			} else {
				_, err := manager.acquire(t.Context(), Exclusive)
				require.ErrorContains(t, err, "must be empty or 1")
			}
		})
	}
}
