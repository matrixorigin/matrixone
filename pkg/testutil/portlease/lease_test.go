// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package portlease

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaseLifecycleAndExhaustion(t *testing.T) {
	a := Allocator{Directory: t.TempDir()}
	var leases []*Lease
	t.Cleanup(func() {
		for _, lease := range leases {
			require.NoError(t, lease.Close())
		}
	})
	for base := MinPort; base+Span <= MaxPort; base += Span {
		lease, err := a.Acquire()
		require.NoError(t, err)
		require.Equal(t, base, lease.Base())
		leases = append(leases, lease)
		contender, locked, err := a.TryAcquire(base)
		require.NoError(t, err)
		require.Nil(t, contender)
		require.False(t, locked)
	}
	_, err := a.Acquire()
	require.ErrorContains(t, err, "no test port range")
	released := leases[len(leases)-1]
	require.NoError(t, released.Close())
	leases = leases[:len(leases)-1]
	again, locked, err := a.TryAcquire(released.Base())
	require.NoError(t, err)
	require.True(t, locked)
	leases = append(leases, again)
	require.Equal(t, released.Base(), again.Base())
}

func TestLeaseAcquisitionErrors(t *testing.T) {
	a := Allocator{Directory: t.TempDir()}
	for _, base := range []uint64{0, MinPort + 1, MaxPort, ^uint64(0)} {
		lease, locked, err := a.TryAcquire(base)
		require.Error(t, err)
		require.Nil(t, lease)
		require.False(t, locked)
	}
	blocker := filepath.Join(a.Directory, fmt.Sprintf("%d.lock", MinPort))
	require.NoError(t, os.Mkdir(blocker, 0755))
	lease, locked, err := a.TryAcquire(MinPort)
	require.Error(t, err)
	require.Nil(t, lease)
	require.False(t, locked)
	require.NoError(t, os.Remove(blocker))
	lease, locked, err = a.TryAcquire(MinPort)
	require.NoError(t, err)
	require.True(t, locked)
	require.NoError(t, lease.Close())
	file := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(file, nil, 0600))
	_, err = (Allocator{Directory: filepath.Join(file, "leases")}).Acquire()
	require.Error(t, err)
}

func TestLeaseReleasedOnProcessExit(t *testing.T) {
	if dir := os.Getenv("MO_PORT_LEASE_HELPER_DIR"); dir != "" {
		lease, err := (Allocator{Directory: dir}).Acquire()
		require.NoError(t, err)
		defer func() { require.NoError(t, lease.Close()) }()
		fmt.Println(lease.Base())
		_, err = io.Copy(io.Discard, os.Stdin)
		require.NoError(t, err)
		return
	}
	a := Allocator{Directory: t.TempDir()}
	args := []string{"-test.run=^TestLeaseReleasedOnProcessExit$"}
	if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+dir.Value.String())
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], args...)
	cmd.Env = append(os.Environ(), "MO_PORT_LEASE_HELPER_DIR="+a.Directory)
	cmd.Stderr = os.Stderr
	input, err := cmd.StdinPipe()
	require.NoError(t, err)
	defer input.Close()
	output, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	line, err := bufio.NewReader(output).ReadString('\n')
	require.NoError(t, err)
	base, err := strconv.ParseUint(strings.TrimSpace(line), 10, 64)
	require.NoError(t, err)
	lease, locked, err := a.TryAcquire(base)
	require.NoError(t, err)
	require.Nil(t, lease)
	require.False(t, locked)
	require.NoError(t, cmd.Process.Kill())
	err = cmd.Wait()
	waited = true
	require.Error(t, err)
	// Wait observes descriptor teardown; no stale-PID cleanup or polling needed.
	lease, locked, err = a.TryAcquire(base)
	require.NoError(t, err)
	require.True(t, locked)
	require.NoError(t, lease.Close())
}

func TestDefaultDirectoryIgnoresTemporaryWorkspace(t *testing.T) {
	expected := fmt.Sprintf("/tmp/mo-test-port-leases-%d", os.Getuid())
	require.Equal(t, expected, DefaultDirectory())
	t.Setenv("TMPDIR", t.TempDir())
	require.Equal(t, expected, DefaultDirectory())
}
