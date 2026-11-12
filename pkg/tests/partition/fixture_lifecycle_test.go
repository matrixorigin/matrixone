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

package partition

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

type fixtureTestCluster struct {
	embed.Cluster
	startErr, closeErr error
	starts, closes     int
}

func (c *fixtureTestCluster) Start() error { c.starts++; return c.startErr }
func (c *fixtureTestCluster) Close() error { c.closes++; return c.closeErr }

func TestPartitionFixtureFailureIsCached(t *testing.T) {
	startErr, closeErr := errors.New("start failed"), errors.New("close failed")
	c := &fixtureTestCluster{startErr: startErr, closeErr: closeErr}
	var fixture partitionClusterFixture
	calls := 0
	create := func() (embed.Cluster, error) { calls++; return c, nil }
	for range 2 {
		got, err := fixture.get(create)
		require.Nil(t, got)
		require.ErrorIs(t, err, startErr)
		require.ErrorIs(t, err, closeErr)
	}
	require.Equal(t, 1, calls)
	require.Equal(t, 1, c.starts)
	require.Equal(t, 1, c.closes)

	// A failed close keeps the cluster owned by the package for a final retry.
	require.Same(t, c, fixture.cluster)
	c.closeErr = nil
	require.NoError(t, fixture.cluster.Close())
	require.Equal(t, 2, c.closes)
}

func TestPartitionFixtureSuccessIsReused(t *testing.T) {
	c := &fixtureTestCluster{}
	var fixture partitionClusterFixture
	calls := 0
	create := func() (embed.Cluster, error) { calls++; return c, nil }
	for range 2 {
		got, err := fixture.get(create)
		require.NoError(t, err)
		require.Same(t, c, got)
	}
	require.Equal(t, 1, calls)
	require.Equal(t, 1, c.starts)
	require.Zero(t, c.closes)
}

// Run the real TestMain in a subprocess: its cleanup runs after m.Run.
func TestPartitionFixtureShutdown(t *testing.T) {
	const childMode = "MO_PARTITION_FIXTURE_SHUTDOWN"
	if mode := os.Getenv(childMode); mode != "" {
		sharedPartitionCluster.cluster = fixtureShutdownCluster{mode: mode}
		return
	}
	for _, mode := range []string{"success", "error", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPartitionFixtureShutdown$", "-test.timeout=200ms")
			cmd.Env = append(os.Environ(), childMode+"="+mode)
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), "%s", output)
			require.Contains(t, string(output), "fixture shutdown: "+mode)
			if mode == "success" {
				require.NoError(t, err, "%s", output)
			} else {
				require.Error(t, err, "%s", output)
				if mode == "blocked" {
					require.Contains(t, string(output), "timed out closing shared partition cluster")
					require.Contains(t, string(output), "fixtureShutdownCluster.Close")
				} else {
					require.Contains(t, string(output), "close shared partition cluster: injected close failure")
				}
			}
		})
	}
}

type fixtureShutdownCluster struct {
	embed.Cluster
	mode string
}

func (c fixtureShutdownCluster) Close() error {
	fmt.Fprintln(os.Stderr, "fixture shutdown:", c.mode)
	switch c.mode {
	case "blocked":
		select {}
	case "error":
		return errors.New("injected close failure")
	default:
		return nil
	}
}
