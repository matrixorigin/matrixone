// Copyright 2021 - 2026 Matrix Origin
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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestTestingTCPOnlyConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		testing bool
		tcpOnly bool
	}{
		{"empty", "", true, false},
		{"disabled", "0", true, false},
		{"other", "true", true, false},
		{"enabled", "1", true, true},
		{"production", "1", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(testClusterTCPOnlyEnv, tc.value)
			opts := []Option{WithCNCount(2)}
			if tc.testing {
				opts = append(opts, WithTesting())
			}
			c, err := NewCluster(opts...)
			if c != nil {
				t.Cleanup(func() { require.NoError(t, c.Close()) })
			}
			require.NoError(t, err)
			ports := make(map[int64]bool)
			for i := 0; i < 2; i++ {
				cn, err := c.GetCNService(i)
				require.NoError(t, err)
				cfg := cn.GetServiceConfig().CN.Frontend
				require.Positive(t, cfg.Port)
				require.False(t, ports[cfg.Port])
				ports[cfg.Port] = true
				if tc.tcpOnly {
					require.Empty(t, cfg.UnixSocketAddress)
				} else {
					require.NotEmpty(t, cfg.UnixSocketAddress)
				}
			}
		})
	}
}

func TestTestingTCPOnlyPreservesCallerOverrideOnValidationError(t *testing.T) {
	for _, address := range []string{"", "explicit.sock"} {
		t.Run(fmt.Sprintf("address=%q", address), func(t *testing.T) {
			t.Setenv(testClusterTCPOnlyEnv, "1")
			c, err := NewCluster(WithTesting(), WithPreStart(func(svc ServiceOperator) {
				if svc.ServiceType() == metadata.ServiceType_CN {
					require.Empty(t, svc.GetServiceConfig().CN.Frontend.UnixSocketAddress)
					svc.Adjust(func(cfg *ServiceConfig) { cfg.CN.Frontend.UnixSocketAddress = "pre-start.sock" })
				}
			}))
			if c != nil {
				t.Cleanup(func() { require.NoError(t, c.Close()) })
			}
			require.NoError(t, err)
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			require.Equal(t, "pre-start.sock", cn.GetServiceConfig().CN.Frontend.UnixSocketAddress)
			cn.Adjust(func(cfg *ServiceConfig) {
				cfg.CN.Frontend.UnixSocketAddress = address
				cfg.Clock.MaxClockOffset.Duration = time.Second
				cfg.CN.Frontend.ConnectTimeout.Duration = time.Millisecond
			})
			require.ErrorContains(t, cn.Start(), "authentication freshness")
			require.Equal(t, address, cn.GetServiceConfig().CN.Frontend.UnixSocketAddress)
			require.False(t, cn.(*operator).needsCleanup())
		})
	}
}

func TestTestingTCPOnlySnapshotAppliesToAdditionalCN(t *testing.T) {
	t.Setenv(testClusterTCPOnlyEnv, "1")
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c, err := StartTestCluster(WithCNCount(1))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	t.Setenv(testClusterTCPOnlyEnv, "0")
	require.NoError(t, c.StartNewCNService(1))
	for i := 0; i < 2; i++ {
		cn, err := c.GetCNService(i)
		require.NoError(t, err)
		require.Empty(t, cn.GetServiceConfig().CN.Frontend.UnixSocketAddress)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		var got int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&got)
		cancel()
		require.NoError(t, err)
		require.Equal(t, 1, got)
	}
}
