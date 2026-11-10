// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

// The bounded qualification wrapper launches a frozen real Python adapter
// outside the native namespace, in this same worker cgroup. No server process
// or Python dependency is added to the normal owning-package test path.
func TestNativeRebaseAdapterPublication(t *testing.T) {
	runNativeRebaseAdapterBridge(t, func(result []byte, firstTableApplied int64) {
		var outcome struct {
			Status               string            `json:"status"`
			Category             string            `json:"category"`
			Publications         int               `json:"publications"`
			MainPathPublications int               `json:"main_path_publications"`
			KillCases            int               `json:"kill_cases"`
			RollbackVerified     bool              `json:"rollback_verified"`
			KilledRecovery       map[string]string `json:"killed_client_recovery"`
			CleanupComplete      bool              `json:"cleanup_complete"`
			MergeVerified        bool              `json:"merge_verified"`
			SingleSendVerified   bool              `json:"single_send_verified"`
			KnownAbortVerified   bool              `json:"known_abort_verified"`
			PendingBranchFence   bool              `json:"pending_branch_fence_verified"`
		}
		require.NoError(t, json.Unmarshal(result, &outcome))
		require.Equal(t, "PASS_REAL_NATIVE_ADAPTER_PUBLICATION", outcome.Status, "adapter failure category: %s", outcome.Category)
		require.Equal(t, 3, outcome.Publications)
		require.Equal(t, 2, outcome.MainPathPublications)
		require.Equal(t, 3, outcome.KillCases)
		require.Equal(t, map[string]string{"snapshot": "UNKNOWN", "before_commit": "UNKNOWN", "after_commit": "COMMITTED"}, outcome.KilledRecovery)
		require.True(t, outcome.RollbackVerified)
		require.GreaterOrEqual(t, firstTableApplied, int64(1), "first table must actually be processed before late-table failure")
		require.True(t, outcome.CleanupComplete)
		require.True(t, outcome.MergeVerified)
		require.True(t, outcome.SingleSendVerified)
		require.True(t, outcome.KnownAbortVerified)
		require.True(t, outcome.PendingBranchFence)
	})
}

func TestNativeRebaseToolSuitePublication(t *testing.T) {
	runNativeRebaseAdapterBridge(t, func(result []byte, _ int64) {
		var outcome struct {
			Status          string `json:"status"`
			Publications    int    `json:"publications"`
			HistoricalReads int    `json:"historical_reads"`
			Policies        int    `json:"policies"`
			MergeVerified   bool   `json:"merge_verified"`
			CleanupComplete bool   `json:"cleanup_complete"`
		}
		require.NoError(t, json.Unmarshal(result, &outcome))
		require.Equal(t, "PASS_REAL_MATRIXONE_TOOLSUITE_NATIVE_REBASE", outcome.Status)
		require.Equal(t, 4, outcome.Publications)
		require.Equal(t, 2, outcome.HistoricalReads)
		require.Equal(t, 2, outcome.Policies)
		require.True(t, outcome.MergeVerified)
		require.True(t, outcome.CleanupComplete)
	})
}

func TestNativeRebasePublicAPI(t *testing.T) {
	runNativeRebaseAdapterBridge(t, func(result []byte, _ int64) {
		var outcome struct {
			Status                string `json:"status"`
			Publications          int    `json:"publications"`
			PublicAPICalls        int    `json:"public_api_calls"`
			HistoricalReads       int    `json:"historical_reads"`
			Policies              int    `json:"policies"`
			UnsupportedRejections int    `json:"unsupported_rejections"`
			ReverseMainVerified   bool   `json:"reverse_main_verified"`
			MergeVerified         bool   `json:"merge_verified"`
			CleanupComplete       bool   `json:"cleanup_complete"`
		}
		require.NoError(t, json.Unmarshal(result, &outcome))
		require.Equal(t, "PASS_REAL_MATRIXONE_PUBLIC_NATIVE_REBASE", outcome.Status)
		require.Equal(t, 5, outcome.Publications)
		require.Equal(t, 5, outcome.PublicAPICalls)
		require.Equal(t, 3, outcome.HistoricalReads)
		require.Equal(t, 2, outcome.Policies)
		require.Equal(t, 2, outcome.UnsupportedRejections)
		require.True(t, outcome.ReverseMainVerified)
		require.True(t, outcome.MergeVerified)
		require.True(t, outcome.CleanupComplete)
	})
}

func runNativeRebaseAdapterBridge(t *testing.T, verify func([]byte, int64)) {
	runNativeRebaseAdapterBridgeWithTimeout(t, 3*time.Minute, verify)
}

func TestNativeRebaseComparisonBridge(t *testing.T) {
	// The external client owns a ten-minute execution limit. Leave a bounded
	// receipt/teardown margin so this observer cannot close its server first.
	runNativeRebaseAdapterBridgeWithTimeout(t, 11*time.Minute, func(result []byte, _ int64) {
		var outcome struct {
			Status          string `json:"status"`
			CleanupComplete bool   `json:"cleanup_complete"`
			Groups          []struct {
				Scale              string `json:"scale"`
				Mode               string `json:"mode"`
				MeasuredOperations int    `json:"measured_operations"`
				WarmupOperations   int    `json:"warmup_operations"`
			} `json:"groups"`
		}
		require.NoError(t, json.Unmarshal(result, &outcome))
		require.Equal(t, "PASS_REAL_REBASE_SMALL_AND_W1_COMPARISON", outcome.Status)
		require.True(t, outcome.CleanupComplete)
		require.Len(t, outcome.Groups, 4)
		for i, identity := range [][2]string{{"small", "legacy"}, {"small", "native"}, {"W1", "native"}, {"W1", "legacy"}} {
			require.Equal(t, identity[0], outcome.Groups[i].Scale)
			require.Equal(t, identity[1], outcome.Groups[i].Mode)
			require.Equal(t, 10, outcome.Groups[i].MeasuredOperations)
			require.Equal(t, 1, outcome.Groups[i].WarmupOperations)
		}
	})
}

func runNativeRebaseAdapterBridgeWithTimeout(t *testing.T, timeout time.Duration, verify func([]byte, int64)) {
	t.Helper()
	bridge := os.Getenv("MO_REBASE_ADAPTER_BRIDGE")
	if bridge == "" {
		t.Skip("requires the frozen external adapter qualification wrapper")
	}
	require.True(t, filepath.IsAbs(bridge))
	info, err := os.Lstat(bridge)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	runRebaseHistoryIntegration(t, func(c embed.Cluster) {
		restore := frontend.SetRebaseSourceExpectationForTest(true)
		defer restore()
		var firstTableApplied atomic.Int64
		restoreApplied := frontend.SetRebaseTableAppliedHookForTest(func(_ context.Context, _ string, table string) {
			if table == "a_ok" {
				firstTableApplied.Add(1)
			}
		})
		defer restoreApplied()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ready, err := json.Marshal(struct {
			Port int64 `json:"port"`
		}{cn.GetServiceConfig().CN.Frontend.Port})
		require.NoError(t, err)
		readyTemporary := filepath.Join(bridge, "ready.tmp")
		file, err := os.OpenFile(readyTemporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		require.NoError(t, err)
		_, err = file.Write(ready)
		closeErr := file.Close()
		require.NoError(t, err)
		require.NoError(t, closeErr)
		require.NoError(t, os.Rename(readyTemporary, filepath.Join(bridge, "ready.json")))
		// This observes an external process terminal receipt, not elapsed-time
		// synchronization of database competitors. The wrapper always provides
		// a failure receipt if its exact Python child fails before writing one.
		resultPath := filepath.Join(bridge, "result.json")
		require.Eventually(t, func() bool {
			_, err := os.Stat(resultPath)
			return err == nil
		}, timeout, 20*time.Millisecond)
		result, err := os.ReadFile(resultPath)
		require.NoError(t, err)
		verify(result, firstTableApplied.Load())
	})
}
