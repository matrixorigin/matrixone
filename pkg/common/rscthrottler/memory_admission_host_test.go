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

package rscthrottler

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/stretchr/testify/require"
)

func TestProductionPoliciesShareHostCacheCreditAndPendingCharges(t *testing.T) {
	for _, policy := range []struct {
		name    string
		acquire func(*memThrottler, int64) (int64, bool)
	}{
		{"workspace", defaultAcquirePolicy},
		{"s3", AcquirePolicyForCNFlushS3},
		{"branch", AcquirePolicyForDataBranch},
	} {
		t.Run(policy.name, func(t *testing.T) {
			const gib = uint64(1 << 30)
			snapshot := system.MemoryDomainSnapshot{
				Used: 14 * gib, Limit: 16 * gib, Reclaimable: 8 * gib,
				Source: system.MemoryDomainSourceHost, Measured: true,
			}
			ledger := system.NewMemoryAdmissionBudgetWithProvider(system.DefaultMemoryDomainReservePolicy(),
				func() (system.MemoryDomainSnapshot, error) { return snapshot, nil })
			m := &memThrottler{memoryLedger: ledger}
			m.limit.Store(int64(12 * gib))
			m.actualTotalMemory.Store(16 * gib)
			m.rss.Store(int64(gib))
			_, ok := policy.acquire(m, int64(gib))
			require.True(t, ok, "warm host cache must not reject a safe allocation")
			// Model an execution reservation using the same production ledger.
			available, ok := ledger.TryReserve(7 * gib)
			require.True(t, ok)
			require.Less(t, available, gib)
			_, ok = policy.acquire(m, int64(gib))
			require.False(t, ok, "write policies cannot reuse execution's pending headroom")
			ledger.Release(7 * gib)
			m.reserved.Add(-int64(gib))
			m.releaseCgroup(int64(gib))
			require.Zero(t, ledger.Reserved())
		})
	}
}
