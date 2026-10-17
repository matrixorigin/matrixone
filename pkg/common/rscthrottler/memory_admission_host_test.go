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
				Used: 15 * gib, Limit: 16 * gib, Reclaimable: 9 * gib,
				Source: system.MemoryDomainSourceHost, Measured: true,
			}
			ledger := system.NewMemoryAdmissionBudgetWithProvider(system.DefaultMemoryDomainReservePolicy(),
				func() (system.MemoryDomainSnapshot, error) { return snapshot, nil })
			m := &memThrottler{memoryLedger: ledger, cgroupBudget: &cgroupMemoryBudget{}}
			m.publishMemoryDomain(&snapshot)
			m.limit.Store(int64(12 * gib))
			m.actualTotalMemory.Store(16 * gib)
			m.rss.Store(int64(gib))
			require.Equal(t, int64(6*gib), m.currentMemoryUsage(), "host cache must not trigger hard reclamation")
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
			snapshot.Used = snapshot.Limit
			snapshot.Reclaimable = 0
			m.publishMemoryDomain(&snapshot)
			_, ok = policy.acquire(m, 1)
			require.False(t, ok, "true host exhaustion must still reject the production write path")
			require.Zero(t, ledger.Reserved())
		})
	}
}

func TestComponentDomainCacheCreditPreservesCgroupBoundary(t *testing.T) {
	for _, source := range []system.MemoryDomainSource{
		system.MemoryDomainSourceHost, system.MemoryDomainSourceCgroupV1, system.MemoryDomainSourceCgroupV2,
	} {
		t.Run(string(source), func(t *testing.T) {
			m := &memThrottler{}
			m.publishMemoryDomain(&system.MemoryDomainSnapshot{
				Used: 95, Limit: 100, Reclaimable: 50, Source: source, Measured: true,
			})
			sample := m.cgroupSnapshot()
			require.Equal(t, int64(95), sample.usage, "retain raw domain usage for diagnostics")
			if source == system.MemoryDomainSourceHost {
				require.Equal(t, int64(45), m.currentMemoryUsage())
				require.Equal(t, int64(50), sample.capAdmissionAvailable(50, 0))
			} else {
				require.Equal(t, int64(95), m.currentMemoryUsage())
				require.Equal(t, int64(5), sample.capAdmissionAvailable(50, 0))
			}
		})
	}
}
