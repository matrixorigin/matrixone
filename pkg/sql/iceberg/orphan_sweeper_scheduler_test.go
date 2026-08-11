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

package iceberg

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
)

func TestOrphanSweepSchedulerRunOnceHonorsOwnerAndLimits(t *testing.T) {
	source := &fakeOrphanSweepAccountSource{accounts: []uint32{7, 8, 9}}
	owner := fakeOrphanSweepOwner{owned: map[uint32]bool{7: true, 8: false}}
	var calls []uint32
	var limits []int
	scheduler := NewOrphanSweepScheduler(OrphanSweepSchedulerOptions{
		Accounts:           source,
		Owner:              owner,
		MaxAccountsPerTick: 2,
		PerAccountLimit:    3,
		Runner: OrphanSweepRunnerFunc(func(ctx context.Context, accountID uint32, limit int) (maintenance.SweepResult, error) {
			calls = append(calls, accountID)
			limits = append(limits, limit)
			return maintenance.SweepResult{Scanned: 2, Deleted: 1}, nil
		}),
	})

	result, err := scheduler.RunOnce(context.Background())
	requireNoErr(t, err)
	if source.limit != 2 {
		t.Fatalf("expected account source limit 2, got %d", source.limit)
	}
	if len(calls) != 1 || calls[0] != 7 {
		t.Fatalf("unexpected runner calls: %+v", calls)
	}
	if len(limits) != 1 || limits[0] != 3 {
		t.Fatalf("unexpected per-account limit: %+v", limits)
	}
	if result.AccountsScanned != 2 || result.AccountsOwned != 1 || result.AccountsSkipped != 1 ||
		result.OrphansScanned != 2 || result.OrphansDeleted != 1 || result.OrphansFailed != 0 {
		t.Fatalf("unexpected scheduler result: %+v", result)
	}
}

func TestOrphanSweepSchedulerRunOnceContinuesAfterTenantError(t *testing.T) {
	sentinel := api.NewError(api.ErrObjectIO, "test orphan sweep failure", nil)
	scheduler := NewOrphanSweepScheduler(OrphanSweepSchedulerOptions{
		Accounts: &fakeOrphanSweepAccountSource{accounts: []uint32{7, 8}},
		Runner: OrphanSweepRunnerFunc(func(ctx context.Context, accountID uint32, limit int) (maintenance.SweepResult, error) {
			if accountID == 7 {
				return maintenance.SweepResult{}, sentinel
			}
			return maintenance.SweepResult{Scanned: 1, Failed: 1}, nil
		}),
	})

	result, err := scheduler.RunOnce(context.Background())
	if err != sentinel {
		t.Fatalf("expected first tenant error, got %v", err)
	}
	if result.AccountsScanned != 2 || result.AccountsOwned != 2 || result.AccountsFailed != 1 ||
		result.OrphansScanned != 1 || result.OrphansFailed != 1 {
		t.Fatalf("unexpected scheduler result after tenant error: %+v", result)
	}
}

type fakeOrphanSweepAccountSource struct {
	accounts []uint32
	limit    int
}

func (s *fakeOrphanSweepAccountSource) ListOrphanSweepAccounts(ctx context.Context, limit int) ([]uint32, error) {
	s.limit = limit
	return append([]uint32(nil), s.accounts...), nil
}

type fakeOrphanSweepOwner struct {
	owned map[uint32]bool
}

func (o fakeOrphanSweepOwner) OwnsOrphanSweep(ctx context.Context, accountID uint32) (bool, error) {
	return o.owned[accountID], nil
}
