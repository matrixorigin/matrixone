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

package lifecycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLBindingPagerReadsOnlyExplicitTenantBindings(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_account",
				accountID: 0,
				result:    lifecycleAccountResult(t, mp, 17),
			},
			{
				contains:  "from mo_catalog.mo_lifecycle_bindings",
				accountID: 17,
				result:    lifecycleBindingResult(t, mp),
			},
		},
	}
	pager := SQLBindingPager{Executor: fake}
	bindings, next, end, err := pager.NextActiveBindings(
		context.Background(),
		BindingCursor{},
		8,
	)
	require.NoError(t, err)
	require.True(t, end)
	require.Len(t, bindings, 1)
	require.Equal(t, uint32(17), bindings[0].AccountID)
	require.Equal(t, uint64(42), bindings[0].PhysicalTableID)
	require.Equal(t, uint64(3), bindings[0].Generation)
	require.Equal(t, uint64(11), bindings[0].Version)
	require.Equal(t, strings.Repeat("b2", 32), bindings[0].StageIdentityDigest)
	require.Equal(t, "ARCHIVE", bindings[0].Action)
	require.Equal(t, uint32(90), bindings[0].ExpireAfterDays)
	require.Equal(
		t,
		time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
		bindings[0].LastFullScanAt,
	)
	require.Equal(t, BindingCursor{AccountID: 18}, next)
	require.Equal(t, 2, fake.offset)
}

func TestSQLBindingPagerPersistsOnlyHintWithVersionCAS(t *testing.T) {
	mp := mpool.MustNewZero()
	result := executor.Result{AffectedRows: 1, Mp: mp}
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "set scan_snapshot_ts=unhex",
			accountID: 17,
			result:    result,
		}},
	}
	pager := SQLBindingPager{Executor: fake}
	snapshot := types.BuildTS(123, 7)
	var name objectio.ObjectNameShort
	copy(name[:], []byte("0123456789abcdefghijklmnopqrstuv"))
	binding := Binding{
		ID:        "00112233445566778899aabbccddeeff",
		AccountID: 17,
		Version:   11,
	}
	updated, err := pager.SaveCursor(
		context.Background(),
		binding,
		DiscoveryCursor{
			Snapshot:       snapshot,
			LastObjectName: name,
			HasLastObject:  true,
		},
		time.Date(2026, 7, 31, 12, 30, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(12), updated.Version)
	require.Equal(
		t,
		time.Date(2026, 7, 31, 12, 30, 0, 0, time.UTC),
		updated.LastFullScanAt,
	)
}

func TestSQLBindingPagerLeavesFullScanAnchorUnchangedBetweenPages(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:    "set scan_snapshot_ts=unhex",
			notContains: "last_full_scan_at",
			accountID:   17,
			result:      executor.Result{AffectedRows: 1, Mp: mp},
		}},
	}
	anchor := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	updated, err := (SQLBindingPager{Executor: fake}).SaveCursor(
		context.Background(),
		Binding{
			ID:             "00112233445566778899aabbccddeeff",
			AccountID:      17,
			Version:        11,
			LastFullScanAt: anchor,
		},
		DiscoveryCursor{Snapshot: types.BuildTS(123, 7)},
		time.Time{},
	)
	require.NoError(t, err)
	require.Equal(t, anchor, updated.LastFullScanAt)
}

type lifecycleSQLStep struct {
	contains    string
	notContains string
	accountID   uint32
	result      executor.Result
	err         error
}

type scriptedLifecycleSQLExecutor struct {
	t      *testing.T
	steps  []lifecycleSQLStep
	offset int
}

func (fake *scriptedLifecycleSQLExecutor) Exec(
	_ context.Context,
	sql string,
	options executor.Options,
) (executor.Result, error) {
	require.Less(fake.t, fake.offset, len(fake.steps))
	step := fake.steps[fake.offset]
	fake.offset++
	require.Contains(fake.t, strings.ToLower(sql), strings.ToLower(step.contains))
	if step.notContains != "" {
		require.NotContains(
			fake.t,
			strings.ToLower(sql),
			strings.ToLower(step.notContains),
		)
	}
	require.Equal(fake.t, step.accountID, options.AccountID())
	return step.result, step.err
}

func (fake *scriptedLifecycleSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected Lifecycle SQL transaction")
}

func lifecycleAccountResult(
	t *testing.T,
	mp *mpool.MPool,
	accountIDs ...uint64,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(1)
	value.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	for _, accountID := range accountIDs {
		require.NoError(t, vector.AppendFixed(value.Vecs[0], accountID, false, mp))
	}
	value.SetRowCount(len(accountIDs))
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}

func lifecycleBindingResult(t *testing.T, mp *mpool.MPool) executor.Result {
	t.Helper()
	value := batch.NewWithSize(20)
	stringColumns := map[int]string{
		0:  "00112233445566778899AABBCCDDEEFF",
		5:  strings.Repeat("A1", 32),
		7:  "ARCHIVE",
		10: "UTC",
		13: strings.Repeat("B2", 32),
		14: "",
		15: "",
		17: "ACTIVE",
	}
	numberColumns := map[int]uint64{
		1: 7, 2: 42, 3: 42, 4: 3, 6: 2,
		8: 90, 9: 1, 11: 12, 12: 730, 18: 11,
	}
	for column := range value.Vecs {
		switch {
		case column == 16:
			value.Vecs[column] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(value.Vecs[column], false, false, mp))
		case column == 19:
			value.Vecs[column] = vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column],
				types.UnixNanoToTimestamp(
					time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC).UnixNano(),
				),
				false,
				mp,
			))
		case stringColumns[column] != "":
			value.Vecs[column] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(
				value.Vecs[column],
				[]byte(stringColumns[column]),
				false,
				mp,
			))
		case column == 14 || column == 15:
			value.Vecs[column] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(value.Vecs[column], nil, false, mp))
		default:
			value.Vecs[column] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(
				value.Vecs[column],
				numberColumns[column],
				false,
				mp,
			))
		}
	}
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}
