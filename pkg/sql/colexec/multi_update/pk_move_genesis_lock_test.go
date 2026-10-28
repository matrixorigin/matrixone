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

package multi_update

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// genesisLockFabric records the SQL the live probe runs and answers it from
// per-test fixtures, and records every OWNED exclusive-lock request the probe
// makes through the pkMoveGenesisOwnedLockFn seam.
type genesisLockFabric struct {
	mp        *mpool.MPool
	sqls      []string
	hitReg    bool
	hitLinks  bool
	lockErr   error
	lockCalls [][2]uint64 // (accountID, tableID) per owned-lock request
}

func (f *genesisLockFabric) runSql(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	f.sqls = append(f.sqls, sql)
	switch {
	case strings.Contains(sql, catalog.MO_DATA_BRANCH_LINK_RETENTION):
		if f.hitReg {
			vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
			if err := vector.AppendFixedList(vec, []uint64{1}, nil, f.mp); err != nil {
				return executor.Result{}, err
			}
			bat := batch.NewWithSize(1)
			bat.SetVector(0, vec)
			bat.SetRowCount(1)
			return executor.Result{Mp: f.mp, Batches: []*batch.Batch{bat}}, nil
		}
		return executor.Result{}, nil
	case strings.Contains(sql, catalog.MO_DATA_BRANCH_UPDATE_LINK):
		if f.hitLinks {
			vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
			if err := vector.AppendFixedList(vec, []uint64{1}, nil, f.mp); err != nil {
				return executor.Result{}, err
			}
			bat := batch.NewWithSize(1)
			bat.SetVector(0, vec)
			bat.SetRowCount(1)
			return executor.Result{Mp: f.mp, Batches: []*batch.Batch{bat}}, nil
		}
		return executor.Result{}, nil
	default:
		return executor.Result{}, moerr.NewInternalErrorNoCtxf("unexpected SQL: %s", sql)
	}
}

func (f *genesisLockFabric) ownedLock(
	_ context.Context,
	_ *process.Process,
	_ engine.Engine,
	accountID uint32,
	tableID uint64,
) error {
	f.lockCalls = append(f.lockCalls, [2]uint64{uint64(accountID), tableID})
	return f.lockErr
}

func withGenesisLockFabric(t *testing.T, f *genesisLockFabric) (context.Context, *process.Process) {
	proc := testutil.NewProcess(t)
	f.mp = proc.Mp()
	origRunSql := pkMoveGenesisRunSql
	origLock := pkMoveGenesisOwnedLockFn
	pkMoveGenesisRunSql = f.runSql
	pkMoveGenesisOwnedLockFn = f.ownedLock
	t.Cleanup(func() {
		pkMoveGenesisRunSql = origRunSql
		pkMoveGenesisOwnedLockFn = origLock
		proc.Free()
	})
	return context.Background(), proc
}

// D186(B.4)(a), review F2: a MISS verdict (OWNED genesis) must establish the
// identity key's EXCLUSIVE row lock in this transaction before the link
// probe runs - exactly once, addressed at the probed (account, table).
func TestPkMoveGenesisProbeMissTakesExclusiveOwnedLock(t *testing.T) {
	f := &genesisLockFabric{}
	ctx, proc := withGenesisLockFabric(t, f)
	registered, committedLinks, _, err := pkMoveGenesisProbeLive(
		ctx, proc, nil, 7, 4242, "abcd", false)
	require.NoError(t, err)
	require.False(t, registered)
	require.False(t, committedLinks)
	require.Equal(t, [][2]uint64{{7, 4242}}, f.lockCalls)
	// Two probes, in order: retention verdict, then committed-link verdict,
	// with the exclusive lock established between them.
	require.Len(t, f.sqls, 2)
	require.Contains(t, f.sqls[0], catalog.MO_DATA_BRANCH_LINK_RETENTION)
	require.Contains(t, f.sqls[1], catalog.MO_DATA_BRANCH_UPDATE_LINK)
}

// A HIT verdict SEALS the consumed ledger row (D198(B.1)/D199(B.2)): the
// byte-identity wide UPDATE runs between the verdict probe and the link
// probe, is the producer's ONLY ledger-key contact on this path, and must
// NEVER request the OWNED exclusive lock.
func TestPkMoveGenesisProbeHitSkipsOwnedLock(t *testing.T) {
	f := &genesisLockFabric{hitReg: true}
	ctx, proc := withGenesisLockFabric(t, f)
	registered, committedLinks, _, err := pkMoveGenesisProbeLive(
		ctx, proc, nil, 7, 4242, "abcd", false)
	require.NoError(t, err)
	require.True(t, registered)
	require.False(t, committedLinks)
	require.Empty(t, f.lockCalls)
	require.Len(t, f.sqls, 3)
	require.Contains(t, f.sqls[0], catalog.MO_DATA_BRANCH_LINK_RETENTION)
	require.True(t, pkMoveIsWideSealSQL(f.sqls[1], 4242),
		"sqls[1] must be the byte-identity wide seal, got %q", f.sqls[1])
	require.Contains(t, f.sqls[2], catalog.MO_DATA_BRANCH_UPDATE_LINK)
}

// D171: an OWNED-lock failure propagates unswallowed and the link probe
// never runs - the transaction is doomed exactly like any lock pipeline
// failure.
func TestPkMoveGenesisProbeOwnedLockErrorPropagates(t *testing.T) {
	want := moerr.NewInternalErrorNoCtx("fabric lock failure")
	f := &genesisLockFabric{lockErr: want}
	ctx, proc := withGenesisLockFabric(t, f)
	_, _, _, err := pkMoveGenesisProbeLive(
		ctx, proc, nil, 7, 4242, "abcd", false)
	require.ErrorIs(t, err, want)
	require.Len(t, f.lockCalls, 1)
	require.Len(t, f.sqls, 1)
	require.Contains(t, f.sqls[0], catalog.MO_DATA_BRANCH_LINK_RETENTION)
}

// Round-50 pre-PR review (P2): the D161(A.1) committed-link presence read
// must mirror pkMoveLinkPresenceProbe's link_id primary-key half-open
// prefix range (pkg/sql/compile/pk_move_link_retention.go) - the link
// catalog's sole primary key is link_id (pkg/frontend/predefined.go), so
// without a primary-key range filter the committed-memory reader falls
// back to a full NewRowsIter partition scan on EVERY first touch
// (pkg/vm/engine/disttae/local_disttae_datasource.go), violating the keyed
// producer charge D159(F) budgets. The table_id guard and own-transaction
// exclusion survive, and presence/absence verdicts flow unchanged.
func TestPkMoveGenesisLinkProbeKeyedPrefixRange(t *testing.T) {
	const tableID uint64 = 4242
	const ownTxnHex = "abcd"
	rangeLo := fmt.Sprintf("%s >= x'%016x'", catalog.DataBranchUpdateLinkColLinkID, tableID)
	rangeHi := fmt.Sprintf("%s < x'%016x'", catalog.DataBranchUpdateLinkColLinkID, tableID+1)
	tableGuard := fmt.Sprintf("%s = %d", catalog.DataBranchUpdateLinkColTableID, tableID)
	ownExclude := fmt.Sprintf("%s <> unhex('%s')", catalog.DataBranchUpdateLinkColTxnID, ownTxnHex)
	for _, tc := range []struct {
		name      string
		hitLinks  bool
		wantLinks bool
	}{
		{name: "committed links present", hitLinks: true, wantLinks: true},
		{name: "committed links absent", hitLinks: false, wantLinks: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &genesisLockFabric{hitReg: true, hitLinks: tc.hitLinks}
			ctx, proc := withGenesisLockFabric(t, f)
			registered, committedLinks, _, err := pkMoveGenesisProbeLive(
				ctx, proc, nil, 7, tableID, ownTxnHex, false)
			require.NoError(t, err)
			require.True(t, registered)
			require.Equal(t, tc.wantLinks, committedLinks)
			require.Empty(t, f.lockCalls)
			require.Len(t, f.sqls, 3)
			linkSQL := f.sqls[2]
			require.Contains(t, linkSQL, catalog.MO_DATA_BRANCH_UPDATE_LINK)
			require.Contains(t, linkSQL, rangeLo,
				"keyed half-open range lower bound on link_id, got %q", linkSQL)
			require.Contains(t, linkSQL, rangeHi,
				"keyed half-open range upper bound on link_id, got %q", linkSQL)
			require.Contains(t, linkSQL, tableGuard,
				"table guard survives, got %q", linkSQL)
			require.Contains(t, linkSQL, ownExclude,
				"own-transaction exclusion survives, got %q", linkSQL)
		})
	}
}

// pkMoveIsWideSealSQL pins the D198(B.1) byte-identity seal's exact wide
// shape: every column sets itself on the shared identity row, pinned to
// row_kind = 0 and the consumed table id.
func pkMoveIsWideSealSQL(sql string, tableID uint64) bool {
	const prefix = "update mo_catalog.mo_data_branch_link_retention set row_kind = row_kind, table_id = table_id, " +
		"database_name = database_name, table_name = table_name, obj_id = obj_id, created_ts = created_ts, logical_id = logical_id"
	const rowPin = "where row_kind = 0 and table_id = "
	return strings.HasPrefix(sql, prefix) &&
		strings.Contains(sql, rowPin) &&
		strings.HasSuffix(sql, fmt.Sprintf("and %s = %d", catalog.DataBranchLinkRetentionColTableID, tableID))
}

// sealFabric answers the retention verdict with a pre-seal row and then
// applies per-test outcomes to each seal shape; every SQL is recorded.
type sealFabric struct {
	mp        *mpool.MPool
	sqls      []string
	wideErr   error
	legacyErr error
}

func (f *sealFabric) runSql(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	f.sqls = append(f.sqls, sql)
	switch {
	case strings.Contains(sql, "mo_data_branch_link_retention") && strings.HasPrefix(sql, "select"):
		vec := vector.NewVec(types.New(types.T_uint64, 0, 0))
		if err := vector.AppendFixedList(vec, []uint64{1}, nil, f.mp); err != nil {
			return executor.Result{}, err
		}
		bat := batch.NewWithSize(1)
		bat.SetVector(0, vec)
		bat.SetRowCount(1)
		return executor.Result{Mp: f.mp, Batches: []*batch.Batch{bat}}, nil
	case strings.Contains(sql, "mo_data_branch_update_link"):
		return executor.Result{}, nil
	case strings.HasPrefix(sql, "update mo_catalog.mo_data_branch_link_retention"):
		if strings.Contains(sql, "row_kind = row_kind") {
			return executor.Result{}, f.wideErr
		}
		return executor.Result{}, f.legacyErr
	default:
		return executor.Result{}, moerr.NewInternalErrorNoCtxf("unexpected SQL: %s", sql)
	}
}

func withSealFabric(t *testing.T, f *sealFabric) (context.Context, *process.Process) {
	proc := testutil.NewProcess(t)
	f.mp = proc.Mp()
	origRunSql := pkMoveGenesisRunSql
	pkMoveGenesisRunSql = f.runSql
	t.Cleanup(func() {
		pkMoveGenesisRunSql = origRunSql
		proc.Free()
	})
	return context.Background(), proc
}

// D198(B.1): the HIT seal degrades to the legacy five-column shape on the
// missing-column capability signal, and only there.
func TestPkMoveSealLegacyDegradeOnMissingColumn(t *testing.T) {
	f := &sealFabric{wideErr: moerr.NewBadFieldError(
		context.Background(), "logical_id", "mo_data_branch_link_retention")}
	ctx, proc := withSealFabric(t, f)
	registered, _, _, err := pkMoveGenesisProbeLive(ctx, proc, nil, 7, 4242, "abcd", false)
	require.NoError(t, err)
	require.True(t, registered)
	require.Len(t, f.sqls, 4)
	require.True(t, pkMoveIsWideSealSQL(f.sqls[1], 4242), "sqls[1] got %q", f.sqls[1])
	const legacyPin = "update mo_catalog.mo_data_branch_link_retention set table_id = table_id, " +
		"database_name = database_name, table_name = table_name, obj_id = obj_id, created_ts = created_ts where table_id = 4242"
	require.Equal(t, legacyPin, f.sqls[2], "the legacy five-column degrade shape")
	require.Contains(t, f.sqls[3], catalog.MO_DATA_BRANCH_UPDATE_LINK)
}

// D198(B.2)/D171: every non-capability seal error propagates unswallowed;
// the link probe never runs after the doomed seal.
func TestPkMoveSealErrorPropagates(t *testing.T) {
	want := moerr.NewInternalErrorNoCtx("seal rejected")
	f := &sealFabric{wideErr: want}
	ctx, proc := withSealFabric(t, f)
	_, _, _, err := pkMoveGenesisProbeLive(ctx, proc, nil, 7, 4242, "abcd", false)
	require.ErrorIs(t, err, want)
	require.Len(t, f.sqls, 2)
	require.True(t, pkMoveIsWideSealSQL(f.sqls[1], 4242), "sqls[1] got %q", f.sqls[1])
}

// D206(C.1)'s seal-removed arm: with the defect flag ON the HIT verdict
// stages NO seal and the producer's transaction stays unwitnessed for this
// path - the literal 'sealing removed' control shape.
func TestPkMoveSealRemovedDefectFlag(t *testing.T) {
	restore := DisablePkMoveRetentionSealForTest(true)
	defer restore()
	f := &sealFabric{}
	ctx, proc := withSealFabric(t, f)
	registered, _, _, err := pkMoveGenesisProbeLive(ctx, proc, nil, 7, 4242, "abcd", false)
	require.NoError(t, err)
	require.True(t, registered)
	require.Len(t, f.sqls, 2, "no seal SQL at all with the defect flag ON")
	require.Contains(t, f.sqls[0], catalog.MO_DATA_BRANCH_LINK_RETENTION)
	require.Contains(t, f.sqls[1], catalog.MO_DATA_BRANCH_UPDATE_LINK)
}

// markSpyOperator witnesses D206(B.1) SETTER SITE (2): the sealing
// transaction's operator receives exactly one optional-interface mark call
// per staged seal, while the rest of the base surface stays untouched on
// this SQL-shape path.
type markSpyOperator struct {
	client.TxnOperator
	marks int32
}

func (s *markSpyOperator) MarkExpectedConflict() { atomic.AddInt32(&s.marks, 1) }
func (s *markSpyOperator) marked() int32         { return atomic.LoadInt32(&s.marks) }

// D206(B.1) SITE (2): the seal's HIT arm marks the producer's own
// transaction at staging time, idempotently across repeated seals.
func TestPkMoveSealMarksProducerTxn(t *testing.T) {
	f := &sealFabric{}
	ctx, proc := withSealFabric(t, f)
	spy := &markSpyOperator{}
	proc.Base.TxnOperator = spy
	registered, _, _, err := pkMoveGenesisProbeLive(ctx, proc, nil, 7, 4242, "abcd", false)
	require.NoError(t, err)
	require.True(t, registered)
	require.Equal(t, int32(1), spy.marked(),
		"the staged seal marks the producer's own transaction exactly once")
	require.True(t, pkMoveIsWideSealSQL(f.sqls[1], 4242))

	registered, _, _, err = pkMoveGenesisProbeLive(ctx, proc, nil, 7, 4243, "abce", false)
	require.NoError(t, err)
	require.True(t, registered)
	require.Equal(t, int32(2), spy.marked(),
		"each staged seal re-marks idempotently (the operator fold is Store-only)")
}

// The MISS arm stages no seal - the OWNED exclusive lock seam carries that
// lane and SITE (2) does not fire.
func TestPkMoveOwnedLaneDoesNotSealMark(t *testing.T) {
	lf := &genesisLockFabric{}
	ctx, proc := withGenesisLockFabric(t, lf)
	spy := &markSpyOperator{}
	proc.Base.TxnOperator = spy
	registered, _, _, err := pkMoveGenesisProbeLive(ctx, proc, nil, 7, 4242, "abcd", false)
	require.NoError(t, err)
	require.False(t, registered)
	require.Equal(t, int32(0), spy.marked(),
		"the MISS lane stages no seal, so SITE (2) stays silent")
	require.Equal(t, [][2]uint64{{7, 4242}}, lf.lockCalls)
}
