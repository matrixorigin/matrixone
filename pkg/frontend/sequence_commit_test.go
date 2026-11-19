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

package frontend

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	txn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------
// Shared deterministic fakes of the #30359 sequence UT harness (design
// §9: I1/K1 commit-stamp oracle extends this store; the §9 B reconcile
// oracle in data_branch_sequence_reconcile_test.go reuses it).
//
// seqStore is an in-memory sequence relation store keyed by (db, table),
// holding the 7-column sequence metadata row per identity. It interprets
// exactly the conditional UPDATE shape emitted by
// stampSequenceObligationsAtCommitUnsafe and by the merge reconcile unit,
// matching rows through the same preserve-or-restore predicate
// (ana UnsafeToPreserve: strictly behind the floor in allocation order,
// or equal while still uncalled).
// ---------------------------------------------------------------------

type seqStoreRow struct {
	last     uint64 // bits; int64(last) is the signed-domain value
	minv     uint64
	maxv     uint64
	start    uint64
	incr     int64
	cycle    bool
	called   bool
	unsigned bool
	// narrowT, when non-zero, is the sequence's declared storage type
	// (T_int16/T_int32/T_uint16/T_uint32); the zero value keeps the
	// historical 64-bit rendering. #30359 pre-PR review N2 needs the
	// reconcile fixture to serve SMALLINT/INT-shaped metadata rows.
	narrowT types.T
}

type seqRows map[[2]string]seqStoreRow

type seqStore struct {
	mu   sync.Mutex
	rows seqRows
}

func newSeqStore() *seqStore { return &seqStore{rows: make(seqRows)} }

func (s *seqStore) add(db, table string, row seqStoreRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[[2]string{db, table}] = row
}

func (s *seqStore) get(db, table string) (seqStoreRow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[[2]string{db, table}]
	return row, ok
}

func (s *seqStore) snapshot() seqRows {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSeqRows(s.rows)
}

func (s *seqStore) clone() seqRows {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSeqRows(s.rows)
}

func (s *seqStore) replace(rows seqRows) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = rows
}

func (s *seqStore) applyConditionalUpdate(db, table, floorLit, cmp string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return applySeqConditionalUpdate(s.rows, db, table, floorLit, cmp)
}

func cloneSeqRows(rows seqRows) seqRows {
	out := make(seqRows, len(rows))
	for k, v := range rows {
		out[k] = v
	}
	return out
}

func seqBitsLess(unsigned bool, a, b uint64) bool {
	if unsigned {
		return a < b
	}
	return int64(a) < int64(b)
}

// seqNegBits stores a negative signed-domain value in the bit slot exactly as
// production stores typed sequence values (sign extension via int64).
func seqNegBits(v int64) uint64 { return uint64(v) }

// applySeqConditionalUpdate executes the monotonic conditional UPDATE of the
// commit stamp and the reconcile unit against one store view, reporting
// whether a row matched the guard arms (rows affected). The comparison is
// domain-aware: descending identities flip the operator, equality with
// is_called=true already matches no arm.
func applySeqConditionalUpdate(rows seqRows, db, table, floorLit, cmp string) (bool, error) {
	key := [2]string{db, table}
	row, ok := rows[key]
	if !ok {
		return false, moerr.NewNoSuchTableNoCtx(db, table)
	}
	floor, err := parseSeqNumericLiteral(row.unsigned, floorLit)
	if err != nil {
		return false, err
	}
	var match bool
	switch cmp {
	case "<":
		match = seqBitsLess(row.unsigned, row.last, floor) || (row.last == floor && !row.called)
	case ">":
		match = seqBitsLess(row.unsigned, floor, row.last) || (row.last == floor && !row.called)
	default:
		return false, moerr.NewInternalErrorNoCtx("sequence store fake: unknown comparison operator")
	}
	if !match {
		return false, nil
	}
	row.last = floor
	row.called = true
	rows[key] = row
	return true, nil
}

func parseSeqNumericLiteral(unsigned bool, lit string) (uint64, error) {
	if unsigned {
		v, err := strconv.ParseUint(lit, 10, 64)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	v, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// The fake executors accept exactly the two statement shapes the sequence
// code emits; anything else is a contract violation and fails loudly.
const seqIdentPattern = "`((?:[^`]|``)+)`"

var (
	seqConditionalUpdateRE = regexp.MustCompile("^update " + seqIdentPattern + "\\." + seqIdentPattern +
		" set last_seq_num = (-?[0-9]+), is_called = true where last_seq_num ([<>]) (-?[0-9]+) " +
		"or \\(last_seq_num = (-?[0-9]+) and is_called = false\\)$")
	seqSelectSequenceRowRE = regexp.MustCompile("^select last_seq_num, min_value, max_value, start_value, " +
		"increment_value, cycle, is_called from " + seqIdentPattern + "\\." + seqIdentPattern + " for update$")
)

// parseSeqConditionalUpdate deconstructs the stamp/reconcile conditional
// UPDATE; the production shape renders the same floor literal in all three
// positions, and anything else means the emission contract changed.
func parseSeqConditionalUpdate(sql string) (db, table, floorLit, cmp string, ok bool) {
	m := seqConditionalUpdateRE.FindStringSubmatch(sql)
	if m == nil {
		return "", "", "", "", false
	}
	if m[3] != m[5] || m[5] != m[6] {
		return "", "", "", "", false
	}
	return unescapeSeqIdent(m[1]), unescapeSeqIdent(m[2]), m[3], m[4], true
}

func parseSeqSelectSequenceRow(sql string) (db, table string, ok bool) {
	m := seqSelectSequenceRowRE.FindStringSubmatch(sql)
	if m == nil {
		return "", "", false
	}
	return unescapeSeqIdent(m[1]), unescapeSeqIdent(m[2]), true
}

func unescapeSeqIdent(inner string) string {
	return strings.ReplaceAll(inner, "``", "`")
}

// installSeqTestExecutor binds the internal SQL executor of the test into the
// session service runtime, restoring any pre-existing executor on cleanup.
func installSeqTestExecutor(t *testing.T, ses *Session, exec executor.SQLExecutor) {
	t.Helper()
	rt := moruntime.ServiceRuntime(ses.GetService())
	old, hadOld := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if hadOld {
		require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, old))
	}
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, exec)
		if hadOld {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, old)
		}
	})
}

// ---------------------------------------------------------------------
// Commit stamp harness (design §9 I1/K1/J3 UT oracle over §4 D item 5).
// ---------------------------------------------------------------------

// seqStampExecutor is the stub executor.SQLExecutor behind the commit stamp:
// it records every statement and interprets the stamp's conditional UPDATE
// against the fake store. failOn/failErr inject one storage failure.
type seqStampExecutor struct {
	store   *seqStore
	sqls    []string
	opts    []executor.Options
	matched []bool
	failOn  func(db, table string) bool
	failErr error
}

var _ executor.SQLExecutor = (*seqStampExecutor)(nil)

func (e *seqStampExecutor) Exec(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
	e.sqls = append(e.sqls, sql)
	e.opts = append(e.opts, opts)
	if e.failErr != nil && e.failOn == nil {
		return executor.Result{}, e.failErr
	}
	db, table, floorLit, cmp, ok := parseSeqConditionalUpdate(sql)
	if !ok {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("sequence stamp fake: unexpected SQL: " + sql)
	}
	if e.failOn != nil && e.failOn(db, table) {
		return executor.Result{}, e.failErr
	}
	matched, err := e.store.applyConditionalUpdate(db, table, floorLit, cmp)
	if err != nil {
		return executor.Result{}, err
	}
	e.matched = append(e.matched, matched)
	res := executor.Result{}
	if matched {
		res.AffectedRows = 1
	}
	return res, nil
}

func (e *seqStampExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	panic("commit stamp must run inside the committing transaction, never an autonomous one")
}

// seqStampRelation/seqStampDatabase model the committing transaction's final
// catalog view for the stamp's lifecycle resolution (J3).
type seqStampRelation struct {
	id  uint64
	err error
}

type seqStampDatabase struct {
	err  error
	rels map[string]seqStampRelation
}

type seqStampFixture struct {
	ctrl    *gomock.Controller
	ctx     context.Context
	ses     *Session
	op      *testTxnOp
	store   *seqStore
	exec    *seqStampExecutor
	execCtx *ExecCtx
}

// newSeqStampFixture builds a committing-transaction fixture: the TxnHandler
// storage resolves identities from catalog (absent databases report
// OkExpectedEOB, absent relations ErrNoSuchTable — the lifecycle-miss
// contract), and the internal executor stamps the fake store.
func newSeqStampFixture(t *testing.T, catalog map[string]seqStampDatabase) *seqStampFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string, _ client.TxnOperator) (engine.Database, error) {
			entry, ok := catalog[name]
			if !ok {
				return nil, moerr.GetOkExpectedEOB()
			}
			if entry.err != nil {
				return nil, entry.err
			}
			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, tbl string, _ any) (engine.Relation, error) {
					rel, ok := entry.rels[tbl]
					if !ok {
						return nil, moerr.NewNoSuchTableNoCtx(name, tbl)
					}
					if rel.err != nil {
						return nil, rel.err
					}
					mRel := mock_frontend.NewMockRelation(ctrl)
					mRel.EXPECT().GetTableID(gomock.Any()).Return(rel.id).AnyTimes()
					return mRel, nil
				}).AnyTimes()
			return db, nil
		}).AnyTimes()
	ses.txnHandler.storage = eng

	op := newTestTxnOp()
	op.meta = txn.TxnMeta{ID: []byte{0x5E, 0x0C}, Status: txn.TxnStatus_Active}
	ses.txnHandler.txnOp = op
	ses.txnHandler.txnCtx = ctx

	store := newSeqStore()
	stampExec := &seqStampExecutor{store: store}
	installSeqTestExecutor(t, ses, stampExec)

	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	t.Cleanup(execCtx.Close)
	return &seqStampFixture{
		ctrl: ctrl, ctx: ctx, ses: ses, op: op,
		store: store, exec: stampExec, execCtx: execCtx,
	}
}

func (f *seqStampFixture) th() *TxnHandler { return f.ses.GetTxnHandler() }

func (f *seqStampFixture) raiseObligations(t *testing.T, obs ...process.SequenceObligation) *process.SequenceObligationSet {
	t.Helper()
	set := f.ses.getSequenceObligationSet()
	require.NotNil(t, set)
	for _, ob := range obs {
		require.NoError(t, set.Raise(ob))
	}
	return set
}

// wantSeqStampSQL renders the exact statement the stamp must emit for one
// obligation, built from the production emitters so any emission-shape
// change fails the comparison.
func wantSeqStampSQL(ob process.SequenceObligation) string {
	floor := formatSequenceObligationFloor(ob)
	cmp := "<"
	if ob.Minus {
		cmp = ">"
	}
	return "update " + sequenceCommitTableName(ob.Db, ob.Table) +
		" set last_seq_num = " + floor + ", is_called = true where last_seq_num " +
		cmp + " " + floor + " or (last_seq_num = " + floor + " and is_called = false)"
}

func wantStampedRow(stored seqStoreRow, last uint64, called bool) seqStoreRow {
	stored.last = last
	stored.called = called
	return stored
}

// TestSequenceCommitStampPreserveRestore pins the preserve-or-restore decision
// of design §4 D item 5c/7 (I1/K1 composition oracle, §9): one conditional
// UPDATE per obligation restores (floor,true) only when the surviving
// position is unsafe for the pass's issued values, and preserves surviving
// advances (e.g. transaction-local merge reconciliation) and safe older-pass
// positions byte-for-byte.
func TestSequenceCommitStampPreserveRestore(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ob         process.SequenceObligation
		stored     seqStoreRow
		wantLast   uint64
		wantCalled bool
		wantMatch  bool
	}{
		{
			// I1 composition: a transaction-local merge reconcile advanced
			// the workspace counter past the allocation floor; the stamp
			// must preserve (w,true), never stamp back to f.
			name:       "ascending preserves surviving reconcile advance",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 2},
			stored:     seqStoreRow{last: 3, called: true},
			wantLast:   3,
			wantCalled: true,
		},
		{
			// K1 rewind-only counterpart: statement rollback rewound the
			// workspace behind the floor; restore the issued position.
			name:       "ascending restores rewound position behind floor",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3},
			stored:     seqStoreRow{last: 1, called: true},
			wantLast:   3,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// Equality while still uncalled is unsafe: the stored value was
			// never emitted, but the floor was — restore and mark called.
			name:       "ascending restores equal-uncalled position",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3},
			stored:     seqStoreRow{last: 3, called: false},
			wantLast:   3,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// Natural case (i): counter already at the floor and called —
			// the no-op elision arm.
			name:       "ascending preserves equal-called position",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3},
			stored:     seqStoreRow{last: 3, called: true},
			wantLast:   3,
			wantCalled: true,
		},
		{
			// CYCLE older-pass arm: an ascending position beyond the floor
			// belongs to an older pass and is safe to preserve (item 5c iv).
			name:       "ascending cycle preserves older-pass beyond-floor uncalled",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3},
			stored:     seqStoreRow{last: 9, called: false},
			wantLast:   9,
			wantCalled: false,
		},
		{
			// Descending mirror of the rewind-only case: a position
			// numerically above the floor is behind in allocation order;
			// per UnsafeToPreserve the W > floor arm restores it.
			name:       "descending restores position above floor",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 10, Floor: 7, Minus: true},
			stored:     seqStoreRow{last: 9, called: true},
			wantLast:   7,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// Descending CYCLE arm: W above the floor even uncalled walks
			// the continuation back through issued values; restore.
			name:       "descending cycle restores above-floor uncalled",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 10, Floor: 7, Minus: true},
			stored:     seqStoreRow{last: 9, called: false},
			wantLast:   7,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// Descending older-pass arm: a position numerically below the
			// floor is beyond it in allocation order; preserve.
			name:       "descending preserves older-pass position below floor",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 10, Floor: 7, Minus: true},
			stored:     seqStoreRow{last: 2, called: false},
			wantLast:   2,
			wantCalled: false,
		},
		{
			name:       "descending preserves equal-called position",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 10, Floor: 7, Minus: true},
			stored:     seqStoreRow{last: 7, called: true},
			wantLast:   7,
			wantCalled: true,
		},
		{
			// Unsigned domain: restore (floor,true) with the floor rendered
			// as an unsigned literal.
			name:       "unsigned ascending restores rewound position",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 4, Unsigned: true},
			stored:     seqStoreRow{last: 2, called: true, unsigned: true},
			wantLast:   4,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			name:       "unsigned descending restores position above floor",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 110, Floor: 99, Unsigned: true, Minus: true},
			stored:     seqStoreRow{last: 101, called: true, unsigned: true},
			wantLast:   99,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// Signed negative domain: the floor literal carries the sign.
			name:       "signed negative ascending restores rewound position",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: seqNegBits(-20), Floor: seqNegBits(-7)},
			stored:     seqStoreRow{last: seqNegBits(-12), called: true},
			wantLast:   seqNegBits(-7),
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// #30359 pre-PR review forward-floor invariant: a statement
			// rewind AFTER an already-observed emission restored the
			// pending 10 while the obligation floor stood at 60, and the
			// widened [10,60] envelope kept the frontier. The surviving
			// (10,called) reads behind the floor, so the stamp restores 60
			// instead of persisting the regressed position — whose next
			// transaction would march back toward the observed 60.
			name:       "ascending restores rewound-regressed position behind extended envelope",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 10, Floor: 60},
			stored:     seqStoreRow{last: 10, called: true},
			wantLast:   60,
			wantCalled: true,
			wantMatch:  true,
		},
		{
			// Descending mirror of the extended-envelope rewind.
			name:       "descending restores rewound-regressed position behind extended envelope",
			ob:         process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 10, Floor: 2, Minus: true},
			stored:     seqStoreRow{last: 10, called: true},
			wantLast:   2,
			wantCalled: true,
			wantMatch:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSeqStampFixture(t, map[string]seqStampDatabase{
				"dst": {rels: map[string]seqStampRelation{"s": {id: 9001}}},
			})
			f.store.add("dst", "s", tc.stored)

			err := f.th().stampSequenceObligationsAtCommitUnsafe(
				f.execCtx, f.ses, []process.SequenceObligation{tc.ob})
			require.NoError(t, err)

			// Exactly one conditional UPDATE per surviving obligation, in
			// the exact emission shape.
			require.Equal(t, []string{wantSeqStampSQL(tc.ob)}, f.exec.sqls)
			row, ok := f.store.get("dst", "s")
			require.True(t, ok)
			require.Equal(t, wantStampedRow(tc.stored, tc.wantLast, tc.wantCalled), row)
			require.Equal(t, []bool{tc.wantMatch}, f.exec.matched)
			// The write joins the committing transaction and keeps it alive.
			require.Len(t, f.exec.opts, 1)
			require.Same(t, f.op, f.exec.opts[0].Txn())
		})
	}
}

// TestSequenceCommitStampLifecycleResolution pins design §4 D item 5a/6
// (J3): obligations resolve through the committing transaction's own final
// catalog view; a self-deleted generation retires unstamped without error,
// and a replaced identity (new relation id) is never stamped.
func TestSequenceCommitStampLifecycleResolution(t *testing.T) {
	t.Run("self deleted database retires unstamped", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			// The database is absent from the final view (OkExpectedEOB).
		})
		f.store.add("dst", "s", seqStoreRow{last: 1, called: true})
		ob := process.SequenceObligation{RelID: 91, Db: "dst", Table: "s", PassStart: 1, Floor: 3}

		require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(
			f.execCtx, f.ses, []process.SequenceObligation{ob}))
		require.Empty(t, f.exec.sqls, "a deleted generation must never be stamped")
		row, ok := f.store.get("dst", "s")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, called: true}, row)
	})

	t.Run("self deleted relation retires unstamped", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			"dst": {rels: map[string]seqStampRelation{}},
		})
		f.store.add("dst", "s", seqStoreRow{last: 1, called: true})
		ob := process.SequenceObligation{RelID: 92, Db: "dst", Table: "s", PassStart: 1, Floor: 3}

		require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(
			f.execCtx, f.ses, []process.SequenceObligation{ob}))
		require.Empty(t, f.exec.sqls)
		row, ok := f.store.get("dst", "s")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, called: true}, row)
	})

	t.Run("replaced identity skipped", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			// Same name, new generation: relation id 42 disagrees with the
			// obligation's 93.
			"dst": {rels: map[string]seqStampRelation{"s": {id: 42}}},
		})
		f.store.add("dst", "s", seqStoreRow{last: 1, called: true})
		ob := process.SequenceObligation{RelID: 93, Db: "dst", Table: "s", PassStart: 1, Floor: 3}

		require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(
			f.execCtx, f.ses, []process.SequenceObligation{ob}))
		require.Empty(t, f.exec.sqls, "an old obligation never applies to the replacement")
		row, ok := f.store.get("dst", "s")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, called: true}, row)
	})

	t.Run("mixed lifecycle set stamps only surviving identities", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			"dst":  {rels: map[string]seqStampRelation{"s": {id: 10}}},
			"reid": {rels: map[string]seqStampRelation{"s": {id: 44}}},
			// "gone" resolves to a lifecycle miss.
			"gone": {err: moerr.GetOkExpectedEOB()},
		})
		f.store.add("dst", "s", seqStoreRow{last: 1, called: true})
		surviving := process.SequenceObligation{RelID: 10, Db: "dst", Table: "s", PassStart: 1, Floor: 3}
		deleted := process.SequenceObligation{RelID: 11, Db: "gone", Table: "s", PassStart: 1, Floor: 3}
		replaced := process.SequenceObligation{RelID: 12, Db: "reid", Table: "s", PassStart: 1, Floor: 3}

		require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(
			f.execCtx, f.ses, []process.SequenceObligation{deleted, surviving, replaced}))
		require.Equal(t, []string{wantSeqStampSQL(surviving)}, f.exec.sqls)
		row, ok := f.store.get("dst", "s")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 3, called: true}, row)
	})
}

// TestSequenceCommitStampOrdering pins design §4 D item 8: pre-commit
// processing walks obligations in ascending relation-id order (Snapshot is
// deterministic), one conditional UPDATE per obligation.
func TestSequenceCommitStampOrdering(t *testing.T) {
	f := newSeqStampFixture(t, map[string]seqStampDatabase{
		"dst": {rels: map[string]seqStampRelation{
			"s10": {id: 10},
			"s20": {id: 20},
			"s30": {id: 30},
		}},
	})
	f.store.add("dst", "s10", seqStoreRow{last: 1, called: true})
	f.store.add("dst", "s20", seqStoreRow{last: 1, called: true})
	f.store.add("dst", "s30", seqStoreRow{last: 1, called: true})
	ob30 := process.SequenceObligation{RelID: 30, Db: "dst", Table: "s30", PassStart: 1, Floor: 3}
	ob10 := process.SequenceObligation{RelID: 10, Db: "dst", Table: "s10", PassStart: 1, Floor: 3}
	ob20 := process.SequenceObligation{RelID: 20, Db: "dst", Table: "s20", PassStart: 1, Floor: 3}

	// Raise out of order; Snapshot must still order by relation id.
	f.raiseObligations(t, ob30, ob10, ob20)
	entries := f.ses.getSequenceObligationSet().Snapshot()
	require.Equal(t, []uint64{10, 20, 30},
		[]uint64{entries[0].RelID, entries[1].RelID, entries[2].RelID})

	require.NoError(t, f.th().applySequenceObligationsBeforeCommitUnsafe(f.execCtx))
	require.Equal(t, []string{
		wantSeqStampSQL(ob10), wantSeqStampSQL(ob20), wantSeqStampSQL(ob30),
	}, f.exec.sqls)
	for _, tbl := range []string{"s10", "s20", "s30"} {
		row, ok := f.store.get("dst", tbl)
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 3, called: true}, row)
	}
	// A consumed set must never stamp a subsequent transaction.
	require.Empty(t, f.ses.getSequenceObligationSet().Snapshot())
	// A second apply with no obligations stamps nothing.
	require.NoError(t, f.th().applySequenceObligationsBeforeCommitUnsafe(f.execCtx))
	require.Len(t, f.exec.sqls, 3)
	require.Equal(t, 0, f.op.rollbackCalls)
}

// TestSequenceCommitStampIdempotentRetry pins the monotonic shape of the
// stamp: re-running the stamp against an already-stamped row moves nothing
// (the conditional matches no arm), so ambiguous commit outcomes or a
// repeated stamp cannot regress a preserved position.
func TestSequenceCommitStampIdempotentRetry(t *testing.T) {
	f := newSeqStampFixture(t, map[string]seqStampDatabase{
		"dst": {rels: map[string]seqStampRelation{"s": {id: 9001}}},
	})
	f.store.add("dst", "s", seqStoreRow{last: 4, called: true})
	ob := process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 10}
	entries := []process.SequenceObligation{ob}

	require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(f.execCtx, f.ses, entries))
	afterFirst := f.store.snapshot()
	row, ok := f.store.get("dst", "s")
	require.True(t, ok)
	require.Equal(t, seqStoreRow{last: 10, called: true}, row)

	require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(f.execCtx, f.ses, entries))
	require.Equal(t, afterFirst, f.store.snapshot(), "a repeated stamp is idempotent")
	require.Equal(t, []bool{true, false}, f.exec.matched)
	require.Equal(t,
		[]string{wantSeqStampSQL(ob), wantSeqStampSQL(ob)}, f.exec.sqls)
}

// TestSequenceCommitStampFailureTerminalRollback pins the terminal-failure
// contract of design §4 D item 5: a storage resolution error or a stamp
// execution error fails the apply, and the apply joins the stamp error with
// a whole-transaction rollback (a partial stamp must never separate from
// the physical commit).
func TestSequenceCommitStampFailureTerminalRollback(t *testing.T) {
	t.Run("identity resolution failure rolls the transaction back", func(t *testing.T) {
		storageErr := errors.New("dst storage down")
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			"dst": {err: storageErr},
		})
		ob := process.SequenceObligation{RelID: 10, Db: "dst", Table: "s", PassStart: 1, Floor: 3}
		f.raiseObligations(t, ob)

		err := f.th().applySequenceObligationsBeforeCommitUnsafe(f.execCtx)
		require.ErrorContains(t, err, "dst storage down")
		require.Empty(t, f.exec.sqls, "nothing may be stamped when resolution failed")
		require.Equal(t, 1, f.op.rollbackCalls, "terminal failure must roll the transaction back")
		require.Empty(t, f.ses.getSequenceObligationSet().Snapshot(),
			"a rolled-back transaction's obligations die with it")
	})

	t.Run("stamp execution failure keeps partial progress with the aborted transaction", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			"dst": {rels: map[string]seqStampRelation{
				"s10": {id: 10},
				"s20": {id: 20},
			}},
		})
		f.store.add("dst", "s10", seqStoreRow{last: 1, called: true})
		f.store.add("dst", "s20", seqStoreRow{last: 1, called: true})
		// The second obligation's storage rejects the conditional update.
		f.exec.failOn = func(db, table string) bool { return db == "dst" && table == "s20" }
		f.exec.failErr = errors.New("s20 update rejected")
		f.raiseObligations(t,
			process.SequenceObligation{RelID: 10, Db: "dst", Table: "s10", PassStart: 1, Floor: 3},
			process.SequenceObligation{RelID: 20, Db: "dst", Table: "s20", PassStart: 1, Floor: 3},
		)

		err := f.th().applySequenceObligationsBeforeCommitUnsafe(f.execCtx)
		require.ErrorContains(t, err, "s20 update rejected")
		require.Equal(t, 1, f.op.rollbackCalls)
		// The first stamp already landed; both partial stamp and rolled-back
		// transaction die together — nothing else may stamp afterwards.
		row, ok := f.store.get("dst", "s10")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 3, called: true}, row)
		row, ok = f.store.get("dst", "s20")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, called: true}, row)
		require.Empty(t, f.ses.getSequenceObligationSet().Snapshot())
	})

	t.Run("stamp error surface without apply wrapper", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			"dst": {rels: map[string]seqStampRelation{"s": {id: 9001}}},
		})
		f.store.add("dst", "s", seqStoreRow{last: 1, called: true})
		f.exec.failErr = errors.New("executor unavailable")
		ob := process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3}

		err := f.th().stampSequenceObligationsAtCommitUnsafe(f.execCtx, f.ses, []process.SequenceObligation{ob})
		require.ErrorContains(t, err, "executor unavailable")
		row, ok := f.store.get("dst", "s")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, called: true}, row)
	})

	t.Run("missing internal sql executor", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{
			"dst": {rels: map[string]seqStampRelation{"s": {id: 9001}}},
		})
		rt := moruntime.ServiceRuntime(f.ses.GetService())
		require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, f.exec))
		ob := process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3}

		err := f.th().stampSequenceObligationsAtCommitUnsafe(f.execCtx, f.ses, []process.SequenceObligation{ob})
		require.ErrorContains(t, err, "missing internal SQL executor")
		require.Empty(t, f.exec.sqls)
	})

	t.Run("missing storage or txn operator", func(t *testing.T) {
		f := newSeqStampFixture(t, map[string]seqStampDatabase{})
		bare := &TxnHandler{}
		ob := process.SequenceObligation{RelID: 9001, Db: "dst", Table: "s", PassStart: 1, Floor: 3}

		err := bare.stampSequenceObligationsAtCommitUnsafe(f.execCtx, f.ses, []process.SequenceObligation{ob})
		require.ErrorContains(t, err, "missing storage or txn operator")
		require.Empty(t, f.exec.sqls)
	})
}

// TestSequenceCommitStampQuotingPinsIdentifierEscaping covers the internal
// SQL identifier escaping of the stamp so a backtick-bearing sequence name
// cannot break or confuse the conditional UPDATE.
func TestSequenceCommitStampQuotingPinsIdentifierEscaping(t *testing.T) {
	f := newSeqStampFixture(t, map[string]seqStampDatabase{
		"d`st": {rels: map[string]seqStampRelation{"s`eq": {id: 9001}}},
	})
	f.store.add("d`st", "s`eq", seqStoreRow{last: 1, called: true})
	ob := process.SequenceObligation{RelID: 9001, Db: "d`st", Table: "s`eq", PassStart: 1, Floor: 3}

	require.NoError(t, f.th().stampSequenceObligationsAtCommitUnsafe(
		f.execCtx, f.ses, []process.SequenceObligation{ob}))
	require.Equal(t,
		[]string{"update `d``st`.`s``eq` set last_seq_num = 3, is_called = true " +
			"where last_seq_num < 3 or (last_seq_num = 3 and is_called = false)"},
		f.exec.sqls)
	row, ok := f.store.get("d`st", "s`eq")
	require.True(t, ok)
	require.Equal(t, seqStoreRow{last: 3, called: true}, row)
}
