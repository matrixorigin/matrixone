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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	txn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------
// Merge sequence reconciliation harness (design §4 B, §9 B/I1 oracles).
// The fake store and the conditional-UPDATE interpreter live in
// sequence_commit_test.go; this file adds the committed-transaction
// executor (ExecTxn keeps a working copy so an aborted attempt leaves the
// store untouched — the failure-atomicity model), the visibility-partitioned
// catalog, and the 7-column metadata-row result builder.
// ---------------------------------------------------------------------

type seqReconcileEvent struct {
	route string // "txn": the autonomous reconcile unit; "local": the in-caller partition
	sql   string
}

type seqReconcileExecutor struct {
	store        *seqStore
	mp           *mpool.MPool
	autonomousOp client.TxnOperator
	callerOp     client.TxnOperator
	attempts     int
	outcomes     []string // "commit" or "rollback" per autonomous attempt
	events       []seqReconcileEvent
	localOps     []client.TxnOperator // the operator every in-caller statement bound
	locks        [][]string
	lockErr      error
}

var _ executor.SQLExecutor = (*seqReconcileExecutor)(nil)

func (e *seqReconcileExecutor) ExecTxn(_ context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	e.attempts++
	working := e.store.clone()
	te := &seqReconcileTxnExecutor{owner: e, working: working, op: e.autonomousOp}
	if err := fn(te); err != nil {
		e.outcomes = append(e.outcomes, "rollback")
		e.locks = append(e.locks, te.locks)
		return err
	}
	e.store.replace(working)
	e.outcomes = append(e.outcomes, "commit")
	e.locks = append(e.locks, te.locks)
	return nil
}

// Exec is the in-caller route: the transaction-local partition binds the
// merge statement's own operator, so the counter growth shares the merged
// rows' commit/rollback fate. Each statement's writes land on the store only
// if the statement itself succeeded — a statement rewind is the caller's
// business, not the reconcile unit's.
func (e *seqReconcileExecutor) Exec(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
	e.localOps = append(e.localOps, opts.Txn())
	rows := e.store.clone()
	res, err := e.execOn(rows, "local", sql)
	if err != nil {
		return executor.Result{}, err
	}
	e.store.replace(rows)
	return res, nil
}

func (e *seqReconcileExecutor) execOn(rows seqRows, route, sql string) (executor.Result, error) {
	if db, table, ok := parseSeqSelectSequenceRow(sql); ok {
		e.events = append(e.events, seqReconcileEvent{route: route, sql: sql})
		row, found := rows[[2]string{db, table}]
		if !found {
			return executor.Result{}, moerr.NewNoSuchTableNoCtx(db, table)
		}
		return seqSelectSequenceRowResult(e.mp, row)
	}
	if db, table, floorLit, cmp, ok := parseSeqConditionalUpdate(sql); ok {
		e.events = append(e.events, seqReconcileEvent{route: route, sql: sql})
		matched, err := applySeqConditionalUpdate(rows, db, table, floorLit, cmp)
		if err != nil {
			return executor.Result{}, err
		}
		res := executor.Result{}
		if matched {
			res.AffectedRows = 1
		}
		return res, nil
	}
	return executor.Result{}, moerr.NewInternalErrorNoCtx("sequence reconcile fake: unexpected SQL: " + sql)
}

type seqReconcileTxnExecutor struct {
	owner   *seqReconcileExecutor
	working seqRows
	op      client.TxnOperator
	locks   []string
}

func (te *seqReconcileTxnExecutor) Use(string) {}

func (te *seqReconcileTxnExecutor) LockTable(table string) error {
	te.locks = append(te.locks, table)
	return te.owner.lockErr
}

func (te *seqReconcileTxnExecutor) Exec(sql string, _ executor.StatementOption) (executor.Result, error) {
	return te.owner.execOn(te.working, "txn", sql)
}

func (te *seqReconcileTxnExecutor) Txn() client.TxnOperator { return te.op }

// seqSelectSequenceRowResult renders one 7-column sequence metadata row
// (last, min, max, start, increment, cycle, is_called) exactly as the
// SELECT ... FOR UPDATE of the reconcile unit reads it.
func seqSelectSequenceRowResult(mp *mpool.MPool, row seqStoreRow) (executor.Result, error) {
	numT := types.T_int64
	if row.unsigned {
		numT = types.T_uint64
	}
	if row.narrowT != 0 {
		numT = row.narrowT
	}
	numType := numT.ToType()
	mr := executor.NewMemResult([]types.Type{
		numType, numType, numType, numType,
		types.T_int64.ToType(),
		types.T_bool.ToType(), types.T_bool.ToType(),
	}, mp)
	mr.NewBatchWithRowCount(1)
	nums := []uint64{row.last, row.minv, row.maxv, row.start}
	switch numT {
	case types.T_int16:
		for col, v := range nums {
			if err := executor.AppendFixedRows[int16](mr, col, []int16{int16(v)}); err != nil {
				return executor.Result{}, err
			}
		}
	case types.T_int32:
		for col, v := range nums {
			if err := executor.AppendFixedRows[int32](mr, col, []int32{int32(v)}); err != nil {
				return executor.Result{}, err
			}
		}
	case types.T_uint16:
		for col, v := range nums {
			if err := executor.AppendFixedRows[uint16](mr, col, []uint16{uint16(v)}); err != nil {
				return executor.Result{}, err
			}
		}
	case types.T_uint32:
		for col, v := range nums {
			if err := executor.AppendFixedRows[uint32](mr, col, []uint32{uint32(v)}); err != nil {
				return executor.Result{}, err
			}
		}
	case types.T_uint64:
		for col, v := range nums {
			if err := executor.AppendFixedRows[uint64](mr, col, []uint64{v}); err != nil {
				return executor.Result{}, err
			}
		}
	default:
		for col, v := range nums {
			if err := executor.AppendFixedRows[int64](mr, col, []int64{int64(v)}); err != nil {
				return executor.Result{}, err
			}
		}
	}
	if err := executor.AppendFixedRows[int64](mr, 4, []int64{row.incr}); err != nil {
		return executor.Result{}, err
	}
	if err := executor.AppendFixedRows[bool](mr, 5, []bool{row.cycle}); err != nil {
		return executor.Result{}, err
	}
	if err := executor.AppendFixedRows[bool](mr, 6, []bool{row.called}); err != nil {
		return executor.Result{}, err
	}
	return mr.GetResult(), nil
}

// seqReconcileCatalog splits resolution by transaction visibility: committed
// generations resolve through any operator (the autonomous unit's too),
// transaction-local generations only through the merge transaction's
// operator — design §4 B step 1's classification source. ordinary marks
// identities whose resolved relation is a same-named ordinary table (the
// relkind-guard oracles); everything else resolves as a sequence.
type seqReconcileCatalog struct {
	committed map[[2]string]uint64
	local     map[[2]string]uint64
	ordinary  map[[2]string]bool
	callerOp  client.TxnOperator
	seenOps   []client.TxnOperator
	// dbCtxHook, when set, intercepts every committed Database resolution
	// with the context the production code passed; oracles capture it for
	// deadline assertions or hold the resolution until it fires. A non-nil
	// return aborts the resolution with that error.
	dbCtxHook func(ctx context.Context) error
}

func (c *seqReconcileCatalog) engine(ctrl *gomock.Controller) engine.Engine {
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, name string, op client.TxnOperator) (engine.Database, error) {
			c.seenOps = append(c.seenOps, op)
			if c.dbCtxHook != nil {
				if err := c.dbCtxHook(ctx); err != nil {
					return nil, err
				}
			}
			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, tbl string, _ any) (engine.Relation, error) {
					key := [2]string{name, tbl}
					kind := catalog.SystemSequenceRel
					if c.ordinary[key] {
						kind = catalog.SystemOrdinaryRel
					}
					if id, ok := c.committed[key]; ok {
						return seqReconcileRel(ctrl, id, kind), nil
					}
					if op == c.callerOp {
						if id, ok := c.local[key]; ok {
							return seqReconcileRel(ctrl, id, kind), nil
						}
					}
					return nil, moerr.NewNoSuchTableNoCtx(name, tbl)
				}).AnyTimes()
			return db, nil
		}).AnyTimes()
	return eng
}

func seqReconcileRel(ctrl *gomock.Controller, id uint64, kind string) engine.Relation {
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableID(gomock.Any()).Return(id).AnyTimes()
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		&engine.PropertiesDef{Properties: []engine.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: kind},
		}},
	}, nil).AnyTimes()
	return rel
}

// seqReconcileBackgroundExec answers backgroundExecTxnOperator with the
// merge transaction's operator so the transaction-local partition can bind
// its internal statements to the caller transaction.
type seqReconcileBackgroundExec struct {
	backgroundExecTest
	op client.TxnOperator
}

func (bh *seqReconcileBackgroundExec) getTxn() TxnOperator { return bh.op }

type seqReconcileFixture struct {
	ses          *Session
	eng          engine.Engine
	catalog      *seqReconcileCatalog
	store        *seqStore
	exec         *seqReconcileExecutor
	bh           *seqReconcileBackgroundExec
	callerOp     *testTxnOp
	autonomousOp *testTxnOp
}

func newSeqReconcileFixture(t *testing.T, catalog *seqReconcileCatalog) *seqReconcileFixture {
	t.Helper()
	if catalog.committed == nil {
		catalog.committed = map[[2]string]uint64{}
	}
	if catalog.local == nil {
		catalog.local = map[[2]string]uint64{}
	}
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)

	callerOp := newTestTxnOp()
	callerOp.meta = txn.TxnMeta{ID: []byte{0xC0}, Status: txn.TxnStatus_Active}
	autonomousOp := newTestTxnOp()
	autonomousOp.meta = txn.TxnMeta{ID: []byte{0xA0}, Status: txn.TxnStatus_Active}
	catalog.callerOp = callerOp
	eng := catalog.engine(ctrl)

	ses.proc = testutil.NewProc(t)
	// No defines.EngineKey on proc.Ctx on purpose: the compiled-statement
	// attach is not guaranteed in the merge driver envelope (Challenger
	// topology `mo-service -launch` exposed this). The reconcile must
	// source the engine from the txn handler instead.
	ses.txnHandler = InitTxnHandler("", eng, context.Background(), callerOp)
	ses.proc.Base.TxnOperator = callerOp

	store := newSeqStore()
	recExec := &seqReconcileExecutor{
		store: store, mp: ses.proc.Mp(),
		autonomousOp: autonomousOp, callerOp: callerOp,
	}
	installSeqTestExecutor(t, ses, recExec)

	bh := &seqReconcileBackgroundExec{op: callerOp}
	bh.init()
	return &seqReconcileFixture{
		ses: ses, eng: eng, catalog: catalog, store: store,
		exec: recExec, bh: bh, callerOp: callerOp, autonomousOp: autonomousOp,
	}
}

// newWatermarkTracker builds a merge-scoped tracker whose items are the
// final watermarks of an apply, as capture would have accumulated them.
// Keys are normalized as the lctn=1 (case-folding) session would, matching
// the historical default.
func newWatermarkTracker(dstDB string, wms ...*sequenceWatermark) *sequenceWatermarkTracker {
	return newWatermarkTrackerLCTN(dstDB, 1, wms...)
}

// newWatermarkTrackerLCTN keys the watermarks under the given effective
// lower_case_table_names: fold at 1, preserve spelling at 0.
func newWatermarkTrackerLCTN(dstDB string, lctn int64, wms ...*sequenceWatermark) *sequenceWatermarkTracker {
	tr := &sequenceWatermarkTracker{
		dstDB: dstDB,
		lctn:  lctn,
		cols:  map[int]sequenceWatermarkKey{},
		items: map[sequenceWatermarkKey]*sequenceWatermark{},
	}
	for _, wm := range wms {
		key := sequenceWatermarkKey{db: wm.db, table: wm.table}
		if lctn == 1 {
			key.db = strings.ToLower(key.db)
			key.table = strings.ToLower(key.table)
		}
		wm.key = key
		tr.items[key] = wm
	}
	return tr
}

// seqWmRange builds a watermark covering the merged-key range [minKey,
// maxKey] through the production accumulate path, so constructed
// watermarks carry exactly the per-storage-domain arms real capture
// produces (#30359 pre-PR review N2/N3): a test literal that encodes a
// stale struct shape can never compile, only drift — accumulate cannot
// drift. rowsCount is set directly because these fixtures model already
// applied batches rather than replaying capture.
func seqWmRange(db, table string, minKey, maxKey uint64, rows int64) *sequenceWatermark {
	wm := &sequenceWatermark{db: db, table: table, rowsCount: rows}
	wm.accumulate(sequenceWatermarkUintValue(minKey))
	if maxKey != minKey {
		wm.accumulate(sequenceWatermarkUintValue(maxKey))
	}
	return wm
}

func wantReconcileSelectSQL(wm *sequenceWatermark) string {
	return "select last_seq_num, min_value, max_value, start_value, increment_value, cycle, is_called from " +
		sequenceCommitTableName(wm.db, wm.table) + " for update"
}

func wantReconcileUpdateSQL(wm *sequenceWatermark, lit string, descending bool) string {
	cmp := "<"
	if descending {
		cmp = ">"
	}
	return "update " + sequenceCommitTableName(wm.db, wm.table) +
		" set last_seq_num = " + lit + ", is_called = true where last_seq_num " +
		cmp + " " + lit + " or (last_seq_num = " + lit + " and is_called = false)"
}

func reconcileEventSQLs(events []seqReconcileEvent, route string) []string {
	var out []string
	for _, ev := range events {
		if ev.route == route {
			out = append(out, ev.sql)
		}
	}
	return out
}

// ---------------------------------------------------------------------
// Task B — watermark capture matrix (design §9 B).
// ---------------------------------------------------------------------

func seqNextvalExpr(args ...string) *plan.Expr {
	exprs := make([]*plan.Expr, len(args))
	for i, a := range args {
		exprs[i] = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: a},
		}}}
	}
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "nextval"},
		Args: exprs,
	}}}
}

func seqFnExpr(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name},
		Args: args,
	}}}
}

func seqDefaultExpr(e *plan.Expr) *plan.Default {
	return &plan.Default{Expr: e}
}

// TestSequenceWatermarkTrackerDefaults pins the reconcilable-shape contract
// of design §4 B: only a single nextval with constant arguments (a
// planner-inserted assignment cast stripped), owned by or unpinned at the
// destination database, is reconciled; transformed, dynamic, and
// foreign-pinned defaults are left alone.
func TestSequenceWatermarkTrackerDefaults(t *testing.T) {
	const dstDB = "db1"
	for _, tc := range []struct {
		name string
		def  *plan.Default
		want bool
		key  sequenceWatermarkKey // expected when want
	}{
		{"bare nextval", seqDefaultExpr(seqNextvalExpr("s")), true, sequenceWatermarkKey{"db1", "s"}},
		{"nextval pinned to dst db", seqDefaultExpr(seqNextvalExpr("s2", "db1")), true, sequenceWatermarkKey{"db1", "s2"}},
		{"assignment cast wrapped nextval", seqDefaultExpr(seqFnExpr("cast", seqNextvalExpr("s3"))), true, sequenceWatermarkKey{"db1", "s3"}},
		{"nested assignment casts stripped", seqDefaultExpr(seqFnExpr("cast", seqFnExpr("cast", seqNextvalExpr("s4")))), true, sequenceWatermarkKey{"db1", "s4"}},
		{"uppercase dst pin lowercased", seqDefaultExpr(seqNextvalExpr("S5", "DB1")), true, sequenceWatermarkKey{"db1", "s5"}},
		{"transformed nextval plus one", seqDefaultExpr(seqFnExpr("add", seqNextvalExpr("s6"), &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "1"}}}})), false, sequenceWatermarkKey{}},
		{"coalesced nextval", seqDefaultExpr(seqFnExpr("coalesce", seqNextvalExpr("s7"), seqNextvalExpr("s8"))), false, sequenceWatermarkKey{}},
		{"foreign db pin", seqDefaultExpr(seqNextvalExpr("s9", "otherdb")), false, sequenceWatermarkKey{}},
		{"cast with non-type second argument rejected", seqDefaultExpr(seqFnExpr("cast", seqNextvalExpr("s10"), seqNextvalExpr("s11"))), false, sequenceWatermarkKey{}},
		{"catalog assignment cast with type target stripped", seqDefaultExpr(seqFnExpr("cast", seqNextvalExpr("s12"),
			&plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})), true, sequenceWatermarkKey{"db1", "s12"}},
		// Planner-owned wrapper family (VARCHAR/TEXT defaults carry
		// "cast_strict", MEDIUMINT the assignmentCastFunctionName pick):
		// every variant must unwrap or those column types silently stop
		// reconciling (#30359 review F1).
		{"cast_strict catalog wrapper stripped", seqDefaultExpr(seqFnExpr("cast_strict", seqNextvalExpr("s13"),
			&plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 32}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})), true, sequenceWatermarkKey{"db1", "s13"}},
		{"cast_assign catalog wrapper stripped", seqDefaultExpr(seqFnExpr("cast_assign", seqNextvalExpr("s14"),
			&plan.Expr{Typ: plan.Type{Id: int32(types.T_int32), Width: 24}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})), true, sequenceWatermarkKey{"db1", "s14"}},
		{"cast_ignore catalog wrapper stripped", seqDefaultExpr(seqFnExpr("cast_ignore", seqNextvalExpr("s15"),
			&plan.Expr{Typ: plan.Type{Id: int32(types.T_int32), Width: 24}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})), true, sequenceWatermarkKey{"db1", "s15"}},
		{"cast_strict single-argument wrapper stripped", seqDefaultExpr(seqFnExpr("cast_strict", seqNextvalExpr("s16"))), true, sequenceWatermarkKey{"db1", "s16"}},
		{"mixed family nested wrappers stripped", seqDefaultExpr(seqFnExpr("cast_strict", seqFnExpr("cast_assign", seqNextvalExpr("s17")))), true, sequenceWatermarkKey{"db1", "s17"}},
		{"cast_strict with non-type second argument rejected", seqDefaultExpr(seqFnExpr("cast_strict", seqNextvalExpr("s18"), seqNextvalExpr("s19"))), false, sequenceWatermarkKey{}},
		{"cast_strict wrapped transformation stays rejected", seqDefaultExpr(seqFnExpr("cast_strict", seqFnExpr("add", seqNextvalExpr("s20"), &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "1"}}}}))), false, sequenceWatermarkKey{}},
		{"dynamic sequence name", seqDefaultExpr(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "nextval"},
			Args: []*plan.Expr{seqFnExpr("col", seqNextvalExpr("ref"))},
		}}}), false, sequenceWatermarkKey{}},
		{"nil default", nil, false, sequenceWatermarkKey{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			// The merge destination (baseRel) carries the default being
			// classified; the row source (tarRel) supplies the apply-row
			// layout through tblStuff.def.
			dstRel := mock_frontend.NewMockRelation(ctrl)
			dstRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
				DbName: dstDB, Name: "dst",
				Cols: []*plan.ColDef{{Name: "c", Default: tc.def}},
			}).AnyTimes()
			srcRel := mock_frontend.NewMockRelation(ctrl)
			srcRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
				DbName: "br1", Name: "src",
				Cols: []*plan.ColDef{{Name: "c"}},
			}).AnyTimes()
			var tblStuff tableStuff
			tblStuff.baseRel = dstRel
			tblStuff.tarRel = srcRel
			tblStuff.def.colNames = []string{"c"}

			tr, err := newSequenceWatermarkTracker(context.Background(), tblStuff, 1)
			require.NoError(t, err)
			require.Equal(t, dstDB, tr.dstDB)
			if !tc.want {
				require.Empty(t, tr.cols)
				return
			}
			require.Equal(t, map[int]sequenceWatermarkKey{0: tc.key}, tr.cols)
		})
	}

	t.Run("defaults are taken from the destination relation", func(t *testing.T) {
		// Regression: only destination-side (baseRel) defaults are tracked;
		// a default that exists only on the row source (tarRel) never enters
		// the tracker and never reconciles the source database's sequence.
		ctrl := gomock.NewController(t)
		dstRel := mock_frontend.NewMockRelation(ctrl)
		dstRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: dstDB, Name: "dst",
			Cols: []*plan.ColDef{{Name: "c"}},
		}).AnyTimes()
		srcRel := mock_frontend.NewMockRelation(ctrl)
		srcRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "br1", Name: "src",
			Cols: []*plan.ColDef{{Name: "c", Default: seqDefaultExpr(seqNextvalExpr("s"))}},
		}).AnyTimes()
		var tblStuff tableStuff
		tblStuff.baseRel = dstRel
		tblStuff.tarRel = srcRel
		tblStuff.def.colNames = []string{"c"}

		tr, err := newSequenceWatermarkTracker(context.Background(), tblStuff, 1)
		require.NoError(t, err)
		require.Equal(t, dstDB, tr.dstDB)
		require.Empty(t, tr.cols)
	})

	t.Run("renamed source column still maps to the apply row layout", func(t *testing.T) {
		// A branch-local rename must not lose the watermark: the destination
		// default resolves through the logical name to the source column and
		// then to its index in tblStuff.def.colNames.
		ctrl := gomock.NewController(t)
		dstRel := mock_frontend.NewMockRelation(ctrl)
		dstRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: dstDB, Name: "dst",
			Cols: []*plan.ColDef{{Name: "id", Default: seqDefaultExpr(seqNextvalExpr("s"))}, {Name: "v"}},
		}).AnyTimes()
		srcRel := mock_frontend.NewMockRelation(ctrl)
		srcRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "br1", Name: "src",
			Cols: []*plan.ColDef{{Name: "id2", OriginName: "id"}, {Name: "v"}},
		}).AnyTimes()
		var tblStuff tableStuff
		tblStuff.baseRel = dstRel
		tblStuff.tarRel = srcRel
		tblStuff.def.colNames = []string{"id2", "v"}

		tr, err := newSequenceWatermarkTracker(context.Background(), tblStuff, 1)
		require.NoError(t, err)
		require.Equal(t, dstDB, tr.dstDB)
		require.Equal(t, map[int]sequenceWatermarkKey{0: {db: "db1", table: "s"}}, tr.cols)
	})

	t.Run("no target relation yields an empty tracker", func(t *testing.T) {
		tr, err := newSequenceWatermarkTracker(context.Background(), tableStuff{}, 1)
		require.NoError(t, err)
		require.Empty(t, tr.cols)
		require.True(t, tr.empty())
	})

	t.Run("nil table definition yields an empty tracker", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(nil).AnyTimes()
		var tblStuff tableStuff
		tblStuff.baseRel = rel
		tblStuff.tarRel = rel
		tr, err := newSequenceWatermarkTracker(context.Background(), tblStuff, 1)
		require.NoError(t, err)
		require.Empty(t, tr.cols)
	})
}

// TestSequenceWatermarkCaptureAccumulation pins the tracker folding of
// design §4 B watermark capture: every captured value updates each of the
// six per-storage-domain arms it is representable in — the column type
// never decides the domain; the sequence's own metadata does at reconcile
// time (#30359 pre-PR review, domain/width conflation) — repeated
// captures merge monotonic, and non-typed values never fold.
func TestSequenceWatermarkCaptureAccumulation(t *testing.T) {
	newTrackerForColumn := func(colType types.Type, key sequenceWatermarkKey) (*sequenceWatermarkTracker, tableStuff) {
		tr := &sequenceWatermarkTracker{
			dstDB: "db1",
			cols:  map[int]sequenceWatermarkKey{0: key},
			items: map[sequenceWatermarkKey]*sequenceWatermark{},
		}
		var tblStuff tableStuff
		tblStuff.def.colTypes = []types.Type{colType}
		return tr, tblStuff
	}

	t.Run("signed ascending extremes merge monotonic", func(t *testing.T) {
		tr, tblStuff := newTrackerForColumn(types.T_int64.ToType(), sequenceWatermarkKey{"db1", "s"})
		tr.capture(tblStuff, []any{int64(2)}, []int{0})
		tr.capture(tblStuff, []any{int64(7)}, []int{0})
		tr.capture(tblStuff, []any{int64(5)}, []int{0})
		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.NotNil(t, wm)
		// Small non-negative values fit every storage domain, so all six
		// arms track them and agree in both domain orders.
		for i := 0; i < int(function.SequenceDomainCount); i++ {
			arm := wm.arms[i]
			require.True(t, arm.has, "arm %d", i)
			require.Equal(t, uint64(7), arm.max, "arm %d", i)
			require.Equal(t, uint64(2), arm.min, "arm %d", i)
		}
		require.Equal(t, int64(3), wm.rowsCount)
		require.Equal(t, "db1", wm.db)
		require.Equal(t, "s", wm.table)

		// Values inside the established range never move the extremes.
		tr.capture(tblStuff, []any{int64(4)}, []int{0})
		for i := 0; i < int(function.SequenceDomainCount); i++ {
			arm := wm.arms[i]
			require.Equal(t, uint64(7), arm.max, "arm %d", i)
			require.Equal(t, uint64(2), arm.min, "arm %d", i)
		}
		require.Equal(t, int64(4), wm.rowsCount)
		// A later higher row still raises the max; a later lower one lowers min.
		tr.capture(tblStuff, []any{int64(9)}, []int{0})
		tr.capture(tblStuff, []any{int64(1)}, []int{0})
		require.Equal(t, uint64(9), wm.arms[function.SeqDomainInt64].max)
		require.Equal(t, uint64(1), wm.arms[function.SeqDomainInt64].min)
	})

	t.Run("negative values order only the signed arms", func(t *testing.T) {
		tr, tblStuff := newTrackerForColumn(types.T_int64.ToType(), sequenceWatermarkKey{"db1", "s"})
		tr.capture(tblStuff, []any{int64(-5)}, []int{0})
		tr.capture(tblStuff, []any{int64(-9)}, []int{0})
		tr.capture(tblStuff, []any{int64(-2)}, []int{0})
		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.NotNil(t, wm)
		for _, d := range []function.SequenceDomain{function.SeqDomainInt16, function.SeqDomainInt32, function.SeqDomainInt64} {
			arm := wm.arms[d]
			require.True(t, arm.has, "signed arm %d", d)
			require.Equal(t, int64(-2), int64(arm.max), "signed arm %d", d)
			require.Equal(t, int64(-9), int64(arm.min), "signed arm %d", d)
		}
		// Negative values have no unsigned-domain meaning: signed -1 and
		// unsigned MaxUint64 share one bit pattern, and folding it into
		// an unsigned arm would false-exhaust an unsigned sequence's
		// reconcile (#30359 pre-PR review, domain conflation). Every
		// unsigned arm stays absent.
		for _, d := range []function.SequenceDomain{function.SeqDomainUint16, function.SeqDomainUint32, function.SeqDomainUint64} {
			require.False(t, wm.arms[d].has, "unsigned arm %d", d)
		}
	})

	t.Run("capture is domain independent", func(t *testing.T) {
		tr, tblStuff := newTrackerForColumn(types.T_uint64.ToType(), sequenceWatermarkKey{"db1", "s"})
		tr.capture(tblStuff, []any{uint64(10)}, []int{0})
		tr.capture(tblStuff, []any{uint64(4)}, []int{0})
		tr.capture(tblStuff, []any{uint64(12)}, []int{0})
		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.NotNil(t, wm)
		for i := 0; i < int(function.SequenceDomainCount); i++ {
			arm := wm.arms[i]
			require.True(t, arm.has, "arm %d", i)
			require.Equal(t, uint64(12), arm.max, "arm %d", i)
			require.Equal(t, uint64(4), arm.min, "arm %d", i)
		}
		// A high-bit magnitude above MaxInt64 joins only the Uint64 arm;
		// every narrower arm's extremes stay the values it can represent.
		tr.capture(tblStuff, []any{uint64(0xFFFFFFFFFFFFFFF0)}, []int{0})
		for i := 0; i < int(function.SequenceDomainCount); i++ {
			arm := wm.arms[i]
			if function.SequenceDomain(i) == function.SeqDomainUint64 {
				require.Equal(t, uint64(0xFFFFFFFFFFFFFFF0), arm.max)
				require.Equal(t, uint64(4), arm.min)
				continue
			}
			require.Equal(t, uint64(12), arm.max, "arm %d", i)
			require.Equal(t, uint64(4), arm.min, "arm %d", i)
		}
	})

	t.Run("uint64 boundary strings through a VARCHAR column keep the unsigned arm", func(t *testing.T) {
		// #30359 pre-PR review: a BIGINT UNSIGNED sequence backing a
		// VARCHAR(32) default casts its generated values to strings. The
		// column's signedness must not pick the comparison domain: 2^63 is
		// negative in int64 order and the max watermark would collapse to
		// 2^63-1, re-issuing the merged 2^63 on the next default insert.
		tr, tblStuff := newTrackerForColumn(types.T_varchar.ToType(), sequenceWatermarkKey{"db1", "s"})
		tr.capture(tblStuff, []any{"9223372036854775807"}, []int{0})
		tr.capture(tblStuff, []any{"9223372036854775808"}, []int{0})
		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.NotNil(t, wm)
		u64 := wm.arms[function.SeqDomainUint64]
		require.Equal(t, uint64(9223372036854775808), u64.max,
			"the uint-magnitude arm keeps the true numeric max across the signed boundary")
		require.Equal(t, uint64(9223372036854775807), u64.min)
		s64 := wm.arms[function.SeqDomainInt64]
		require.True(t, s64.has)
		require.Equal(t, int64(9223372036854775807), int64(s64.max),
			"the int64 arm carries only values it can represent")
		require.Equal(t, int64(9223372036854775807), int64(s64.min),
			"2^63 exceeds MaxInt64 and never folds into the signed arm as its bit pattern")
		// Neither value fits any 16- or 32-bit domain: those arms stay
		// empty instead of reading truncated bits (#30359 review N2).
		for _, d := range []function.SequenceDomain{
			function.SeqDomainInt16, function.SeqDomainInt32,
			function.SeqDomainUint16, function.SeqDomainUint32,
		} {
			require.False(t, wm.arms[d].has, "narrow arm %d", d)
		}
	})

	t.Run("typed row values fold per domain", func(t *testing.T) {
		for _, v := range []struct {
			name  string
			value any
			hasU  bool
			u     uint64
			hasS  bool
			s     uint64
		}{
			{"int8", int8(-6), false, 0, true, seqNegBits(-6)},
			{"int16", int16(-7), false, 0, true, seqNegBits(-7)},
			{"int32v", int32(-8), false, 0, true, seqNegBits(-8)},
			{"int", int(-9), false, 0, true, seqNegBits(-9)},
			{"int64", int64(-10), false, 0, true, seqNegBits(-10)},
			{"uint8", uint8(11), true, 11, true, 11},
			{"uint16", uint16(12), true, 12, true, 12},
			{"uint32", uint32(13), true, 13, true, 13},
			{"uint64", uint64(14), true, 14, true, 14},
			{"uint64 above MaxInt64", uint64(0x8000000000000000), true, 0x8000000000000000, false, 0},
			{"uint64 max", uint64(18446744073709551615), true, 18446744073709551615, false, 0},
			{"numeric string", "15", true, 15, true, 15},
			{"numeric bytes", []byte("16"), true, 16, true, 16},
			{"negative string", "-17", false, 0, true, seqNegBits(-17)},
			{"uint64 string above MaxInt64", "9223372036854775808", true, 9223372036854775808, false, 0},
		} {
			t.Run(v.name, func(t *testing.T) {
				tr, tblStuff := newTrackerForColumn(types.T_int64.ToType(), sequenceWatermarkKey{"db1", "s"})
				tr.capture(tblStuff, []any{v.value}, []int{0})
				wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
				require.NotNil(t, wm)
				u64 := wm.arms[function.SeqDomainUint64]
				require.Equal(t, v.hasU, u64.has, "unsigned-arm presence")
				if v.hasU {
					require.Equal(t, v.u, u64.max)
					require.Equal(t, v.u, u64.min)
				}
				s64 := wm.arms[function.SeqDomainInt64]
				require.Equal(t, v.hasS, s64.has, "signed-arm presence")
				if v.hasS {
					require.Equal(t, v.s, s64.max)
					require.Equal(t, v.s, s64.min)
				}
				// Full-domain truth table: each of the six storage-domain
				// arms is present exactly when the captured classification
				// says the value is representable there (#30359 review N2).
				val, ok := sequenceWatermarkValueOf(v.value, tblStuff.def.colTypes[0])
				require.True(t, ok)
				for i := 0; i < int(function.SequenceDomainCount); i++ {
					d := function.SequenceDomain(i)
					want := (val.hasS && d.ContainsInt64(int64(val.s))) ||
						(val.hasU && d.ContainsUint64(val.u))
					require.Equal(t, want, wm.arms[i].has, "domain %d presence", i)
				}
			})
		}
	})

	t.Run("decimal columns normalize integral keys by scale", func(t *testing.T) {
		// #30359 post-PR review: a DECIMAL(p,s) column backed by a
		// nextval default stores each issued key unscaled as key * 10^s.
		// Typed decimals previously matched no capture case, so merging
		// them advanced no watermark and the next default insert
		// regenerated an already-merged key (duplicate entry '2' / '2.00').
		// Integral decimals normalize by the column scale; stored
		// fractions can never collide with future integral emissions and
		// stay skipped.
		decType := func(oid types.T, scale int32) types.Type {
			tp := oid.ToType()
			tp.Scale = scale
			return tp
		}
		neg128 := func(lo uint64) types.Decimal128 {
			return types.Decimal128{B0_63: lo}.Minus()
		}
		neg256 := func(lo uint64) types.Decimal256 {
			return types.Decimal256{B0_63: lo}.Minus()
		}
		pow20 := types.Decimal128{B0_63: 7766279631452241920, B64_127: 5} // 10^20
		for _, v := range []struct {
			name    string
			colType types.Type
			value   any
			wantOK  bool
			hasU    bool
			u       uint64
			hasS    bool
			s       uint64
		}{
			{"decimal64 scale 0", decType(types.T_decimal64, 0), types.Decimal64(200), true, true, 200, true, 200},
			{"decimal64 integral after scale", decType(types.T_decimal64, 2), types.Decimal64(200), true, true, 2, true, 2},
			{"decimal64 fractional skipped", decType(types.T_decimal64, 2), types.Decimal64(250), false, false, 0, false, 0},
			{"decimal64 negative integral", decType(types.T_decimal64, 2), types.Decimal64(300).Minus(), true, false, 0, true, seqNegBits(-3)},
			{"decimal64 negative fractional skipped", decType(types.T_decimal64, 2), types.Decimal64(250).Minus(), false, false, 0, false, 0},
			{"decimal64 zero at scale", decType(types.T_decimal64, 2), types.Decimal64(0), true, true, 0, true, 0},
			{"decimal128 scale 0", decType(types.T_decimal128, 0), types.Decimal128{B0_63: 21}, true, true, 21, true, 21},
			{"decimal128 integral after scale", decType(types.T_decimal128, 4), types.Decimal128{B0_63: 12340000}, true, true, 1234, true, 1234},
			{"decimal128 fractional skipped", decType(types.T_decimal128, 4), types.Decimal128{B0_63: 12345000}, false, false, 0, false, 0},
			{"decimal128 negative integral", decType(types.T_decimal128, 2), neg128(300), true, false, 0, true, seqNegBits(-3)},
			{"decimal128 divisor crossing 64 bits", decType(types.T_decimal128, 20), pow20, true, true, 1, true, 1},
			{"decimal128 quotient at 2^64 skipped", decType(types.T_decimal128, 0), types.Decimal128{B64_127: 1}, false, false, 0, false, 0},
			{"decimal256 scale 0", decType(types.T_decimal256, 0), types.Decimal256{B0_63: 7}, true, true, 7, true, 7},
			{"decimal256 negative integral after scale", decType(types.T_decimal256, 6), neg256(7000000), true, false, 0, true, seqNegBits(-7)},
			{"decimal256 fractional skipped", decType(types.T_decimal256, 2), types.Decimal256{B0_63: 7}, false, false, 0, false, 0},
			{"decimal256 quotient above 2^64 skipped", decType(types.T_decimal256, 0), types.Decimal256{B64_127: 1}, false, false, 0, false, 0},
		} {
			t.Run(v.name, func(t *testing.T) {
				tr, tblStuff := newTrackerForColumn(v.colType, sequenceWatermarkKey{"db1", "s"})
				tr.capture(tblStuff, []any{v.value}, []int{0})
				wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
				if !v.wantOK {
					require.Nil(t, wm, "a fractional or out-of-domain decimal must never fold")
					require.True(t, tr.empty())
					return
				}
				require.NotNil(t, wm)
				u64 := wm.arms[function.SeqDomainUint64]
				require.Equal(t, v.hasU, u64.has, "unsigned-arm presence")
				if v.hasU {
					require.Equal(t, v.u, u64.max)
					require.Equal(t, v.u, u64.min)
				}
				s64 := wm.arms[function.SeqDomainInt64]
				require.Equal(t, v.hasS, s64.has, "signed-arm presence")
				if v.hasS {
					require.Equal(t, v.s, s64.max)
					require.Equal(t, v.s, s64.min)
				}
				// Full-domain truth table across the six storage-domain
				// arms, identical to the sibling subtests (#30359 review N2).
				val, ok := sequenceWatermarkValueOf(v.value, tblStuff.def.colTypes[0])
				require.True(t, ok)
				for i := 0; i < int(function.SequenceDomainCount); i++ {
					d := function.SequenceDomain(i)
					want := (val.hasS && d.ContainsInt64(int64(val.s))) ||
						(val.hasU && d.ContainsUint64(val.u))
					require.Equal(t, want, wm.arms[i].has, "domain %d presence", i)
				}
			})
		}
	})

	t.Run("decimal accumulation merges monotonic with fractional skips", func(t *testing.T) {
		tr, tblStuff := newTrackerForColumn(func() types.Type {
			tp := types.T_decimal64.ToType()
			tp.Scale = 2
			return tp
		}(), sequenceWatermarkKey{"db1", "s"})
		tr.capture(tblStuff, []any{types.Decimal64(200)}, []int{0})
		tr.capture(tblStuff, []any{types.Decimal64(250)}, []int{0}) // 2.50: fractional, never folds
		tr.capture(tblStuff, []any{types.Decimal64(700)}, []int{0})
		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.NotNil(t, wm)
		require.Equal(t, int64(2), wm.rowsCount, "fractional captures never reach the watermark")
		require.Equal(t, uint64(7), wm.arms[function.SeqDomainInt64].max)
		require.Equal(t, uint64(2), wm.arms[function.SeqDomainInt64].min)
		require.Equal(t, uint64(7), wm.arms[function.SeqDomainUint64].max)
		require.Equal(t, uint64(2), wm.arms[function.SeqDomainUint64].min)
	})

	t.Run("two columns owning one sequence merge into one watermark", func(t *testing.T) {
		tr := &sequenceWatermarkTracker{
			dstDB: "db1",
			cols: map[int]sequenceWatermarkKey{
				0: {"db1", "s"},
				2: {"db1", "s"},
			},
			items: map[sequenceWatermarkKey]*sequenceWatermark{},
		}
		var tblStuff tableStuff
		tblStuff.def.colTypes = []types.Type{
			types.T_int64.ToType(), types.T_varchar.ToType(), types.T_int64.ToType(),
		}
		tr.capture(tblStuff, []any{int64(3), "x", int64(9)}, []int{0, 2})
		tr.capture(tblStuff, []any{int64(7), "y", int64(1)}, []int{0, 2})
		require.Len(t, tr.items, 1)
		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.NotNil(t, wm)
		require.Equal(t, uint64(9), wm.arms[function.SeqDomainInt64].max)
		require.Equal(t, uint64(1), wm.arms[function.SeqDomainInt64].min)
		require.Equal(t, uint64(9), wm.arms[function.SeqDomainUint64].max)
		require.Equal(t, uint64(1), wm.arms[function.SeqDomainUint64].min)
		require.Equal(t, int64(4), wm.rowsCount)
	})

	t.Run("kind filtering leaves deletes and noise out", func(t *testing.T) {
		tr, tblStuff := newTrackerForColumn(types.T_int64.ToType(), sequenceWatermarkKey{"db1", "s"})
		// Index not registered as reconcilable (a delete-key or data column).
		tr.capture(tblStuff, []any{int64(100)}, []int{1})
		// Index beyond the row and beyond the column-types slice.
		tr.capture(tblStuff, []any{int64(100)}, []int{7})
		// Non-typed and unparsable values never fold.
		tr.capture(tblStuff, []any{"not-a-number"}, []int{0})
		tr.capture(tblStuff, []any{float64(1.5)}, []int{0})
		tr.capture(tblStuff, []any{nil}, []int{0})
		require.True(t, tr.empty())
	})

	t.Run("nil or column-less tracker captures nothing", func(t *testing.T) {
		var nilTracker *sequenceWatermarkTracker
		nilTracker.capture(tableStuff{}, []any{int64(1)}, []int{0})
		require.True(t, nilTracker.empty())
		tr := &sequenceWatermarkTracker{dstDB: "db1", cols: map[int]sequenceWatermarkKey{}, items: map[sequenceWatermarkKey]*sequenceWatermark{}}
		tr.capture(tableStuff{}, []any{int64(1)}, []int{0})
		require.True(t, tr.empty())
	})
}

// ---------------------------------------------------------------------
// Task C — reconcile unit (design §4 B steps 1–4, §9 B/I1 oracles).
// ---------------------------------------------------------------------

// TestSequenceReconcileCommittedAdvance pins the committed partition: one
// autonomous transaction selects the metadata row FOR UPDATE and installs
// the watermark with a monotonic conditional UPDATE; a merge retry finds the
// row already at the watermark called and writes nothing (idempotent,
// never regresses).
func TestSequenceReconcileCommittedAdvance(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		committed: map[[2]string]uint64{{"db1", "s"}: 42},
	})
	f.store.add("db1", "s", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wm := seqWmRange("db1", "s", 2, 5, 2)
	tracker := newWatermarkTracker("db1", wm)

	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))

	row, ok := f.store.get("db1", "s")
	require.True(t, ok)
	require.Equal(t, uint64(5), row.last)
	require.True(t, row.called)

	// One autonomous attempt, one SELECT FOR UPDATE + one conditional
	// UPDATE, nothing on the in-caller route, no exclusive data-table lock:
	// the real catalog fence comes from the SELECT FOR UPDATE pipeline.
	require.Equal(t, 1, f.exec.attempts)
	require.Equal(t, []string{"commit"}, f.exec.outcomes)
	require.Equal(t, []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "5", false)},
		reconcileEventSQLs(f.exec.events, "txn"))
	require.Empty(t, reconcileEventSQLs(f.exec.events, "local"))
	for _, attemptLocks := range f.exec.locks {
		require.Empty(t, attemptLocks, "the unit must not take exclusive data-table locks (catalog fence comes from the statement pipeline)")
	}
	for _, op := range f.catalog.seenOps {
		require.Same(t, f.autonomousOp, op, "committed resolution must use the autonomous operator")
	}

	// Merge retry re-runs reconciliation with the same watermarks: the
	// conditional matches no arm and the reconcile writes nothing.
	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))
	require.Equal(t, 2, f.exec.attempts)
	require.Equal(t, []string{"commit", "commit"}, f.exec.outcomes)
	require.Equal(t,
		[]string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "5", false), wantReconcileSelectSQL(wm)},
		reconcileEventSQLs(f.exec.events, "txn"))
	row, _ = f.store.get("db1", "s")
	require.Equal(t, uint64(5), row.last)
	require.True(t, row.called)
}

// TestSequenceReconcileCommittedSurvivesDeadProcCtx pins the reconcile
// unit's context contract (issue #30359): the session process context is
// reseated by statement machinery and can already be canceled once the
// merge's apply pipelines finished. The reconcile unit must bind its
// committed transaction to the caller-supplied (merge statement) context,
// which stays alive through the unit's completion — observed live as
// "context canceled" mid-commit before the caller-picked context fix.
func TestSequenceReconcileCommittedSurvivesDeadProcCtx(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		committed: map[[2]string]uint64{{"db1", "s"}: 42},
	})
	f.store.add("db1", "s", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wm := seqWmRange("db1", "s", 2, 5, 2)
	tracker := newWatermarkTracker("db1", wm)

	// Kill the session process context before reconciling, as a reseated
	// session process context can be after the apply pipelines finish. A
	// canceled context still serves stored values, so the engine lookup
	// keeps working exactly as it did live.
	deadCtx, cancel := context.WithCancel(f.ses.proc.Ctx)
	f.ses.proc.Ctx = deadCtx
	cancel()

	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))

	row, ok := f.store.get("db1", "s")
	require.True(t, ok)
	require.Equal(t, uint64(5), row.last)
	require.True(t, row.called)
	require.Equal(t, []string{"commit"}, f.exec.outcomes)
	require.Equal(t, []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "5", false)},
		reconcileEventSQLs(f.exec.events, "txn"))
}

// TestSequenceReconcileDirectionArms pins the monotonic conditional UPDATE
// per direction and numeric domain: descending watermarks lower the counter
// with the mirrored guard, the equality-with-uncalled arm adopts a
// never-called position equal to the watermark, and a not-ahead watermark
// (already strictly ahead, called) installs nothing.
func TestSequenceReconcileDirectionArms(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stored   seqStoreRow
		wm       *sequenceWatermark
		wantSQLs func(wm *sequenceWatermark) []string
		wantLast uint64
		wantCall bool
	}{
		{
			name:   "descending reconciles to numeric min watermark",
			stored: seqStoreRow{last: 100, minv: 1, maxv: 100, start: 100, incr: -1, called: true},
			wm:     seqWmRange("db1", "s", 60, 90, 2),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "60", true)}
			},
			wantLast: 60,
			wantCall: true,
		},
		{
			name:   "never called destination adopts equal watermark",
			stored: seqStoreRow{last: 5, minv: 1, maxv: 100, start: 5, incr: 1, called: false},
			wm:     seqWmRange("db1", "s", 5, 5, 1),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "5", false)}
			},
			wantLast: 5,
			wantCall: true,
		},
		{
			name:   "descending never called destination adopts equal watermark",
			stored: seqStoreRow{last: 60, minv: 1, maxv: 100, start: 60, incr: -1, called: false},
			wm:     seqWmRange("db1", "s", 60, 70, 1),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "60", true)}
			},
			wantLast: 60,
			wantCall: true,
		},
		{
			name:   "strictly ahead called position is a genuine no-op",
			stored: seqStoreRow{last: 5, minv: 1, maxv: 100, start: 1, incr: 1, called: true},
			wm:     seqWmRange("db1", "s", 2, 3, 2),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm)}
			},
			wantLast: 5,
			wantCall: true,
		},
		{
			name:   "unsigned domain reconciles with unsigned literal",
			stored: seqStoreRow{last: 1, minv: 1, maxv: 1000, start: 1, incr: 1, called: true, unsigned: true},
			wm:     seqWmRange("db1", "s", 2, 9, 2),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "9", false)}
			},
			wantLast: 9,
			wantCall: true,
		},
		{
			// Review regression: a BIGINT UNSIGNED sequence whose merged
			// keys cross the signed 64-bit boundary (…807 then …808, as
			// captured from a VARCHAR default column — the column type must
			// not decide the comparison). Int64 ordering ranks …808 below
			// …807, so a column-derived signed compare would reconcile only
			// to …807 and the next default insert would duplicate …808.
			// The sequence metadata declares the unsigned domain, so the
			// uint-magnitude arm wins and the counter lands on …808.
			name:   "unsigned reconcile crosses the signed 64-bit boundary",
			stored: seqStoreRow{last: 9223372036854775806, minv: 1, maxv: 18446744073709551615, start: 9223372036854775806, incr: 1, called: true, unsigned: true},
			// …808 is not representable in the signed domain, so the
			// signed arms carry only …807 (no bit-pattern folding).
			wm: seqWmRange("db1", "s", 9223372036854775807, 9223372036854775808, 2),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "9223372036854775808", false)}
			},
			wantLast: 9223372036854775808,
			wantCall: true,
		},
		{
			// N2 regression (pre-PR review): a SMALLINT UNSIGNED sequence
			// whose merged keys are 2 and MaxUint64 — as captured from a
			// VARCHAR default, where a merged peer row may carry any
			// numeric spelling. MaxUint64 is not representable in the
			// sequence's uint16 storage domain and never enters its arm;
			// pre-fix the width-blind uint-magnitude arm read MaxUint64
			// and the narrowing range check false-exhausted this
			// NO CYCLE sequence at the merge. The reconcile installs
			// exactly 2.
			name:   "narrow unsigned sequence reconciles only its own arm",
			stored: seqStoreRow{last: 1, minv: 1, maxv: math.MaxUint16, start: 1, incr: 1, called: true, unsigned: true, narrowT: types.T_uint16},
			wm:     seqWmRange("db1", "s", 2, math.MaxUint64, 2),
			wantSQLs: func(wm *sequenceWatermark) []string {
				return []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "2", false)}
			},
			wantLast: 2,
			wantCall: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSeqReconcileFixture(t, &seqReconcileCatalog{
				committed: map[[2]string]uint64{{"db1", "s"}: 42},
			})
			f.store.add("db1", "s", tc.stored)
			tracker := newWatermarkTracker("db1", tc.wm)

			require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))
			row, ok := f.store.get("db1", "s")
			require.True(t, ok)
			require.Equal(t, tc.wantLast, row.last)
			require.Equal(t, tc.wantCall, row.called)
			require.Equal(t, tc.wantSQLs(tc.wm), reconcileEventSQLs(f.exec.events, "txn"))
			require.Empty(t, reconcileEventSQLs(f.exec.events, "local"))

			// Retry keeps the reconciled position — the conditional update
			// can never regress a past install.
			require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))
			row, _ = f.store.get("db1", "s")
			require.Equal(t, tc.wantLast, row.last)
			require.Equal(t, tc.wantCall, row.called)
		})
	}
}

// TestSequenceReconcileMixedDomainWatermark pins the #30359 pre-PR review
// domain-conflation regression: capture classifies each merged key by the
// numeric domains that can represent it, so a foreign-domain key never
// contaminates the arm the sequence's own metadata reads. Signed -1 and
// unsigned MaxUint64 share one bit pattern; bit-folding one into the other
// false-exhausted or mis-advanced the reconcile.
func TestSequenceReconcileMixedDomainWatermark(t *testing.T) {
	newCaptureTracker := func(colType types.Type, rows ...any) *sequenceWatermarkTracker {
		tr := &sequenceWatermarkTracker{
			dstDB: "db1",
			cols:  map[int]sequenceWatermarkKey{0: {"db1", "s"}},
			items: map[sequenceWatermarkKey]*sequenceWatermark{},
		}
		var tblStuff tableStuff
		tblStuff.def.colTypes = []types.Type{colType}
		for _, v := range rows {
			tr.capture(tblStuff, []any{v}, []int{0})
		}
		return tr
	}

	t.Run("unsigned sequence reconciles past mixed-sign merged keys", func(t *testing.T) {
		// F1 regression: VARCHAR-wrapped default column keys "-1" and "2"
		// against a BIGINT UNSIGNED sequence. Folding -1's bit pattern into
		// the uint arm produced a MaxUint64 watermark whose range guard
		// false-exhausts this NO CYCLE sequence; domain-aware capture
		// keeps the uint arm at {2} and the reconcile installs exactly 2.
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s"}: 42},
		})
		f.store.add("db1", "s", seqStoreRow{last: 1, minv: 1, maxv: 18446744073709551615, start: 1, incr: 1, called: true, unsigned: true})
		tr := newCaptureTracker(types.T_varchar.ToType(), "-1", "2")

		require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tr))
		row, ok := f.store.get("db1", "s")
		require.True(t, ok)
		require.Equal(t, uint64(2), row.last)
		require.True(t, row.called)

		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		require.True(t, wm.arms[function.SeqDomainUint64].has)
		require.True(t, wm.arms[function.SeqDomainInt64].has)
		require.Equal(t, uint64(2), wm.arms[function.SeqDomainUint64].max,
			"the negative key never folds into the unsigned arms")
		require.Equal(t, []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "2", false)},
			reconcileEventSQLs(f.exec.events, "txn"))
	})

	t.Run("an all-negative merged set reconciles nothing for an unsigned sequence", func(t *testing.T) {
		// Every captured key is negative and the sequence is BIGINT
		// UNSIGNED: the unsigned arm stays empty, so the position is left
		// untouched — a negative key can never collide with a value the
		// unsigned sequence may issue. Bit-folding would have installed a
		// near-MaxUint64 position.
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s"}: 42},
		})
		f.store.add("db1", "s", seqStoreRow{last: 5, minv: 1, maxv: 18446744073709551615, start: 1, incr: 1, called: true, unsigned: true})
		tr := newCaptureTracker(types.T_int64.ToType(), int64(-9), int64(-4))

		require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tr))
		row, ok := f.store.get("db1", "s")
		require.True(t, ok)
		require.Equal(t, uint64(5), row.last)
		require.True(t, row.called)

		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		for _, d := range []function.SequenceDomain{
			function.SeqDomainUint16, function.SeqDomainUint32, function.SeqDomainUint64,
		} {
			require.False(t, wm.arms[d].has, "no captured key fits unsigned domain %d", d)
		}
		require.Equal(t, []string{wantReconcileSelectSQL(wm)},
			reconcileEventSQLs(f.exec.events, "txn"), "an empty domain arm reconciles nothing")
	})

	t.Run("signed descending sequence ignores unsigned-only merged keys", func(t *testing.T) {
		// A merged key above MaxInt64 has no signed-domain meaning:
		// bit-folding read MaxUint64 as -1 and would have dragged the
		// descending watermark down to -1; the empty signed arm leaves the
		// position untouched.
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s"}: 42},
		})
		f.store.add("db1", "s", seqStoreRow{last: 100, minv: 1, maxv: 100, start: 100, incr: -1, called: true})
		tr := newCaptureTracker(types.T_varchar.ToType(), "18446744073709551615")

		require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tr))
		row, ok := f.store.get("db1", "s")
		require.True(t, ok)
		require.Equal(t, uint64(100), row.last)
		require.True(t, row.called)

		wm := tr.items[sequenceWatermarkKey{"db1", "s"}]
		for _, d := range []function.SequenceDomain{
			function.SeqDomainInt16, function.SeqDomainInt32, function.SeqDomainInt64,
		} {
			require.False(t, wm.arms[d].has, "no captured key fits signed domain %d", d)
		}
		require.Equal(t, []string{wantReconcileSelectSQL(wm)},
			reconcileEventSQLs(f.exec.events, "txn"), "an empty domain arm reconciles nothing")
	})
}

// TestSequenceReconcileNoCycleGuardFailureAtomic pins the §4 B step 2f
// guard: a NO CYCLE watermark whose post-transition position can never
// issue aborts the whole reconcile unit failure-atomically — no partial
// advance across multiple sequences persists — and fails the merge. A CYCLE
// watermark at the same edge wraps instead and installs.
func TestSequenceReconcileNoCycleGuardFailureAtomic(t *testing.T) {
	t.Run("guard failure rolls back the whole reconcile transaction", func(t *testing.T) {
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s1"}: 10, {"db1", "s2"}: 20},
		})
		f.store.add("db1", "s1", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
		// NO CYCLE, maxv=10: installing (10,true) could never issue 11.
		f.store.add("db1", "s2", seqStoreRow{last: 1, minv: 1, maxv: 10, start: 1, incr: 1, called: true})
		wm1 := seqWmRange("db1", "s1", 2, 5, 2)
		wm2 := seqWmRange("db1", "s2", 4, 10, 2)
		tracker := newWatermarkTracker("db1", wm1, wm2)

		err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker)
		require.ErrorContains(t, err, "Reached maximum value of sequence")
		// The guard error is deterministic, not a transient conflict.
		require.Equal(t, 1, f.exec.attempts)
		require.Equal(t, []string{"rollback"}, f.exec.outcomes)
		// Canonical (account, relation id) ascending order — proven by the
		// statement order below (s1 id 10 before s2 id 20); the unit never
		// takes exclusive data-table locks.
		for _, attemptLocks := range f.exec.locks {
			require.Empty(t, attemptLocks)
		}
		// s1's conditional update ran inside the aborted attempt; failure
		// atomicity means no partial advance persists for any sequence.
		require.Equal(t, []string{
			wantReconcileSelectSQL(wm1), wantReconcileUpdateSQL(wm1, "5", false),
			wantReconcileSelectSQL(wm2),
		}, reconcileEventSQLs(f.exec.events, "txn"))
		row, ok := f.store.get("db1", "s1")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true}, row)
		row, ok = f.store.get("db1", "s2")
		require.True(t, ok)
		require.Equal(t, seqStoreRow{last: 1, minv: 1, maxv: 10, start: 1, incr: 1, called: true}, row)
	})

	t.Run("cycle sequence at the edge wraps and installs", func(t *testing.T) {
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s2"}: 20},
		})
		f.store.add("db1", "s2", seqStoreRow{last: 1, minv: 1, maxv: 10, start: 1, incr: 1, cycle: true, called: true})
		wm2 := seqWmRange("db1", "s2", 4, 10, 2)
		tracker := newWatermarkTracker("db1", wm2)

		require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))
		row, ok := f.store.get("db1", "s2")
		require.True(t, ok)
		require.Equal(t, uint64(10), row.last)
		require.True(t, row.called)
		require.Equal(t, []string{"commit"}, f.exec.outcomes)
	})

	t.Run("descending edge guard rejects an unissuable install", func(t *testing.T) {
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s2"}: 20},
		})
		// Descending NO CYCLE, minv=1: installing (1,true) could never issue 0.
		f.store.add("db1", "s2", seqStoreRow{last: 100, minv: 1, maxv: 100, start: 100, incr: -1, called: true})
		wm2 := seqWmRange("db1", "s2", 1, 90, 2)
		tracker := newWatermarkTracker("db1", wm2)

		err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker)
		require.ErrorContains(t, err, "Reached maximum value of sequence")
		row, ok := f.store.get("db1", "s2")
		require.True(t, ok)
		require.Equal(t, uint64(100), row.last)
	})

	t.Run("narrow unsigned edge guard rejects an unissuable install", func(t *testing.T) {
		// N2 complement: width filtering removes foreign-domain keys, it
		// never weakens in-domain exhaustion. A representable 65535
		// watermark on a NO CYCLE SMALLINT UNSIGNED sequence (maxv
		// 65535) can never issue a next value and aborts the merge.
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s2"}: 20},
		})
		f.store.add("db1", "s2", seqStoreRow{last: 1, minv: 1, maxv: math.MaxUint16, start: 1, incr: 1, called: true, unsigned: true, narrowT: types.T_uint16})
		tracker := newWatermarkTracker("db1", seqWmRange("db1", "s2", math.MaxUint16, math.MaxUint16, 1))

		err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker)
		require.ErrorContains(t, err, "Reached maximum value of sequence")
		require.Equal(t, []string{"rollback"}, f.exec.outcomes)
		row, ok := f.store.get("db1", "s2")
		require.True(t, ok)
		require.Equal(t, uint64(1), row.last, "the failure-atomic guard leaves the position untouched")
	})
}

// TestSequenceReconcilePartitionRoutesByVisibility pins the §4 B step 1/3
// split: the committed generation advances in the autonomous reconcile
// transaction while the transaction-local generation (resolvable only
// through the merge transaction) advances in-caller bound to the merge
// transaction's operator.
func TestSequenceReconcilePartitionRoutesByVisibility(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		committed: map[[2]string]uint64{{"db1", "sc"}: 1},
		local:     map[[2]string]uint64{{"db1", "sl"}: 2},
	})
	f.store.add("db1", "sc", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	f.store.add("db1", "sl", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wmc := seqWmRange("db1", "sc", 2, 5, 2)
	wml := seqWmRange("db1", "sl", 3, 7, 2)
	tracker := newWatermarkTracker("db1", wmc, wml)

	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))

	row, ok := f.store.get("db1", "sc")
	require.True(t, ok)
	require.Equal(t, uint64(5), row.last)
	require.True(t, row.called)
	row, ok = f.store.get("db1", "sl")
	require.True(t, ok)
	require.Equal(t, uint64(7), row.last)
	require.True(t, row.called)

	// Committed identity advanced on the autonomous route with its own
	// operator; the local identity advanced only on the in-caller route,
	// every statement bound to the merge transaction's operator.
	require.Equal(t,
		[]string{wantReconcileSelectSQL(wmc), wantReconcileUpdateSQL(wmc, "5", false)},
		reconcileEventSQLs(f.exec.events, "txn"))
	require.Equal(t,
		[]string{wantReconcileSelectSQL(wml), wantReconcileUpdateSQL(wml, "7", false)},
		reconcileEventSQLs(f.exec.events, "local"))
	require.NotEmpty(t, f.exec.localOps)
	for _, op := range f.exec.localOps {
		require.Same(t, f.callerOp, op)
	}
	require.Equal(t, 1, f.exec.attempts)
	require.Equal(t, []string{"commit"}, f.exec.outcomes)
	for _, attemptLocks := range f.exec.locks {
		require.Empty(t, attemptLocks, "no exclusive data-table locks; catalog fences come from the statement pipeline")
	}

	// Resolution drove both visibility views: the autonomous probe missed
	// the local-only generation, the caller view saw both.
	var sawAutonomous, sawCaller bool
	for _, op := range f.catalog.seenOps {
		sawAutonomous = sawAutonomous || op == f.autonomousOp
		sawCaller = sawCaller || op == f.callerOp
	}
	require.True(t, sawAutonomous)
	require.True(t, sawCaller)
}

// TestSequenceReconcileAllLocalPartition pins that a fully transaction-local
// watermark set performs no committed writes inside the classification
// transaction while still advancing every local identity in-caller.
func TestSequenceReconcileAllLocalPartition(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		local: map[[2]string]uint64{{"db1", "sl1"}: 21, {"db1", "sl2"}: 22},
	})
	f.store.add("db1", "sl1", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	f.store.add("db1", "sl2", seqStoreRow{last: 10, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wm1 := seqWmRange("db1", "sl1", 2, 4, 2)
	wm2 := seqWmRange("db1", "sl2", 5, 6, 2)
	tracker := newWatermarkTracker("db1", wm1, wm2)

	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))

	require.Equal(t, 1, f.exec.attempts)
	require.Equal(t, []string{"commit"}, f.exec.outcomes)
	require.Empty(t, reconcileEventSQLs(f.exec.events, "txn"), "no committed writes when every identity is transaction-local")
	// The local partition walks identities in ascending relation-id order.
	require.Equal(t, []string{
		wantReconcileSelectSQL(wm1), wantReconcileUpdateSQL(wm1, "4", false),
		wantReconcileSelectSQL(wm2),
	}, reconcileEventSQLs(f.exec.events, "local"))
	row, ok := f.store.get("db1", "sl1")
	require.True(t, ok)
	require.Equal(t, uint64(4), row.last)
	row, ok = f.store.get("db1", "sl2")
	require.True(t, ok)
	require.Equal(t, uint64(10), row.last, "not-ahead local watermark installs nothing")
}

// TestDataBranchSequenceReconcileZeroWatermarkNoTxn pins §4 B step 4: with
// zero diff rows or zero reconcilable columns there is nothing to reconcile
// — no autonomous transaction opens and no statement runs.
func TestDataBranchSequenceReconcileZeroWatermarkNoTxn(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{})

	// No captured rows at all: the tracker is empty.
	empty := newWatermarkTracker("db1")
	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, empty))
	// A nil tracker (a non-merge apply path) reconciles to nothing too.
	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, nil))

	require.Equal(t, 0, f.exec.attempts, "no autonomous txn may open for an empty watermark set")
	require.Empty(t, f.exec.events)
	require.Empty(t, f.catalog.seenOps)
}

// TestSequenceReconcileRejectsOrdinaryReplacement pins the relkind boundary
// (v12-review F1): a watermark whose name resolves to a same-named ordinary
// table — one that merely carries sequence-shaped columns — is not a
// sequence. The reconcile must fail the merge atomically before any metadata
// SELECT or UPDATE, deterministically (no retry budget consumed), and must
// never modify the ordinary table, on both visibility partitions.
func TestSequenceReconcileRejectsOrdinaryReplacement(t *testing.T) {
	t.Run("committed identity", func(t *testing.T) {
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s"}: 42},
			ordinary:  map[[2]string]bool{{"db1", "s"}: true},
		})
		f.store.add("db1", "s", seqStoreRow{last: 100, minv: 1, maxv: 1000, start: 1, incr: 1, called: true})
		wm := seqWmRange("db1", "s", 2, 5, 2)
		tracker := newWatermarkTracker("db1", wm)

		err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker)
		require.ErrorContains(t, err, "Table input is not a sequence")
		require.Equal(t, 1, f.exec.attempts, "the relkind guard is deterministic, not a transient conflict")
		require.Equal(t, []string{"rollback"}, f.exec.outcomes)
		require.Empty(t, reconcileEventSQLs(f.exec.events, "txn"),
			"no metadata SELECT or UPDATE may touch the ordinary table")
		require.Empty(t, reconcileEventSQLs(f.exec.events, "local"))
		row, ok := f.store.get("db1", "s")
		require.True(t, ok)
		require.Equal(t, uint64(100), row.last, "the same-named ordinary replacement is left untouched")
	})

	t.Run("transaction-local identity", func(t *testing.T) {
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			local:    map[[2]string]uint64{{"db1", "sl"}: 7},
			ordinary: map[[2]string]bool{{"db1", "sl"}: true},
		})
		f.store.add("db1", "sl", seqStoreRow{last: 50, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
		wm := seqWmRange("db1", "sl", 2, 9, 2)
		tracker := newWatermarkTracker("db1", wm)

		err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker)
		require.ErrorContains(t, err, "Table input is not a sequence")
		require.Equal(t, []string{"commit"}, f.exec.outcomes,
			"the classification attempt commits empty; the local partition then fails")
		require.Empty(t, reconcileEventSQLs(f.exec.events, "txn"))
		require.Empty(t, reconcileEventSQLs(f.exec.events, "local"),
			"no in-caller statement may touch the ordinary table")
		row, ok := f.store.get("db1", "sl")
		require.True(t, ok)
		require.Equal(t, uint64(50), row.last)
	})
}

// TestSequenceWatermarkTrackerCaseSemantics pins identity normalization per
// lower_case_table_names (v12-review F9): case folding applies exactly when
// the session folds; under case-sensitive mode the resolved spelling is the
// identity and a case-differing database pin is foreign.
func TestSequenceWatermarkTrackerCaseSemantics(t *testing.T) {
	buildTracker := func(lctn int64, dstDB string, def *plan.Default) *sequenceWatermarkTracker {
		ctrl := gomock.NewController(t)
		dstRel := mock_frontend.NewMockRelation(ctrl)
		dstRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: dstDB, Name: "Dst",
			Cols: []*plan.ColDef{{Name: "c", Default: def}},
		}).AnyTimes()
		srcRel := mock_frontend.NewMockRelation(ctrl)
		srcRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "Br1", Name: "Src",
			Cols: []*plan.ColDef{{Name: "c"}},
		}).AnyTimes()
		var tblStuff tableStuff
		tblStuff.baseRel = dstRel
		tblStuff.tarRel = srcRel
		tblStuff.def.colNames = []string{"c"}
		tr, err := newSequenceWatermarkTracker(context.Background(), tblStuff, lctn)
		require.NoError(t, err)
		return tr
	}

	t.Run("case-sensitive mode preserves the resolved spelling", func(t *testing.T) {
		tr := buildTracker(0, "Db1", seqDefaultExpr(seqNextvalExpr("Seq")))
		require.Equal(t, "Db1", tr.dstDB)
		require.Equal(t, map[int]sequenceWatermarkKey{0: {db: "Db1", table: "Seq"}}, tr.cols)

		var tblStuff tableStuff
		tblStuff.def.colTypes = []types.Type{types.T_int64.ToType()}
		tr.capture(tblStuff, []any{int64(7)}, []int{0})
		wm := tr.items[sequenceWatermarkKey{db: "Db1", table: "Seq"}]
		require.NotNil(t, wm)
		require.Equal(t, "Db1", wm.db)
		require.Equal(t, "Seq", wm.table, "engine lookup and SQL use the resolved spelling")
	})
	t.Run("case-sensitive pinned database must match exactly", func(t *testing.T) {
		tr := buildTracker(0, "Db1", seqDefaultExpr(seqNextvalExpr("Seq", "Db1")))
		require.Equal(t, map[int]sequenceWatermarkKey{0: {db: "Db1", table: "Seq"}}, tr.cols)

		foreign := buildTracker(0, "Db1", seqDefaultExpr(seqNextvalExpr("Seq", "db1")))
		require.Empty(t, foreign.cols, "a case-differing pin is foreign under lower_case_table_names=0")
	})
	t.Run("case-insensitive mode folds keys", func(t *testing.T) {
		tr := buildTracker(1, "db1", seqDefaultExpr(seqNextvalExpr("Seq", "DB1")))
		require.Equal(t, map[int]sequenceWatermarkKey{0: {db: "db1", table: "seq"}}, tr.cols)
	})
}

// TestSequenceReconcileCaseSensitiveIdentity pins the reconcile end of
// v12-review F9: under lower_case_table_names=0 a mixed-case identity is
// resolved and updated by its exact spelling, and a folded-name sibling
// sequence in the same database stays untouched.
func TestSequenceReconcileCaseSensitiveIdentity(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		committed: map[[2]string]uint64{{"Db1", "Seq"}: 42, {"Db1", "seq"}: 43},
	})
	f.store.add("Db1", "Seq", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	f.store.add("Db1", "seq", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wm := seqWmRange("Db1", "Seq", 2, 9, 2)
	tracker := newWatermarkTrackerLCTN("Db1", 0, wm)

	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))

	row, ok := f.store.get("Db1", "Seq")
	require.True(t, ok)
	require.Equal(t, uint64(9), row.last, "the exact-spelling identity advances")
	row, ok = f.store.get("Db1", "seq")
	require.True(t, ok)
	require.Equal(t, uint64(1), row.last, "the folded-name sibling identity is untouched")
	require.Equal(t, []string{wantReconcileSelectSQL(wm), wantReconcileUpdateSQL(wm, "9", false)},
		reconcileEventSQLs(f.exec.events, "txn"))
}

// ---------------------------------------------------------------------
// Challenger regression — engine sourcing (challenge-1793265851588).
// ---------------------------------------------------------------------

// TestSequenceReconcileEngineSourcedFromTxnHandler pins the Challenger
// failure: `DATA BRANCH MERGE` returned 20101 "missing engine for sequence
// reconciliation" when the CN ran inside `mo-service -launch`, because the
// reconcile read the engine from proc.Ctx where only compiled statement
// execution attaches it (compile2.go). The fixture deliberately leaves
// proc.Ctx without EngineKey (the merge driver envelope), so every
// reconcile test in this file exercises the txn-handler source; this named
// case pins that sourcing is what the committed advance depends on.
func TestSequenceReconcileEngineSourcedFromTxnHandler(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		committed: map[[2]string]uint64{{"db1", "s"}: 42},
	})
	require.Nil(t, f.ses.proc.Ctx.Value(defines.EngineKey{}), "fixture must not smuggle the engine through proc.Ctx")
	f.store.add("db1", "s", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wm := seqWmRange("db1", "s", 2, 5, 2)
	tracker := newWatermarkTracker("db1", wm)

	require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))
	row, ok := f.store.get("db1", "s")
	require.True(t, ok)
	require.Equal(t, uint64(5), row.last)
	require.True(t, row.called)
}

// TestSequenceReconcileMissingTxnHandlerStorage keeps the deterministic
// error for a genuinely engine-less session (nil txn-handler storage): the
// merge must fail loudly instead of drifting into a nil-interface panic.
func TestSequenceReconcileMissingTxnHandlerStorage(t *testing.T) {
	f := newSeqReconcileFixture(t, &seqReconcileCatalog{
		committed: map[[2]string]uint64{{"db1", "s"}: 42},
	})
	f.ses.txnHandler = InitTxnHandler("", nil, context.Background(), f.callerOp)
	f.store.add("db1", "s", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
	wm := seqWmRange("db1", "s", 2, 5, 2)
	err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, newWatermarkTracker("db1", wm))
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing engine for sequence reconciliation")
}

// TestSequenceReconcileAttemptCtxBoundsEngineResolution pins the reconcile
// unit's attempt-context propagation (design §4 D H2 + the deadline
// propagation disposition): the committed partition's engine resolution runs
// on the ATTEMPT's bounded context, exactly as allocation does. Catalog
// misses in disttae run metadata SQL on the context they receive, so a
// resolution bound to the merge statement's (possibly deadline-less)
// context could block past the per-attempt budget and hold the merge.
// Each arm either captures the context the engine op receives (deadline
// must be the attempt min(statement, budget)) or holds the resolution
// until it fires, with a watchdog well past the compressed budget:
// pre-propagation code hands the engine the caller's own context, so the
// watchdog fires and the arm fails.
func TestSequenceReconcileAttemptCtxBoundsEngineResolution(t *testing.T) {
	errWatchdog := errors.New("reconcile resolution watchdog fired — the attempt context never fired")

	mkFixture := func(t *testing.T) (*seqReconcileFixture, *sequenceWatermarkTracker) {
		f := newSeqReconcileFixture(t, &seqReconcileCatalog{
			committed: map[[2]string]uint64{{"db1", "s"}: 42},
		})
		f.store.add("db1", "s", seqStoreRow{last: 1, minv: 1, maxv: 100, start: 1, incr: 1, called: true})
		wm := seqWmRange("db1", "s", 2, 5, 2)
		return f, newWatermarkTracker("db1", wm)
	}

	t.Run("attempt deadline is installed on a deadline-less statement context", func(t *testing.T) {
		f, tracker := mkFixture(t)
		start := time.Now()
		var seen []time.Time
		f.catalog.dbCtxHook = func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			require.True(t, ok,
				"the reconcile unit's resolution must receive the attempt's bounded context")
			require.WithinDuration(t, start.Add(function.SequenceAllocationRetryBudget()),
				deadline, 2*time.Second,
				"the received deadline is the base attempt budget min(statement, 10 s)")
			seen = append(seen, deadline)
			return nil
		}

		require.NoError(t, reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker))
		require.NotEmpty(t, seen, "the committed partition resolved through the engine")
		row, ok := f.store.get("db1", "s")
		require.True(t, ok)
		require.Equal(t, uint64(5), row.last, "the reconcile still installs the watermark")
		require.Equal(t, []string{"commit"}, f.exec.outcomes)
	})

	t.Run("attempt deadline clamps a farther statement deadline", func(t *testing.T) {
		f, tracker := mkFixture(t)
		stmtCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		start := time.Now()
		var seen []time.Time
		f.catalog.dbCtxHook = func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			// min(statement 30 s, budget): the attempt's own budget wins;
			// without propagation the received context shows the full 30 s.
			require.WithinDuration(t, start.Add(function.SequenceAllocationRetryBudget()),
				deadline, 2*time.Second,
				"the received deadline is the attempt budget, not the raw statement deadline")
			seen = append(seen, deadline)
			return nil
		}

		require.NoError(t, reconcileMergeSequenceWatermarks(stmtCtx, f.ses, f.bh, tracker))
		require.NotEmpty(t, seen)
		require.Equal(t, []string{"commit"}, f.exec.outcomes)
	})

	t.Run("blocked resolution completes at the attempt deadline", func(t *testing.T) {
		restore := function.SetSequenceAutonomousTimingForTesting(
			800*time.Millisecond, 800*time.Millisecond, 5*time.Millisecond)
		defer restore()
		f, tracker := mkFixture(t)

		start := time.Now()
		f.catalog.dbCtxHook = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(4 * time.Second):
				return errWatchdog
			}
		}

		err := reconcileMergeSequenceWatermarks(context.Background(), f.ses, f.bh, tracker)
		elapsed := time.Since(start)
		require.ErrorContains(t, err, "allocation could not acquire the sequence row within 8 attempts",
			"the budget-aborted resolution keeps the named surface")
		require.False(t, errors.Is(err, errWatchdog),
			"the resolution aborted at the attempt deadline, not the watchdog")
		require.Less(t, elapsed, 3*time.Second,
			"the blocked resolution completed at the compressed budget, not at the watchdog")
		require.Equal(t, 1, f.exec.attempts, "one bounded attempt, no overshoot")
		require.Equal(t, []string{"rollback"}, f.exec.outcomes,
			"the aborted attempt rolls the unit back")
		row, ok := f.store.get("db1", "s")
		require.True(t, ok)
		require.Equal(t, uint64(1), row.last, "the failed attempt stores nothing")
	})

	t.Run("caller cancellation keeps precedence over the attempt budget", func(t *testing.T) {
		f, tracker := mkFixture(t)
		stmtCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		start := time.Now()
		f.catalog.dbCtxHook = func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			require.True(t, ok,
				"the received context still carries the attempt deadline when the caller fires first")
			require.WithinDuration(t, start.Add(function.SequenceAllocationRetryBudget()),
				deadline, 2*time.Second)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(4 * time.Second):
				return errWatchdog
			}
		}
		time.AfterFunc(120*time.Millisecond, cancel)

		err := reconcileMergeSequenceWatermarks(stmtCtx, f.ses, f.bh, tracker)
		elapsed := time.Since(start)
		require.True(t, errors.Is(err, context.Canceled),
			"caller cancellation surfaces unchanged, got %v", err)
		require.False(t, errors.Is(err, errWatchdog))
		require.Less(t, elapsed, 3*time.Second,
			"caller cancellation unblocked the resolution at once")
		require.Equal(t, []string{"rollback"}, f.exec.outcomes)
	})
}
