// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// This provider applies the injected real byte budget to the whole candidate
// list, then uses an actual ticket. A direct old-policy call demonstrates that
// a smaller unit really could have succeeded before the D33 preference.
type staticTargetAdmission struct {
	*compilerMemoryAdmission
	limit        int64
	reserveCalls int
	selected     []rscthrottler.WorkingSetSpec
	onHeadroom   func()
}

func (m *staticTargetAdmission) WorkingSetHeadroom() rscthrottler.WorkingSetHeadroomSnapshot {
	if m.onHeadroom != nil {
		m.onHeadroom()
	}
	return rscthrottler.WorkingSetHeadroomSnapshot{Limit: m.limit, Available: m.available, HasOwners: m.owners}
}

func (m *staticTargetAdmission) ReserveWorkingSetCandidates(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	m.reserveCalls++
	m.selected = specs
	for _, spec := range specs {
		if workingLayoutTotal(spec) <= m.available {
			return rscthrottler.ReserveWorkingSet(ctx, m.real, spec)
		}
	}
	return nil, &rscthrottler.WorkingSetCapacityError{Minimum: workingLayoutTotal(specs[len(specs)-1]), Available: m.available}
}

func TestImportMemoryTargetImpossiblePrefersDOPBeforeFittingSmallerUnit(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	specs, _, err := memoryManifestCandidates(manifest, 64<<20)
	require.NoError(t, err)
	require.Greater(t, len(specs), 1)
	target, minimum := workingLayoutTotal(specs[0]), workingLayoutTotal(specs[len(specs)-1])
	require.Greater(t, target, minimum)
	budget := minimum + (target-minimum)/2
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: budget}, limit: budget}
	old, err := m.ReserveWorkingSetCandidates(context.Background(), specs)
	require.NoError(t, err)
	t.Cleanup(old.Close)
	require.Less(t, old.UnitBytes(), specs[0].UnitBytes, "this is not the already-covered all-minima-impossible case")
	old.Close()
	m.reserveCalls = 0
	err = c.admitImportMemory(context.Background(), m)
	var retry *importDOPRetryError
	require.ErrorAs(t, err, &retry)
	require.Equal(t, 1, retry.next)
	require.Zero(t, m.reserveCalls, "the static target proof must request recompile before any ticket")
	require.Nil(t, c.proc.Base.ImportMemory)
	require.Equal(t, 2, c.ncpu)
	require.Equal(t, 2, c.scopes[0].NodeInfo.Mcpu, "the old graph stays wired until actual recompile")
}

func TestImportMemoryTargetLimitBoundaryAndUnknownProvider(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta int64
		known bool
		retry bool
	}{
		{name: "target_exceeds_limit_by_one", delta: -1, known: true, retry: true},
		{name: "target_equals_limit", known: true},
		{name: "limit_exceeds_target_by_one", delta: 1, known: true},
		{name: "unknown_zero_limit"},
		{name: "unknown_negative_limit", delta: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := memoryCompileFixture(t, 2)
			_, target, _, err := c.importMemoryLayout(64 << 20)
			require.NoError(t, err)
			total := workingLayoutTotal(target)
			limit, available := test.delta, total
			if test.known {
				limit = total + test.delta
				available = min(total, limit)
			}
			m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: available}, limit: limit}
			err = c.admitImportMemory(context.Background(), m)
			if test.retry {
				require.Equal(t, 1, MemoryRetryParallel(err))
				require.Zero(t, m.reserveCalls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, m.reserveCalls)
				require.Equal(t, int64(64<<20), c.proc.Base.ImportMemory.UnitBytes)
			}
		})
	}
}

func TestImportMemoryTargetStaticFitKeepsActiveOwnerSmallerCandidate(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	manifest, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	specs, _, err := memoryManifestCandidates(manifest, 64<<20)
	require.NoError(t, err)
	target, minimum := workingLayoutTotal(specs[0]), workingLayoutTotal(specs[len(specs)-1])
	available := minimum + (target-minimum)/2
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{
		real: rscthrottler.NewMemThrottler(t.Name(), .9), available: available, owners: true,
	}, limit: target}
	require.NoError(t, c.admitImportMemory(context.Background(), m))
	require.Equal(t, 1, m.reserveCalls)
	require.Equal(t, len(specs), len(m.selected), "transient owner pressure must keep all candidate units")
	require.Less(t, c.proc.Base.ImportMemory.UnitBytes, int64(64<<20))
	require.Equal(t, 2, c.ncpu)
}

func TestImportMemoryTargetRefreshCancellationWinsOverRetry(t *testing.T) {
	c := memoryCompileFixture(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{}, limit: 1, onHeadroom: cancel}
	err := c.admitImportMemory(ctx, m)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, MemoryRetryParallel(err))
	require.Zero(t, m.reserveCalls)
	require.Nil(t, c.proc.Base.ImportMemory)
}

func TestImportMemoryTargetTotalSaturatesInsteadOfWrapping(t *testing.T) {
	for _, spec := range []rscthrottler.WorkingSetSpec{
		{Inputs: []rscthrottler.InputSlotSpec{{Count: 2, Bytes: math.MaxInt64}}},
		{Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: math.MaxInt64}}, IOBytes: 1},
	} {
		require.Equal(t, int64(math.MaxInt64), workingLayoutTotal(spec), "overflow cannot make an impossible target look small")
	}
}

func TestImportMemoryPublicNarrowTargetFitPreservesPlanAndDOP(t *testing.T) {
	previousRuntime := moruntime.ServiceRuntime("")
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	t.Cleanup(func() { moruntime.SetupServiceBasedRuntime("", previousRuntime) })
	catalog.SetupDefines("")
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(string) (executor.Result, error) { return executor.Result{}, nil }))
	client, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	file := filepath.Join(t.TempDir(), "narrow.csv")
	require.NoError(t, os.WriteFile(file, []byte("1|2|3|4\n"), 0600))
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	t.Cleanup(func() { proc.Free(); proc.GetSessionInfo().Buf.Free() })
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "STRICT_TRANS_TABLES", nil })
	ctrl := gomock.NewController(t)
	txnClient, txnOp := newTestTxnClientAndOp(ctrl)
	txnClient.(*mock_frontend.MockTxnClient).EXPECT().RefreshExpressionEnabled().Return(false).AnyTimes()
	proc.Base.TxnClient = txnClient
	// This public control represents an ordinary autocommit statement. The
	// shared helper's zero options deliberately remain conservative for D34.
	proc.Base.TxnOperator = &importMemoryTxnOptions{TxnOperator: txnOp, options: txn.TxnOptions{Autocommit: true}}
	lockConfig := lockservice.Config{ServiceID: t.Name()}
	lockConfig.Validate()
	lockConfig.ServiceID = ""
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockConfig).AnyTimes()
	proc.Base.LockService = lockSvc
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)
	base := plan2.NewEmptyCompilerContext()
	base.SetContext(ctx)
	base.DatabaseExistsFunc = func(string, *plan.Snapshot) bool { return true }
	base.GetDatabaseIdFunc = func(string, *plan.Snapshot) (uint64, error) { return 1, nil }
	cc := &memoryLoadCatalog{MockCompilerContext: base, proc: proc, tables: make(map[string]*plan.TableDef)}
	statements, err := mysql.Parse(ctx, "create table memory_narrow_load (id bigint primary key, a bigint, b bigint, c bigint)", 1)
	require.NoError(t, err)
	create, err := plan2.BuildPlan(cc, statements[0], false)
	require.NoError(t, err)
	def := create.GetDdl().GetCreateTable().TableDef
	def.TblId, def.DbName, def.TableType = 91000, "test", catalog.SystemOrdinaryRel
	def.Cols = append(def.Cols, plan2.MakeRowIdColDef())
	def.Name2ColIndex = make(map[string]int32)
	for i, col := range def.Cols {
		def.Name2ColIndex[col.Name] = int32(i)
	}
	cc.tables[strings.ToLower(def.Name)] = def
	mockEngine := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	mockEngine.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(db, nil).AnyTimes()
	const addr = "127.0.0.1:6001"
	mockEngine.EXPECT().Nodes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(engine.Nodes{{Id: "test", Addr: addr, Mcpu: 4}}, nil).AnyTimes()
	mockEngine.EXPECT().GetMessageCenter().Return(nil).AnyTimes()
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("1").AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().CopyTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef { return plan2.DeepCopyTableDef(def, true) }).AnyTimes()
	rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), def.Name, gomock.Any()).Return(rel, nil).AnyTimes()
	// LOCAL/FALSE uses its real public planner fallback; no stats, write flags,
	// source schema or threshold are injected to obtain the physical graph.
	sql := fmt.Sprintf("load data local infile '%s' into table memory_narrow_load fields terminated by '|' parallel 'false'", file)
	statements, err = mysql.Parse(ctx, sql, 1)
	require.NoError(t, err)
	pn, err := plan2.BuildPlan(cc, statements[0], false)
	require.NoError(t, err)
	c := NewCompile(addr, "test", sql, "", "", mockEngine, proc, statements[0], false, nil, time.Now())
	t.Cleanup(c.Release)
	t.Cleanup(c.endImportMemory)
	c.ncpu = 4
	require.NoError(t, c.Compile(ctx, pn, testPrint))
	require.True(t, c.supportsBoundedImportKeys())
	before, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	// Parallel LOAD writes through independent sibling scopes, each with
	// Mcpu=1. Count the actual writers and their feeding dispatch instead of
	// confusing one scope's copy count with the width of the whole pipeline.
	writers, fanout := 0, 0
	seen := make(map[*Scope]bool)
	var walk func(*Scope)
	walk = func(scope *Scope) {
		if seen[scope] {
			return
		}
		seen[scope] = true
		for _, child := range scope.PreScopes {
			walk(child)
		}
		require.NoError(t, vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			switch op := op.(type) {
			case *multi_update.MultiUpdate:
				if op.Action == multi_update.UpdateWriteS3 {
					writers++
				}
			case *insert.Insert:
				if op.ToWriteS3 {
					writers++
				}
			case *dispatch.Dispatch:
				fanout = max(fanout, len(op.LocalRegs))
			}
			return nil
		}))
	}
	for _, scope := range c.scopes {
		walk(scope)
	}
	if writers != 4 || fanout != 4 {
		for i, node := range pn.GetQuery().Nodes {
			t.Logf("public node%d type=%v stats=%+v", i, node.NodeType, node.Stats)
		}
	}
	require.Equal(t, 4, writers, "the public narrow control must actually retain four physical S3 writers")
	require.Equal(t, 4, fanout, "the source dispatch must feed those four writer scopes")
	specs, _, err := memoryManifestCandidates(before, 64<<20)
	require.NoError(t, err)
	require.NotEmpty(t, specs)
	require.LessOrEqual(t, workingLayoutTotal(specs[0]), int64(22<<30))
	m := &staticTargetAdmission{compilerMemoryAdmission: &compilerMemoryAdmission{
		real: rscthrottler.NewMemThrottler(t.Name(), .9), available: 22 << 30,
	}, limit: 22 << 30}
	require.NoError(t, c.beginStatementImportMemory(ctx, m))
	require.NotNil(t, c.importMemory)
	require.Equal(t, 1, m.reserveCalls)
	require.Equal(t, int64(64<<20), c.importMemory.UnitBytes)
	require.Equal(t, 4, c.ncpu)
	after, err := c.localMemoryManifest(64 << 20)
	require.NoError(t, err)
	require.Equal(t, before, after, "a statically fitting public narrow LOAD keeps its full physical DAG")
}
