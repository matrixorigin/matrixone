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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type memoryLoadCatalog struct {
	*plan2.MockCompilerContext
	proc   *process.Process
	tables map[string]*plan.TableDef
}

func (c *memoryLoadCatalog) DefaultDatabase() string      { return "test" }
func (c *memoryLoadCatalog) GetProcess() *process.Process { return c.proc }
func (c *memoryLoadCatalog) Resolve(db, name string, _ *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	def := c.tables[strings.ToLower(name)]
	if def == nil {
		return nil, nil, nil
	}
	return &plan.ObjectRef{SchemaName: "test", ObjName: def.Name, Obj: int64(def.TblId), Db: 1}, plan2.DeepCopyTableDef(def, true), nil
}
func (c *memoryLoadCatalog) ResolveIndexTableByRef(ref *plan.ObjectRef, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return c.Resolve(ref.SchemaName, name, snapshot)
}

func TestImportMemoryPublicIndexedLoadCompileIsControlled(t *testing.T) {
	setupMemoryLoadRuntime(t)
	// The modern planner chooses S3 from actual estimated cardinality, not the
	// legacy LoadWriteS3 flag. One MiB of short, legal rows is sufficient to
	// cross that public threshold. Only planning samples the file; no engine
	// executes LOAD and no synthetic plan statistics/flags are injected.
	file := filepath.Join(t.TempDir(), "indexed-load.csv")
	row := "0|\\N|v|x\n" // auto IDs and nullable UNIQUE values are legal in both schemas.
	require.NoError(t, os.WriteFile(file, []byte(strings.Repeat(row, (1<<20)/len(row)+1)), 0600))
	wideFile := filepath.Join(t.TempDir(), "indexed-wide-types.csv")
	var wideCSV strings.Builder
	for id := 1; wideCSV.Len() < 2<<20; id++ {
		fmt.Fprintf(&wideCSV, "%d|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N|\\N\n", id)
	}
	require.NoError(t, os.WriteFile(wideFile, []byte(wideCSV.String()), 0600))
	for _, fixture := range []struct {
		name, ddl, file string
		defaultLock     bool
		headroom        int64
	}{
		{name: "true", ddl: "create table memory_indexed_load (id bigint auto_increment primary key, code varchar(64), v varchar(128), body text, unique key uq(code), key ix(v), fulltext key ft(body) with parser ngram)", file: file},
		{name: "false", ddl: "create table memory_indexed_load (id bigint auto_increment primary key, v varchar(128), body text, other text, key ix(v), fulltext key ft(body) with parser ngram, fulltext key ft2(other) with parser ngram)", file: file},
		{name: "varlen-key-json-vector-two-fulltext", ddl: "create table memory_indexed_load (id varchar(255) not null primary key, question text, payload json, keywords text, embedding vecf64(1024), access_text varchar(511), identity_text varchar(512), deleted int, created_at timestamp default current_timestamp(), updated_at timestamp default current_timestamp() on update current_timestamp(), fulltext key ft_question(question) with parser ngram, fulltext key ft_keywords(keywords) with parser ngram, key ix_access_identity(access_text, identity_text))", file: wideFile, defaultLock: true, headroom: 22 << 30},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			cc, ctrl := newMemoryLoadCatalog(t, fixture.defaultLock)
			sql := fmt.Sprintf("load data infile '%s' into table memory_indexed_load fields terminated by '|' parallel 'true'", fixture.file)
			c := compileMemoryLoad(t, cc, ctrl, fixture.ddl, sql, 1)
			proc, ctx := c.proc, c.proc.Ctx
			var writeS3, flush int
			var walk func(*Scope)
			walk = func(scope *Scope) {
				for _, child := range scope.PreScopes {
					walk(child)
				}
				require.NoError(t, vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
					if update, ok := op.(*multi_update.MultiUpdate); ok {
						if update.Action == multi_update.UpdateWriteS3 {
							writeS3++
						}
						if update.Action == multi_update.UpdateFlushS3Info {
							flush++
						}
					}
					return nil
				}))
			}
			for _, scope := range c.scopes {
				walk(scope)
			}
			require.Positive(t, writeS3)
			require.Positive(t, flush)
			m := &compilerMemoryAdmission{real: rscthrottler.NewMemThrottler(t.Name(), .9), available: math.MaxInt64}
			if fixture.headroom > 0 {
				m.available = fixture.headroom
				m.real = rscthrottler.NewMemThrottler(t.Name(), .9, rscthrottler.WithConstLimit(fixture.headroom))
			}
			require.NoError(t, c.beginStatementImportMemory(ctx, m))
			require.NotNil(t, c.importMemory, "the real public indexed LOAD DAG must retain a complete ticket through every gate")
			require.NotNil(t, c.importMemory.WorkingSet)
			c.endImportMemory()
			require.Nil(t, proc.Base.ImportMemory)
		})
	}
}

// Public LOAD controls share catalog/engine setup while keeping their SQL,
// transaction policy, topology and admission oracles in the individual tests.
func setupMemoryLoadRuntime(t *testing.T) {
	t.Helper()
	previousRuntime := moruntime.ServiceRuntime("")
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	t.Cleanup(func() { moruntime.SetupServiceBasedRuntime("", previousRuntime) })
	catalog.SetupDefines("")
	// CREATE asks the internal executor whether catalog FK metadata refers to
	// this new table. Reuse the planner's empty-catalog executor seam.
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(string) (executor.Result, error) { return executor.Result{}, nil }))
	client, err := cnclient.NewPipelineClient("", "test", &cnclient.PipelineConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
}

func newMemoryLoadCatalog(t *testing.T, defaultLock bool) (*memoryLoadCatalog, *gomock.Controller) {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	t.Cleanup(func() { proc.Free(); proc.GetSessionInfo().Buf.Free() })
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "STRICT_TRANS_TABLES", nil })
	ctrl := gomock.NewController(t)
	if defaultLock {
		lockConfig := lockservice.Config{ServiceID: t.Name()}
		lockConfig.Validate()
		// Retain the existing isolated runtime namespace. Only the
		// service identity changes; all real default capacity fields stay.
		lockConfig.ServiceID = ""
		lockSvc := mock_lock.NewMockLockService(ctrl)
		lockSvc.EXPECT().GetConfig().Return(lockConfig).AnyTimes()
		proc.Base.LockService = lockSvc
	}
	txnClient, txnOp := newTestTxnClientAndOp(ctrl)
	txnClient.(*mock_frontend.MockTxnClient).EXPECT().RefreshExpressionEnabled().Return(false).AnyTimes()
	proc.Base.TxnClient, proc.Base.TxnOperator = txnClient, txnOp
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)
	base := plan2.NewEmptyCompilerContext()
	base.SetContext(ctx)
	base.DatabaseExistsFunc = func(string, *plan.Snapshot) bool { return true }
	base.GetDatabaseIdFunc = func(string, *plan.Snapshot) (uint64, error) { return 1, nil }
	cc := &memoryLoadCatalog{MockCompilerContext: base, proc: proc, tables: make(map[string]*plan.TableDef)}
	return cc, ctrl
}

func compileMemoryLoad(t *testing.T, cc *memoryLoadCatalog, ctrl *gomock.Controller, ddl, sql string, parallel int) *Compile {
	t.Helper()
	proc, ctx := cc.proc, cc.proc.Ctx
	statements, err := mysql.Parse(ctx, ddl, 1)
	require.NoError(t, err)
	create, err := plan2.BuildPlan(cc, statements[0], false)
	require.NoError(t, err)
	ct := create.GetDdl().GetCreateTable()
	require.NotNil(t, ct)
	all := append([]*plan.TableDef{ct.TableDef}, ct.IndexTables...)
	mockEngine := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	mockEngine.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(db, nil).AnyTimes()
	const addr = "127.0.0.1:6001"
	mockEngine.EXPECT().Nodes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(engine.Nodes{{Id: "test", Addr: addr, Mcpu: parallel}}, nil).AnyTimes()
	mockEngine.EXPECT().GetMessageCenter().Return(nil).AnyTimes()
	db.EXPECT().GetDatabaseId(gomock.Any()).Return("1").AnyTimes()
	for i, def := range all {
		def.TblId = uint64(90000 + i)
		def.DbName, def.TableType = "test", catalog.SystemOrdinaryRel
		def.Cols = append(def.Cols, plan2.MakeRowIdColDef())
		def.Name2ColIndex = make(map[string]int32)
		for j, col := range def.Cols {
			def.Name2ColIndex[col.Name] = int32(j)
		}
		cc.tables[strings.ToLower(def.Name)] = def
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		rel.EXPECT().CopyTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef { return plan2.DeepCopyTableDef(def, true) }).AnyTimes()
		rel.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), def.Name, gomock.Any()).Return(rel, nil).AnyTimes()
	}
	statements, err = mysql.Parse(ctx, sql, 1)
	require.NoError(t, err)
	pn, err := plan2.BuildPlan(cc, statements[0], false)
	require.NoError(t, err)
	c := NewCompile(addr, "test", sql, "", "", mockEngine, proc, statements[0], false, nil, time.Now())
	t.Cleanup(c.Release)
	t.Cleanup(c.endImportMemory)
	c.ncpu = parallel
	require.NoError(t, c.Compile(ctx, pn, testPrint))
	return c
}
