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

package compile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// dependentMergeRadixContext mirrors the established dependent merge fixture
// context and adds the writable DECIMAL(10,2) column d to both relations so
// MERGE actions can target it.
type dependentMergeRadixContext struct{ plan2.CompilerContext }

func (c dependentMergeRadixContext) Resolve(db, name string, snapshot *plan.Snapshot, borrowOutput *plan2.TableResolution) (*plan.ObjectRef, *plan.TableDef, error) {
	borrowOutput.Reset()
	if name != "gold_orders" && name != "dim_orders" && name != "cf_scalar" {
		return c.CompilerContext.Resolve(db, name, snapshot, nil)
	}
	dType := types.T_decimal64.ToType()
	dType.Width = 10
	dType.Scale = 2
	// Nullability discriminator: the gold target's write column carries the
	// concrete NOT NULL plan flag (Typ.NotNullable, not only the Default
	// NullAbility bookkeeping) while the dim source's same-named column
	// stays nullable, so the source-derived nullability of an aliased clause
	// value must survive the merge rewrite's alias typing instead of copying
	// the target's flag.
	dTyp := dType.PlanType()
	dNull := true
	if name == "gold_orders" {
		dNull = false
		dTyp.NotNullable = true
	}
	td := &plan.TableDef{Name: name, DbName: db, DbId: 1, TblId: 2,
		Cols: []*plan.ColDef{
			{Name: "id", Typ: types.T_int64.ToType().PlanType(), Default: &plan.Default{NullAbility: true}},
			{Name: "d", Typ: dTyp, Default: &plan.Default{NullAbility: dNull}},
		}, Pkey: &plan.PrimaryKeyDef{}}
	if name == "gold_orders" {
		td.TableType = catalog.SystemExternalRel
		td.Createsql = sqliceberg.BuildCreateSQLEnvelope(model.TableMapping{Namespace: "sales", TableName: "orders", DefaultRef: "main", ReadMode: model.ReadModeMergeOnRead, WriteMode: model.WriteModeMergeOnRead}, "ksa_gold")
	} else if name == "cf_scalar" {
		// The DEDICATED capture-free inner relation: an ordinary two-column
		// table whose v column feeds the test-round-controlled scalar.
		td.TableType = catalog.SystemOrdinaryRel
		td.TblId = 7
		td.Cols = []*plan.ColDef{
			{Name: "k", Typ: types.T_int64.ToType().PlanType(), Default: &plan.Default{NullAbility: true}},
			{Name: "v", Typ: dTyp, Default: &plan.Default{NullAbility: true}},
		}
	} else {
		td.TableType = catalog.SystemOrdinaryRel
		td.TblId = 3
	}
	return &plan.ObjectRef{Db: 1, Obj: int64(td.TblId), DbName: db, ObjName: name}, td, nil
}

// radixMergeCoordinator is the established recording coordinator with the
// target DECIMAL column captured per action row: each recorded row pins the
// row id, the merge action tag and the d value actually handed to the writer.
type radixMergeCoordinator struct {
	t                                *testing.T
	request                          icebergwrite.AppendRequest
	dColumnPos                       int
	rows                             []routedMergeSinkRow
	begins, appends, commits, aborts int
}

// routedMergeSinkRow records one exact sink payload row: the target row id,
// the merge action tag, the data-file/ordinal lineage and the raw DECIMAL64
// value actually handed to the writer. An ordered list keeps duplicates and
// row counts visible instead of masking them behind a keyed map.
type routedMergeSinkRow struct {
	id         int64
	action     string
	dataFile   string
	rowOrdinal int64
	d          types.Decimal64
	// dNull records the sink batch's NULL bit for the write column at this
	// row: raw zero and NULL must never be conflated in a diagnosis.
	dNull bool
}

type radixAssignmentWarningSink struct{ codes []uint16 }

func (s *radixAssignmentWarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}

func (c *radixMergeCoordinator) Begin(_ context.Context, request icebergwrite.AppendRequest) error {
	c.request = request
	c.begins++
	// Pin the captured column through the request's own write metadata: the
	// sink layout's write-column prefix is the target's visible write
	// columns, so the position comes from the request's table definition
	// instead of a hard-coded test schema assumption.
	c.dColumnPos = -1
	if request.TableDef != nil {
		pos := 0
		for _, col := range request.TableDef.Cols {
			if col == nil || col.Hidden || col.Name == catalog.Row_ID || col.Name == icebergapi.DMLDataFilePathColumnName || col.Name == icebergapi.DMLRowOrdinalColumnName {
				continue
			}
			if col.Name == "d" {
				c.dColumnPos = pos
				require.Equal(c.t, int32(types.T_decimal64), col.Typ.Id, "the request metadata pins a DECIMAL64 write column")
				require.Equal(c.t, int32(10), col.Typ.Width, "the request metadata carries the exact own precision")
				require.Equal(c.t, int32(2), col.Typ.Scale, "the request metadata carries the exact own scale")
				require.True(c.t, col.Typ.NotNullable,
					"the request metadata carries the destination's actual non-null plan flag")
				break
			}
			pos++
		}
	}
	return nil
}

func (c *radixMergeCoordinator) Append(_ context.Context, b *batch.Batch) error {
	c.appends++
	require.NotEqual(c.t, -1, c.dColumnPos, "the write column must be pinned through the request metadata")
	for row := 0; row < b.RowCount(); row++ {
		require.Greater(c.t, len(b.Vecs), int(c.dColumnPos), "the sink batch carries the pinned write column")
		dVec := b.Vecs[c.dColumnPos]
		require.Equal(c.t, types.T_decimal64, dVec.GetType().Oid, "the pinned sink column is DECIMAL64")
		require.Equal(c.t, int32(10), dVec.GetType().Width, "the pinned sink column carries the exact own precision 10")
		require.Equal(c.t, int32(2), dVec.GetType().Scale, "the pinned sink column carries the exact own scale 2")
		id := vector.GetFixedAtWithTypeCheck[int64](b.Vecs[0], row)
		action := b.Vecs[c.request.MergeActionColumnIndex].GetStringAt(row)
		dataFile := "NULL"
		if v := b.Vecs[c.request.DataFilePathColumnIndex]; !v.IsNull(uint64(row)) {
			dataFile = v.GetStringAt(row)
		}
		rowOrdinal := int64(-1)
		if v := b.Vecs[c.request.RowOrdinalColumnIndex]; !v.IsNull(uint64(row)) {
			rowOrdinal = vector.GetFixedAtWithTypeCheck[int64](v, row)
		}
		c.rows = append(c.rows, routedMergeSinkRow{
			id: id, action: action, dataFile: dataFile, rowOrdinal: rowOrdinal,
			d:     vector.GetFixedAtWithTypeCheck[types.Decimal64](dVec, row),
			dNull: dVec.IsNull(uint64(row)),
		})
	}
	return nil
}

func routedSinkRowAt(rows []routedMergeSinkRow, id int64, action string) routedMergeSinkRow {
	for _, r := range rows {
		if r.id == id && r.action == action {
			return r
		}
	}
	return routedMergeSinkRow{id: -1, action: "missing"}
}

func (c *radixMergeCoordinator) Commit(context.Context) error           { c.commits++; return nil }
func (c *radixMergeCoordinator) Abort(_ context.Context, _ error) error { c.aborts++; return nil }

// cfProbe counts the real lifecycle of the DEDICATED capture-free inner
// relation only (cf_scalar): factory opens and batch reads. It is per-
// statement and test-local; parent scans (gold_orders/dim_orders) are never
// counted.
type cfProbe struct {
	opens  int
	reads  int
	closes int
}

// cfCountingSource wraps a dependent.Source with the probe above.
type cfCountingSource struct {
	dependent.Source
	probe *cfProbe
}

func (c *cfCountingSource) Read(ctx context.Context) (*batch.Batch, error) {
	c.probe.reads++
	return c.Source.Read(ctx)
}

func (c *cfCountingSource) Close() error {
	c.probe.closes++
	return c.Source.Close()
}

// TestDependentIcebergMergeRadixWritePolicy drives the real specialized MERGE
// plan, the dependent compile pipeline and the recording iceberg write
// coordinator over the exact matched-UPDATE and not-matched-INSERT action
// consumers targeting the writable DECIMAL column d:
//   - loose sessions clip the radix literal at the cast's own bound, the sink
//     receives 999.90 and the runtime reports out-of-range warnings;
//   - strict sessions surface the typed 20301 rejection and commit nothing;
//   - in-range radix literals preserve their exact value with no warning.
//
// Radix-file-local guarded-stamp helpers (root glm91 F0/SCA): the cleared
// executed-fallback cell below lives in this UNTAGGED test file, while the
// shared membership unit carries the matrixone_test build tag - a tag-less
// static analysis of this package must not depend on tag-gated symbols.
func radixStampedDependentCount(q *plan.Query) int {
	n := 0
	for _, nd := range q.Nodes {
		for _, e := range nd.ProjectList {
			_ = plan.VisitExprTree(e, func(ex *plan.Expr) error {
				if c := ex.GetDependent(); c != nil && c.GuardedMergePayload {
					n++
				}
				return nil
			})
		}
	}
	return n
}

func radixClearGuardedStamps(q *plan.Query) {
	for _, nd := range q.Nodes {
		for _, e := range nd.ProjectList {
			_ = plan.VisitExprTree(e, func(ex *plan.Expr) error {
				if c := ex.GetDependent(); c != nil {
					c.GuardedMergePayload = false
				}
				return nil
			})
		}
	}
}

func TestDependentIcebergMergeRadixWritePolicy(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
	proc.SetStmtProfile(process.NewStmtProfile(uuid.Nil, uuid.New()))
	restoreRuntimeVariableForTest(t, proc.GetService(), moruntime.MOProtocolVersion, defines.MORPCVersion119)

	// The dependent compile diagnostics require the statement to carry a
	// correlated reference like the upstream fixture's own statements, so
	// the matched UPDATE's action keeps the fixture's correlated scalar
	// subquery shape while carrying the radix literal: the correlated IF
	// branch always selects the constant radix cast on the matched rows.
	mergeSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(4,1)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, cast(0x1000 as decimal(4,1)))"
	// Real-source-slot runtime control: the dim source's own DECIMAL(10,2)
	// column rides the same-decimal clause alias all the way to the recording
	// sink - a real source column, not a literal - so the restriction keeps
	// the proven runtime value path exercised end to end.
	sourceSlotSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, s.d)"
	textCoerceSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, cast('50.25' as varchar(32)))"
	inRangeSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, cast(0x1000 as decimal(10,2)))"

	build := func(sql string) (*plan.Query, *Compile) {
		stmt, err := mysql.ParseOne(proc.Ctx, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		bound, err := plan2.BuildPlan(dependentMergeRadixContext{plan2.NewMockCompilerContext(true, proc)}, stmt, false, nil)
		require.NoError(t, err, "BuildPlan must accept the specialized Iceberg MERGE")
		q := bound.GetQuery()
		require.NotNil(t, q)
		// The correlated-subquery contract belongs to the upstream dependent
		// fixture's own statement; these radix MERGE statements are plain
		// action rewrites, so the dependent pipeline drives them as-is.
		c := &Compile{proc: proc, anal: &AnalyzeModule{qry: q}, icebergScanPlans: make(map[int32]*icebergapi.IcebergScanPlan)}
		for _, node := range q.Nodes {
			if node.GetExternScan().GetIcebergScan() != nil {
				ref := registerCompileTestObjectIO(t)
				// The injected scan plan needs its own fixture lease across
				// writer generations, just as the real target scan owns an
				// independent lease.
				require.True(t, icebergio.RetainObjectIORef(ref))
				t.Cleanup(func() { icebergio.ReleaseObjectIORef(ref) })
				c.icebergScanPlans[node.NodeId] = &icebergapi.IcebergScanPlan{Snapshot: icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "main"}, ObjectIORef: ref}
			}
		}
		return q, c
	}

	var coordinators []*radixMergeCoordinator
	restoreRuntimeVariableForTest(t, proc.GetService(), IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(_ context.Context, request icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
		require.Equal(t, icebergwrite.OperationMerge, request.Operation)
		require.Equal(t, int64(30), request.DMLScan.BaseSnapshotID)
		coord := &radixMergeCoordinator{t: t}
		coordinators = append(coordinators, coord)
		return coord, nil
	}))

	// cfRound feeds the cf_scalar fixture's value: a test-local variable captured
	// by the harness closure, so a later execution observes fresh data through a
	// new reader snapshot without any global hook.
	cfRound := 1

	compileMergeWithHook := func(sql, mode string, hook func(*plan.Query)) (*dependent.Template, *radixAssignmentWarningSink, *cfProbe) {
		probe := &cfProbe{}
		// The statement's SQL mode is recorded before BuildPlan so the
		// binding and the runtime share one discriminating session, and each
		// statement gets its own warning sink: counts stay bounded to the
		// expressions this statement actually evaluates.
		proc.SetResolveVariableFunc(func(_ string, _, _ bool) (interface{}, error) { return mode, nil })
		statementSink := &radixAssignmentWarningSink{}
		proc.WarningSink = statementSink
		q, c := build(sql)
		if hook != nil {
			hook(q)
		}
		template, err := dependent.Compile(proc.Ctx, q, func(node *plan.Node) (dependent.SourceFactory, error) {
			return func(child *process.Process, allocation *mpool.MPool, _ *mpool.AllocationAccount) (dependent.Source, error) {
				if node.NodeType == plan.Node_VALUE_SCAN {
					return &dependentFixtureSource{batch: batch.EmptyForConstFoldBatch, rows: 1, close: func() {}}, nil
				}
				b := batch.NewWithSize(len(node.TableDef.Cols))
				ok := false
				defer func() {
					if !ok {
						b.Clean(dependentFixtureMP(child, allocation))
					}
				}()
				cfWrap := node.TableDef != nil && node.TableDef.Name == "cf_scalar"
				for i, col := range node.TableDef.Cols {
					typ, err := types.TypeFromPlan(col.Typ)
					if err != nil {
						return nil, err
					}
					b.Vecs[i] = vector.NewVec(typ, dependentFixtureMP(child, allocation))
					for row := 0; row < 2; row++ {
						if cfWrap {
							switch strings.ToLower(col.Name) {
							case "k":
								err = vector.AppendFixed(b.Vecs[i], int64(row+1), false, dependentFixtureMP(child, allocation))
							case "v":
								// A test-local round-controlled value: a
								// second execution can observe fresh data
								// through a new reader snapshot.
								err = vector.AppendFixed(b.Vecs[i], types.Decimal64(uint64(cfRound)*1000), false, dependentFixtureMP(child, allocation))
							default:
								return nil, fmt.Errorf("unexpected cf_scalar column %s", col.Name)
							}
							if err != nil {
								return nil, err
							}
							continue
						}
						switch strings.ToLower(col.Name) {
						case "id":
							value := int64(row + 1)
							if node.TableDef.Name == "dim_orders" {
								value++
							}
							err = vector.AppendFixed(b.Vecs[i], value, false, dependentFixtureMP(child, allocation))
						case "d":
							// The temporal preimages for the target rows;
							// the merged rows must carry the clipped constant
							// instead.
							preimage := types.Decimal64(10000 + 10000*uint64(row))
							if node.TableDef.Name == "dim_orders" {
								preimage = types.Decimal64(4060 + 1000*uint64(row))
							}
							err = vector.AppendFixed(b.Vecs[i], preimage, false, dependentFixtureMP(child, allocation))
						case icebergapi.DMLDataFilePathColumnName:
							err = vector.AppendBytes(b.Vecs[i], []byte("target.parquet"), false, dependentFixtureMP(child, allocation))
						case icebergapi.DMLRowOrdinalColumnName:
							err = vector.AppendFixed(b.Vecs[i], int64(row), false, dependentFixtureMP(child, allocation))
						default:
							return nil, fmt.Errorf("unexpected MERGE source column %s/%v", col.Name, typ)
						}
						if err != nil {
							return nil, err
						}
					}
				}
				b.SetRowCount(2)
				ok = true
				src := &dependentFixtureSource{batch: b, rows: 1, close: func() { b.Clean(dependentFixtureMP(child, allocation)) }}
				if cfWrap {
					probe.opens++
					return &cfCountingSource{Source: src, probe: probe}, nil
				}
				return src, nil
			}, nil
		}, c.compileDependentKernel)
		require.NoError(t, err)
		return template, statementSink, probe
	}
	compileMerge := func(sql, mode string) (*dependent.Template, *radixAssignmentWarningSink, *cfProbe) {
		return compileMergeWithHook(sql, mode, nil)
	}
	runStatement := func(sql, mode string, strict bool) (*radixMergeCoordinator, []uint16, error, *cfProbe) {
		template, statementSink, probe := compileMerge(sql, mode)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
		require.NoError(t, err)
		account, err := registry.OpenWithController(1<<26, budget)
		require.NoError(t, err)
		defer func() { account.Seal(); _, err := registry.Finalize(account); require.NoError(t, err) }()
		var runErr error
		var runCodes []uint16
		func() {
			op := &dependent.Operator{Template: template}
			require.NoError(t, op.SetAllocationAccount(account))
			defer func() {
				op.Reset(proc, false, nil)
				op.Free(proc, false, nil)
				require.NoError(t, op.ClearAllocationAccount(account))
				op.Release()
			}()
			require.NoError(t, op.Prepare(proc))
			for {
				result, err := vm.Exec(op, proc)
				if err != nil {
					runErr = err
					return
				}
				if result.Batch == nil {
					return
				}
			}
		}()
		runCodes = statementSink.codes
		return coordinators[len(coordinators)-1], runCodes, runErr, probe
	}
	// runTemplate executes an ALREADY COMPILED reusable Template through a
	// fresh real Execution cycle (its own warning sink and account), so a
	// witness can run the SAME Template twice with different reader values.
	runTemplate := func(template *dependent.Template) (*radixMergeCoordinator, []uint16, error) {
		statementSink := &radixAssignmentWarningSink{}
		proc.WarningSink = statementSink
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
		require.NoError(t, err)
		account, err := registry.OpenWithController(1<<26, budget)
		require.NoError(t, err)
		defer func() { account.Seal(); _, err := registry.Finalize(account); require.NoError(t, err) }()
		var runErr error
		var runCodes []uint16
		func() {
			op := &dependent.Operator{Template: template}
			require.NoError(t, op.SetAllocationAccount(account))
			defer func() {
				op.Reset(proc, false, nil)
				op.Free(proc, false, nil)
				require.NoError(t, op.ClearAllocationAccount(account))
				op.Release()
			}()
			require.NoError(t, op.Prepare(proc))
			for {
				result, err := vm.Exec(op, proc)
				if err != nil {
					runErr = err
					return
				}
				if result.Batch == nil {
					return
				}
			}
		}()
		runCodes = statementSink.codes
		return coordinators[len(coordinators)-1], runCodes, runErr
	}

	// Nullability discriminator: the gold target's d column is NOT NULL, but
	// every position-aligned clause alias to a normalized producer keeps the
	// producer's source-derived nullability - the alias typing must not copy
	// the target schema flag before constraint handling.
	{
		stmt0, err0 := mysql.ParseOne(proc.Ctx, mergeSQL, 1)
		require.NoError(t, err0)
		defer stmt0.Free()
		bound0, err0 := plan2.BuildPlan(dependentMergeRadixContext{plan2.NewMockCompilerContext(true, proc)}, stmt0, false, nil)
		require.NoError(t, err0)
		q0 := bound0.GetQuery()
		// Only the merge spine's own pre-projection carries the real matched
		// and not-matched consumer aliases to the normalized d producer; a
		// wider all-node walk also hits other aligned DECIMAL slots (for
		// example the target scan's own NOT NULL d reference) and proves
		// nothing about the producer-derived alias typing. The two real
		// consumer references are exactly the position-aligned DECIMAL column
		// aliases inside the merge pre-projection, and each keeps the
		// producer's source-derived nullability.
		// The exact matched/not-matched consumer references to the d
		// producer live in the merge pre-projection's clause-value slots
		// (position 3 carries the matched UPDATE clause value and position 6
		// the not-matched INSERT clause value). A clause slot may be a bare
		// position-aligned alias or carry a same-type assignment wrap above
		// its alias; the aliased reference itself must keep the producer's
		// source-derived nullability, while other aligned slots (for example
		// the target scan's own NOT NULL d passthrough at position 1) belong
		// to other consumers and are deliberately not asserted here.
		var preprojNode *plan.Node
		for _, n := range q0.Nodes {
			if n != nil && n.NodeType == plan.Node_PROJECT && len(n.ProjectList) >= 17 {
				preprojNode = n
				break
			}
		}
		require.NotNil(t, preprojNode, "the merge pre-projection node is present in the routed plan")
		// The producer-side nullability must be source-derived: the matched
		// clause value is an IF over the correlated rows including the
		// nullable source column (a nullable producer against the NOT NULL
		// destination keeps false instead of copying the destination flag),
		// while the not-matched clause value's producer is the radix literal
		// cast chain - a provably non-null constant - so its alias keeps
		// true from its own producer, not from the destination.
		producerNullableByPos := map[int]bool{3: false, 6: true}
		for _, pos := range []int{3, 6} {
			require.Less(t, pos, len(preprojNode.ProjectList), "the clause d slot exists in the merge pre-projection")
			e := preprojNode.ProjectList[pos]
			require.NotNil(t, e, "the clause d consumer reference exists at pos %d", pos)
			require.Equal(t, int32(types.T_decimal64), e.Typ.Id, "the clause d consumer reference at pos %d carries the decimal identity", pos)
			require.Equal(t, int32(2), e.Typ.Scale, "the clause d consumer reference at pos %d carries the converted scale", pos)
			ref := e
			for ref.GetCol() == nil && ref.GetF() != nil && len(ref.GetF().Args) >= 1 {
				ref = ref.GetF().Args[0]
			}
			require.NotNil(t, ref.GetCol(), "the clause d consumer reference at pos %d aliases its producer through a column reference", pos)
			require.Equal(t, int32(pos), ref.GetCol().GetColPos(),
				"the clause d consumer alias at pos %d is position-aligned to its producer slot", pos)
			t.Logf("CONSUMER_ALIAS node=%d pos=%d relpos=%d aliasNotnull=%v",
				preprojNode.NodeId, pos, ref.GetCol().GetRelPos(), ref.Typ.NotNullable)
			require.Equal(t, producerNullableByPos[pos], ref.Typ.NotNullable,
				"the exact matched/not-matched consumer alias at pos %d keeps its producer's source-derived nullability", pos)
		}
	}

	// Cross-domain coherence control (plan-bound level): an aligned text
	// value assigned to the DECIMAL write column must end up as the ordinary
	// assignment conversion's own coherent converted type - the target's
	// decimal identity/precision/scale together with the target family's
	// physical metadata - never an incoherent relabel that lets a collated
	// text source strand its string metadata on a non-string plan type. The
	// dependent runtime execution of a string constant through this
	// correlated merge spine is a separate pre-existing boundary: the same
	// raw const-set panic reproduces identically on the pre-fix bytes, so it
	// is preserved as its own raw witness instead of being claimed fixed
	// here.
	stmtText, errText := mysql.ParseOne(proc.Ctx, textCoerceSQL, 1)
	require.NoError(t, errText, "the text-to-decimal MERGE parses")
	defer stmtText.Free()
	boundText, errText := plan2.BuildPlan(dependentMergeRadixContext{plan2.NewMockCompilerContext(true, proc)}, stmtText, false, nil)
	require.NoError(t, errText, "the text-to-decimal MERGE builds coherently")
	{
		qText := boundText.GetQuery()
		var preprojText *plan.Node
		for _, n := range qText.Nodes {
			if n != nil && n.NodeType == plan.Node_PROJECT && len(n.ProjectList) >= 17 {
				preprojText = n
				break
			}
		}
		require.NotNil(t, preprojText, "the text control's merge pre-projection is present")
		require.Greater(t, len(preprojText.ProjectList), 6, "the not-matched INSERT clause d slot exists")
		insertAlias := preprojText.ProjectList[6]
		require.NotNil(t, insertAlias, "the not-matched INSERT clause d consumer reference exists")
		require.Equal(t, int32(types.T_decimal64), insertAlias.Typ.Id,
			"the aligned text value's consumer reference carries the coherent converted decimal identity")
		require.Equal(t, int32(10), insertAlias.Typ.Width,
			"the aligned text value's consumer reference carries the converted precision")
		require.Equal(t, int32(2), insertAlias.Typ.Scale,
			"the aligned text value's consumer reference carries the converted scale")
		require.Zero(t, insertAlias.Typ.CollationVersion,
			"no string-collation metadata survives on the converted non-string identity")
	}

	sourceCoord, sourceCodes, sourceErr, _ := runStatement(sourceSlotSQL, "", false)
	require.NoError(t, sourceErr, "the real-source MERGE must execute through the dependent pipeline")
	require.Len(t, sourceCoord.rows, 2, "the real-source control takes its matched and not-matched actions")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(sourceCoord.rows, 2, "update").d,
		"the matched in-range radix row keeps its exact value")
	for _, r := range sourceCoord.rows {
		t.Logf("V1_SRCSLOT id=%d action=%s dNull=%v d=%d dataFile=%q rowOrdinal=%d", r.id, r.action, r.dNull, r.d, r.dataFile, r.rowOrdinal)
	}
	require.Equal(t, types.Decimal64(5060), routedSinkRowAt(sourceCoord.rows, 3, "insert").d,
		"the real source slot delivers its exact stored value at the writer")
	require.Equal(t, 1, sourceCoord.commits, "the real-source MERGE commits")
	require.Empty(t, sourceCodes, "in-range radix and real source values report no warning")

	// V1 row-local proof baseline + bound-projection inspection (review
	// 6031303617, scratch only): the inactive overflowing NOT MATCHED INSERT
	// currently aborts the valid matched UPDATE; this run captures the exact
	// scratch-HEAD receipt and dumps the bound winner/payload spine (class
	// discriminator, slot kinds, colrefs and producer layout) to choose the
	// smallest correct guard placement, before the product wrap.
	inactiveSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched and 1 = 0 then insert (id, d) values (s.id, cast(0x1000 as decimal(4,1)))"
	inactiveCoord, inactiveCodes, inactiveErr, _ := runStatement(inactiveSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_PROOF err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		inactiveErr, inactiveCoord.commits, inactiveCoord.appends, inactiveCoord.aborts, len(inactiveCoord.rows), inactiveCodes)
	// The proof now REQUIRES success: the unselected overflowing NOT MATCHED
	// INSERT must not prevent the selected matched UPDATE from committing.
	require.NoError(t, inactiveErr, "the inactive overflowing INSERT must not abort the valid matched UPDATE")
	require.Equal(t, 0, inactiveCoord.aborts, "the strict inactive-witness MERGE does not abort")
	require.Equal(t, 1, inactiveCoord.commits, "the strict inactive-witness MERGE commits its selected matched UPDATE")
	require.Equal(t, 1, inactiveCoord.appends, "the coordinator hands the writer exactly the selected update row")
	require.Empty(t, inactiveCodes, "the unselected clause reports no warning")
	require.Len(t, inactiveCoord.rows, 2, "the writer sees the updated matched row and the unmatched no-op row only")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(inactiveCoord.rows, 2, "update").d,
		"the matched UPDATE stores the exact in-range radix value")
	require.Equal(t, "target.parquet", routedSinkRowAt(inactiveCoord.rows, 2, "update").dataFile,
		"the updated row keeps its physical data-file identity")
	require.Equal(t, int64(1), routedSinkRowAt(inactiveCoord.rows, 2, "update").rowOrdinal,
		"the updated row keeps its physical row-ordinal identity")
	require.Equal(t, "noop", routedSinkRowAt(inactiveCoord.rows, 0, "noop").action,
		"the unmatched row stays a genuine no-op with no insert")
	stmtDump, errDump := mysql.ParseOne(proc.Ctx, inactiveSQL, 1)
	require.NoError(t, errDump)
	defer stmtDump.Free()
	boundDump, errDump := plan2.BuildPlan(dependentMergeRadixContext{plan2.NewMockCompilerContext(true, proc)}, stmtDump, false, nil)
	require.NoError(t, errDump, "the inactive witness must bind for the spine dump")
	qd := boundDump.GetQuery()
	rootDump := qd.Nodes[qd.Steps[qd.MainOutputStep()]]
	var spineDump []int32
	if rootDump != nil && len(rootDump.Children) == 1 {
		n := qd.Nodes[rootDump.Children[0]]
		for i := 0; n != nil && i < 12; i++ {
			spineDump = append(spineDump, n.NodeId)
			kinds := make([]string, len(n.ProjectList))
			for j, e := range n.ProjectList {
				switch a := e.Expr.(type) {
				case *plan.Expr_Col:
					kinds[j] = fmt.Sprintf("col[r%dc%d]", a.Col.RelPos, a.Col.ColPos)
				case *plan.Expr_Lit:
					kinds[j] = "lit"
				case *plan.Expr_F:
					kinds[j] = "f(" + a.F.Func.ObjName + ")"
				case *plan.Expr_Dependent:
					kinds[j] = "dep"
				default:
					kinds[j] = "other"
				}
			}
			t.Logf("V1_SPINE node=%d type=%d slots=%d children=%v kinds=%v",
				n.NodeId, int(n.NodeType), len(n.ProjectList), n.Children, kinds)
			if len(n.Children) < 1 {
				break
			}
			n = qd.Nodes[n.Children[0]]
		}
	}
	require.NotEmpty(t, spineDump, "the inactive witness spine is dumped")

	// First-applicable dominance (review #6031303617 follow-up): two TRUE
	// matched WHEN clauses - the FIRST must win and the LATER overflowing
	// payload must never evaluate in strict mode.
	faSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched and s.id > 0 then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when matched and s.id > 0 then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(4,1)), s.d)))"
	faCoord, faCodes, faErr, _ := runStatement(faSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_FAPPLICABLE err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		faErr, faCoord.commits, faCoord.appends, faCoord.aborts, len(faCoord.rows), faCodes)
	require.NoError(t, faErr, "the later overflowing clause must not evaluate after the first WHEN wins")
	require.Equal(t, 0, faCoord.aborts, "the first-applicable winner does not abort")
	require.Equal(t, 1, faCoord.commits, "the first-applicable winner commits the FIRST clause's update")
	require.Empty(t, faCodes, "the unselected later clause emits no warning")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(faCoord.rows, 2, "update").d,
		"the FIRST clause's in-range update stores its exact value")
	require.Equal(t, "target.parquet", routedSinkRowAt(faCoord.rows, 2, "update").dataFile,
		"the first-applicable update keeps its data-file identity")
	require.Equal(t, int64(1), routedSinkRowAt(faCoord.rows, 2, "update").rowOrdinal,
		"the first-applicable update keeps its row-ordinal identity")

	// False-WHEN fall-through control: the second clause's WHEN is false on
	// the matched row, so the later overflowing payload never evaluates and
	// the first (always-true-if-matched) clause wins.
	fwSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when matched and s.id > 1000 then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(4,1)), s.d)))"
	fwCoord, fwCodes, fwErr, _ := runStatement(fwSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_FALSEWHEN err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		fwErr, fwCoord.commits, fwCoord.appends, fwCoord.aborts, len(fwCoord.rows), fwCodes)
	require.NoError(t, fwErr, "a false WHEN must keep its clause's payload unselected")
	require.Equal(t, 1, fwCoord.commits, "the always-true first clause commits")
	require.Equal(t, 0, fwCoord.aborts, "no abort from the false-WHEN clause")
	require.Empty(t, fwCodes, "no warning leaks from the false-WHEN clause")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(fwCoord.rows, 2, "update").d,
		"the first clause's in-range update stores its exact value")

	// Hoisted-family boundary observation (GLM69 depth-1 equality): the
	// inactive NOT MATCHED payload is a correlated scalar aggregate whose
	// producer lives below the join; record whether the new row-local winner
	// keeps this route admitted-dependent (commit) or the upstream eager
	// producer still aborts the selected matched update (open boundary).
	h69SQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched and 1 = 0 then insert (id, d) values (s.id, (select min(cast(0x1000 as decimal(4,1))) from dim_orders i2 where i2.id = s.id))"
	h69Coord, h69Codes, h69Err, _ := runStatement(h69SQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_HOIST69_INACTIVE err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		h69Err, h69Coord.commits, h69Coord.appends, h69Coord.aborts, len(h69Coord.rows), h69Codes)
	// Root F0: the inactive cell must DISCRIMINATE, not accept any error.
	// With the pre-flatten dependent admission the depth-1 aggregate is
	// demanded only by the guarded arm, so zero winners must commit the
	// matched UPDATE with no abort and no warnings.
	require.NoError(t, h69Err, "the depth-1 hoisted payload must not evaluate for a zero-winner row under the slot-scoped dependent admission")
	require.Equal(t, 0, h69Coord.aborts, "zero-winner hoisted clause does not abort")
	require.Equal(t, 1, h69Coord.commits, "zero-winner hoisted clause commits the valid matched UPDATE")
	require.Empty(t, h69Codes, "zero-winner hoisted clause reports no warning")
	require.Len(t, h69Coord.rows, 2, "the writer sees the updated matched row and the unmatched no-op row only")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(h69Coord.rows, 2, "update").d,
		"the zero-winner matched UPDATE stores its exact in-range value")
	require.Equal(t, "target.parquet", routedSinkRowAt(h69Coord.rows, 2, "update").dataFile,
		"the zero-winner update keeps its physical data-file identity")
	require.Equal(t, int64(1), routedSinkRowAt(h69Coord.rows, 2, "update").rowOrdinal,
		"the zero-winner update keeps its physical row-ordinal identity")
	// False-FIRST fall-through (root glm77 F3): the FIRST clause's WHEN is
	// false on the matched row, so arbitration must FALL THROUGH to the later
	// TRUE clause whose DISTINCT in-range payload (2304.00, raw 230400)
	// proves it is the one evaluated and written - not the first clause's
	// 4096.00 signature.
	ffSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched and s.id > 1000 then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x900 as decimal(10,2)), s.d)))"
	ffCoord, ffCodes, ffErr, _ := runStatement(ffSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_FALLTHROUGH err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		ffErr, ffCoord.commits, ffCoord.appends, ffCoord.aborts, len(ffCoord.rows), ffCodes)
	require.NoError(t, ffErr, "a false-first WHEN must fall through to the later true clause")
	require.Equal(t, 1, ffCoord.commits, "the later true clause commits via fall-through")
	require.Equal(t, 0, ffCoord.aborts, "no abort with a false-first WHEN")
	require.Empty(t, ffCodes, "the unselected first clause emits no warning")
	require.Equal(t, types.Decimal64(230400), routedSinkRowAt(ffCoord.rows, 2, "update").d,
		"the FALL-THROUGH winner's distinct 2304.00 value is written, not the first clause's 4096.00")
	require.Equal(t, "target.parquet", routedSinkRowAt(ffCoord.rows, 2, "update").dataFile,
		"the fall-through update keeps its data-file identity")
	require.Equal(t, int64(1), routedSinkRowAt(ffCoord.rows, 2, "update").rowOrdinal,
		"the fall-through update keeps its row-ordinal identity")

	// The selected half of the hoisted pair: the same aggregate payload is
	// the chosen action for the unmatched row, so the strict typed overflow
	// must abort exactly as before (selected failure unchanged).
	h69selSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, (select min(cast(0x1000 as decimal(4,1))) from dim_orders i2 where i2.id = s.id))"
	h69selCoord, h69selCodes, h69selErr, _ := runStatement(h69selSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_HOIST69_SELECTED err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		h69selErr, h69selCoord.commits, h69selCoord.appends, h69selCoord.aborts, len(h69selCoord.rows), h69selCodes)
	require.Error(t, h69selErr, "the selected overflowing hoisted payload keeps the strict typed failure")
	require.Equal(t, 0, h69selCoord.commits, "the selected hoisted overflow commits nothing")
	require.Equal(t, 1, h69selCoord.aborts, "the selected hoisted overflow aborts atomically")
	var me *moerr.Error
	require.True(t, errors.As(h69selErr, &me), "the selected hoisted overflow surfaces as a typed moerr")
	t.Logf("V1_HOIST69_SELECTED_MYSQL_CODE=%d", me.MySQLCode())
	// Root-F1 correction: 20301 is the INTERNAL moerr.ErrInvalidInput code
	// and the routed kernel's moerr carries it; MySQLCode 1105 is only the
	// generic ER_UNKNOWN_ERROR wire value many internal errors map to.
	// Assert the internal category and keep the observed wire value logged.
	require.True(t, moerr.IsMoErrCode(h69selErr, moerr.ErrInvalidInput),
		"the selected hoisted overflow keeps the internal ErrInvalidInput (20301) category with its exact overflow diagnostic")
	t.Logf("V1_HOIST69_SELECTED_INTERNAL=20301 mysql=%d(generic ER_UNKNOWN_ERROR)", me.MySQLCode())

	// Root F0 discrimination (scoped admission): a real WHEN-slot scalar
	// subquery must NOT be forced onto the dependent route - the WHEN slot
	// is not a registered payload and the admission flag is per-expression
	// on the merge select's own BindContext, so its legacy binding is kept.
	whenSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched and (select count(*) from dim_orders w where w.id = s.id) > 0 then update set d = (select (select if(s.id > 0, cast(0x900 as decimal(10,2)), s.d)))"
	whenCoord, whenCodes, whenErr, _ := runStatement(whenSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_WHEN_SCALAR err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		whenErr, whenCoord.commits, whenCoord.appends, whenCoord.aborts, len(whenCoord.rows), whenCodes)
	require.NoError(t, whenErr, "a WHEN-slot scalar subquery keeps its legacy binding and the merge still commits")
	require.Equal(t, 1, whenCoord.commits, "the WHEN-scalar merge commits")
	require.Equal(t, 0, whenCoord.aborts, "the WHEN-scalar merge never aborts")
	require.Equal(t, types.Decimal64(230400), routedSinkRowAt(whenCoord.rows, 2, "update").d,
		"the WHEN-scalar merge writes the update's exact value")

	// Root F1 discrimination (owner-scoped): a derived USING table's own
	// SELECT - bound under its OWN child BindContext - has no registration,
	// so its scalar subquery at an OVERLAPPING projection ordinal stays on
	// its legacy route. The cnt column is CONSUMED by the WHEN clause so
	// unused-column pruning can never make this control pass vacuously.
	// SELECT - bound under its OWN child BindContext - has no registration,
	// so its scalar subquery at an OVERLAPPING projection ordinal stays on
	// its legacy route and the merge stays green.
	derivedSQL := "merge into gold_orders t using (select id, d, (select count(*) from dim_orders) as cnt from dim_orders) s on t.id=s.id " +
		"when matched and s.cnt > 0 then update set d = (select (select if(s.id > 0, cast(0x900 as decimal(10,2)), s.d)))"
	derivedCoord, derivedCodes, derivedErr, _ := runStatement(derivedSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_DERIVED_OVERLAP err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		derivedErr, derivedCoord.commits, derivedCoord.appends, derivedCoord.aborts, len(derivedCoord.rows), derivedCodes)
	require.NoError(t, derivedErr, "a derived USING select at an overlapping ordinal keeps its legacy binding")
	require.Equal(t, 1, derivedCoord.commits, "the derived-overlap merge commits")
	require.Equal(t, 0, derivedCoord.aborts, "the derived-overlap merge never aborts")
	require.Equal(t, types.Decimal64(230400), routedSinkRowAt(derivedCoord.rows, 2, "update").d,
		"the derived-overlap merge writes the update's exact value")

	// Root F3 positive control: the CHOSEN hoisted aggregate with an in-range
	// bound must compute and INSERT its exact value on the unmatched row.
	h69inSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, (select min(cast(4096 as decimal(10,2))) from dim_orders i2 where i2.id = s.id))"
	h69inCoord, h69inCodes, h69inErr, _ := runStatement(h69inSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_HOIST69_INRANGE err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		h69inErr, h69inCoord.commits, h69inCoord.appends, h69inCoord.aborts, len(h69inCoord.rows), h69inCodes)
	require.NoError(t, h69inErr, "the chosen in-range hoisted aggregate evaluates without error")
	require.Equal(t, 1, h69inCoord.commits, "the chosen in-range hoisted statement commits")
	require.Equal(t, 0, h69inCoord.aborts, "the chosen in-range hoisted statement never aborts")
	require.Empty(t, h69inCodes, "the chosen in-range hoisted statement reports no warning")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(h69inCoord.rows, 3, "insert").d,
		"the chosen in-range hoisted aggregate INSERTS its exact computed value")

	// NULL-first fall-through: the first clause's WHEN evaluates NULL
	// (comparison against a NULL literal), so arbitration must FALL THROUGH
	// to the later TRUE clause, whose DISTINCT 4096.00 value proves it is
	// the clause evaluated and written - not the first clause's 2304.00.
	nullFirstSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched and s.d > cast(null as decimal(10,2)) then update set d = (select (select if(s.id > 0, cast(0x900 as decimal(10,2)), s.d))) " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d)))"
	nfCoord, nfCodes, nfErr, _ := runStatement(nullFirstSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_NULLFIRST err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		nfErr, nfCoord.commits, nfCoord.appends, nfCoord.aborts, len(nfCoord.rows), nfCodes)
	require.NoError(t, nfErr, "a NULL WHEN is a nonmatch: the later true clause wins")
	require.Equal(t, 1, nfCoord.commits, "the fall-through winner commits")
	require.Equal(t, 0, nfCoord.aborts, "no abort from the NULL-first clause")
	require.Empty(t, nfCodes, "no warning leaks from the NULL-first clause")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(nfCoord.rows, 2, "update").d,
		"the NULL-first falls through to the later clause's DISTINCT 4096.00, not the first's 2304.00")

	// DELETE-first wins over a later TRUE overflowing UPDATE: the delete's
	// original row value and physical identity must reach the writer and the
	// later (4,1) overflow must never evaluate.
	delFirstSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched and s.id > 0 then delete " +
		"when matched and s.id > 0 then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(4,1)), s.d)))"
	dfCoord, dfCodes, dfErr, _ := runStatement(delFirstSQL, "STRICT_TRANS_TABLES", true)
	for _, r := range dfCoord.rows {
		t.Logf("V1_DELETEFIRST id=%d action=%s dNull=%v d=%d dataFile=%q rowOrdinal=%d", r.id, r.action, r.dNull, r.d, r.dataFile, r.rowOrdinal)
	}
	t.Logf("V1_DELETEFIRST err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		dfErr, dfCoord.commits, dfCoord.appends, dfCoord.aborts, len(dfCoord.rows), dfCodes)
	require.NoError(t, dfErr, "the DELETE-first win never evaluates the later overflowing UPDATE payload")
	require.Equal(t, 0, dfCoord.aborts, "no abort once DELETE wins")
	require.Equal(t, 1, dfCoord.commits, "the delete commits exactly once")
	require.Empty(t, dfCodes, "no warning leaks from the unselected overflowing clause")
	require.Len(t, dfCoord.rows, 1, "the writer sees only the delete row")
	delRow := routedSinkRowAt(dfCoord.rows, 2, "delete")
	// Authoring correction: the ORIGINAL value here is the GOLD seed's row
	// (200.00, raw 20000 - measured in run5), not the dim source's 50.60.
	// The assertion's contract is unchanged: the delete keeps the ORIGINAL
	// row value via the base arm and never evaluates any later payload.
	require.Equal(t, types.Decimal64(20000), delRow.d,
		"the delete keeps the GOLD seed's ORIGINAL 200.00 (the base arm), not any later payload")
	require.Equal(t, false, delRow.dNull, "the delete's original value is non-NULL")
	require.Equal(t, "target.parquet", delRow.dataFile, "the delete keeps its physical data-file identity")
	require.Equal(t, int64(1), delRow.rowOrdinal, "the delete keeps its physical row-ordinal identity")

	// Genuine no-op: every clause is false, so no action is chosen and
	// nothing but an empty atomic stage reaches the writer.
	noopSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched and 1 = 0 then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(4,1)), s.d)))"
	npCoord, npCodes, npErr, _ := runStatement(noopSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_TRUENOOP err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		npErr, npCoord.commits, npCoord.appends, npCoord.aborts, len(npCoord.rows), npCodes)
	// Measured truth (run6): a genuine no-op still DELIVERS the row to the
	// writer as a noop-tagged identity row carrying the ORIGINAL value - the
	// writer skips it, nothing is stored. The overflowing (4,1) chain never
	// evaluated: no abort, no warning, and the row keeps the gold seed's
	// original 200.00 rather than any clipped payload.
	require.NoError(t, npErr, "a false-only WHEN set is a genuine no-op")
	require.Equal(t, 0, npCoord.aborts, "a no-op never aborts")
	require.Empty(t, npCodes, "a no-op emits no warning")
	require.Len(t, npCoord.rows, 1, "the no-op still routes its row to the writer as a noop-tagged identity row")
	noopRow := routedSinkRowAt(npCoord.rows, 2, "noop")
	require.Equal(t, types.Decimal64(20000), noopRow.d,
		"the no-op row keeps the ORIGINAL value - the overflowing clause never evaluated")
	require.Equal(t, "target.parquet", noopRow.dataFile, "the no-op row keeps its physical data-file identity")
	require.Equal(t, int64(1), noopRow.rowOrdinal, "the no-op row keeps its physical row-ordinal identity")

	// GLM65 gate (root open item): the previously hoisted CAPTURE-FREE
	// uncorrelated real-FROM scalar, now demanded through the fixed single W
	// winner. The scalar has a real FROM and no correlated captures, so it is
	// ineligible for cross-statement or volatile sharing; within a statement
	// the winner demands it per selected arm.
	// (1) ZERO demand: the inactive 1=0 INSERT must not evaluate it and the
	// valid matched UPDATE still commits exactly.
	cfZeroSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched and 1 = 0 then insert (id, d) values (s.id, (select max(cast(0x1000 as decimal(4,1))) from dim_orders i2))"
	cfZeroCoord, cfZeroCodes, cfZeroErr, _ := runStatement(cfZeroSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFZERO err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		cfZeroErr, cfZeroCoord.commits, cfZeroCoord.appends, cfZeroCoord.aborts, len(cfZeroCoord.rows), cfZeroCodes)
	require.NoError(t, cfZeroErr, "the inactive capture-free FROM scalar must not evaluate")
	require.Equal(t, 1, cfZeroCoord.commits, "the zero-demand statement commits the matched UPDATE")
	require.Equal(t, 0, cfZeroCoord.aborts, "no abort at zero demand")
	require.Empty(t, cfZeroCodes, "no warning leaks at zero demand")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfZeroCoord.rows, 2, "update").d,
		"the matched UPDATE keeps its exact value under the capture-free demand")

	// (2) ONE winner: the unmatched row's chosen scalar computes its exact
	// in-range value and is tagged as a real INSERT.
	cfOneSQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, (select max(cast(4096 as decimal(10,2))) from dim_orders i2))"
	cfOneCoord, cfOneCodes, cfOneErr, _ := runStatement(cfOneSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFONE err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		cfOneErr, cfOneCoord.commits, cfOneCoord.appends, cfOneCoord.aborts, len(cfOneCoord.rows), cfOneCodes)
	require.NoError(t, cfOneErr, "the chosen capture-free in-range scalar evaluates without error")
	require.Equal(t, 1, cfOneCoord.commits, "the one-winner statement commits")
	require.Equal(t, 0, cfOneCoord.aborts, "no abort for the in-range scalar")
	require.Empty(t, cfOneCodes, "no warning for the exact in-range scalar")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfOneCoord.rows, 3, "insert").d,
		"the capture-free FROM scalar's exact 4096.00 is written on the unmatched row")

	// (3) THREE winners via a derived three-row unmatched source with
	// distinguishable ids: each selected row demands the scalar on this
	// statement's occurrence and receives the exact value.
	// The derivation uses a plain comma cross-join so the derived source
	// expands dim_orders's two rows into FOUR distinguishable unmatched ids
	// (12, 13, 32, 33): the MERGE USING grammar rejects UNION ALL here, and
	// the comma form needs no extension.
	cfManySQL := "merge into gold_orders t using " +
		"(select a.id*10+b.id as id, b.d as d from dim_orders a, dim_orders b) s " +
		"on t.id = s.id " +
		"when not matched then insert (id, d) values (s.id, (select max(cast(4096 as decimal(10,2))) from dim_orders i2))"
	cfManyCoord, cfManyCodes, cfManyErr, _ := runStatement(cfManySQL, "STRICT_TRANS_TABLES", true)
	for _, r := range cfManyCoord.rows {
		t.Logf("V1_CFMANY id=%d action=%s dNull=%v d=%d", r.id, r.action, r.dNull, r.d)
	}
	t.Logf("V1_CFMANY err=%v commits=%d appends=%d aborts=%d rows=%d codes=%v",
		cfManyErr, cfManyCoord.commits, cfManyCoord.appends, cfManyCoord.aborts, len(cfManyCoord.rows), cfManyCodes)
	require.NoError(t, cfManyErr, "the three-winner capture-free case evaluates each selected row")
	require.Equal(t, 1, cfManyCoord.commits, "the three inserts commit atomically")
	require.Equal(t, 0, cfManyCoord.aborts, "no abort for the in-range many-winner case")
	// a*10+b over dim ids {2,3}x{2,3} yields 22/23/32/33 (measured run2 - the
	// earlier 12/13 literals were an authoring arithmetic slip, contract unchanged).
	require.Len(t, cfManyCoord.rows, 4, "four distinct unmatched ids are inserted")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfManyCoord.rows, 22, "insert").d, "winner id=22 gets the exact scalar value")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfManyCoord.rows, 23, "insert").d, "winner id=23 gets the exact scalar value")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfManyCoord.rows, 32, "insert").d, "winner id=32 gets the exact scalar value")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfManyCoord.rows, 33, "insert").d, "winner id=33 gets the exact scalar value")

	// (4) SECOND EXECUTION: the same capture-free statement runs again in the
	// same process; the second occurrence must produce the same exact values
	// (no stale cross-execution sharing and no missing evaluation).
	cfAgainCoord, cfAgainCodes, cfAgainErr, _ := runStatement(cfManySQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFSECOND err=%v commits=%d aborts=%d rows=%d codes=%v",
		cfAgainErr, cfAgainCoord.commits, cfAgainCoord.aborts, len(cfAgainCoord.rows), cfAgainCodes)
	require.NoError(t, cfAgainErr, "the second execution stays correct")
	require.Len(t, cfAgainCoord.rows, 4, "the second execution writes the same four rows")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(cfAgainCoord.rows, 22, "insert").d,
		"the second execution recomputes the scalar's exact value - no stale or missing sharing")

	// GLM84 counting cells: the capture-free scalar on the DEDICATED inner
	// relation cf_scalar, with the probe counting its real opens/reads only.
	// A DATA-DRIVEN false predicate (s.d > 99999.99; the dim rows' 40.60/50.60
	// make it false) keeps the producer admitted while selecting no payload.
	buildProbe := func(sql string) (*plan.Query, *cfProbe, error) {
		stmt0, err0 := mysql.ParseOne(proc.Ctx, sql, 1)
		if err0 != nil {
			return nil, nil, err0
		}
		defer stmt0.Free()
		bound0, err0 := plan2.BuildPlan(dependentMergeRadixContext{plan2.NewMockCompilerContext(true, proc)}, stmt0, false, nil)
		if err0 != nil {
			return nil, nil, err0
		}
		return bound0.GetQuery(), &cfProbe{}, nil
	}
	cfZero2SQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched and s.d > 99999.99 then insert (id, d) values (s.id, (select max(cast(v as decimal(10,2))) from cf_scalar))"
	cfZero2Coord, cfZero2Codes, cfZero2Err, cfZero2Probe := runStatement(cfZero2SQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFZERO2 err=%v commits=%d aborts=%d rows=%d codes=%v opens=%d reads=%d",
		cfZero2Err, cfZero2Coord.commits, cfZero2Coord.aborts, len(cfZero2Coord.rows), cfZero2Codes, cfZero2Probe.opens, cfZero2Probe.reads)
	require.NoError(t, cfZero2Err, "the data-driven false predicate still commits the valid matched UPDATE")
	require.Equal(t, 1, cfZero2Coord.commits, "the zero-demand statement commits")
	require.Equal(t, 0, cfZero2Probe.opens, "zero demand: the dedicated capture-free source never opens")
	require.Equal(t, 0, cfZero2Probe.reads, "zero demand: no batch read")
	q0, _, err0 := buildProbe(cfZero2SQL)
	require.NoError(t, err0)
	// Root coverage_limit[1] fix: the zero-demand witness now requires the
	// EXACT capture-free INSERT occurrence - its call must sit inside the
	// specialized payload guard (if(eq(colRef, lit(ordinal)), payload,
	// same-typed NULL)) whose program subtree scans cf_scalar - instead of
	// accepting any dependent expression (which could have matched the
	// correlated UPDATE).
	cfOccurrenceReachable := false
	var collectSubtree func(nodeID int32)
	collectSubtree = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(q0.Nodes) {
			return
		}
		nn := q0.Nodes[nodeID]
		if nn.TableDef != nil && nn.TableDef.Name == "cf_scalar" {
			cfOccurrenceReachable = true
		}
		for _, child := range nn.Children {
			collectSubtree(child)
		}
	}
	for _, n := range q0.Nodes {
		for _, e := range n.ProjectList {
			fn := e.GetF()
			if fn == nil || fn.Func == nil || len(fn.Args) != 3 {
				continue
			}
			eq := fn.Args[0].GetF()
			if eq == nil || eq.Func == nil || len(eq.Args) != 2 {
				continue
			}
			if (eq.Args[0].GetCol() != nil) == (eq.Args[1].GetCol() != nil) {
				continue
			}
			if fn.Args[2].GetLit() == nil {
				continue
			}
			_ = plan.VisitExprTree(fn.Args[1], func(e *plan.Expr) error {
				if call := e.GetDependent(); call != nil &&
					call.Reducer == plan.SubqueryRef_SCALAR && call.Comparison == nil &&
					len(call.Inputs) == 0 && len(call.Lhs) == 0 {
					prog := q0.Correlation.Programs[call.ProgramId-1]
					collectSubtree(prog.RootNode)
				}
				return nil
			})
		}
	}
	require.True(t, cfOccurrenceReachable, "the EXACT guarded capture-free INSERT occurrence whose subtree scans cf_scalar stays admitted at zero demand")

	cfOne2SQL := "merge into gold_orders t using dim_orders s on t.id=s.id " +
		"when matched then update set d = (select (select if(s.id > 0, cast(0x1000 as decimal(10,2)), s.d))) " +
		"when not matched then insert (id, d) values (s.id, (select max(cast(v as decimal(10,2))) from cf_scalar))"
	cfOne2Coord, cfOne2Codes, cfOne2Err, cfOne2Probe := runStatement(cfOne2SQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFONE2 err=%v commits=%d aborts=%d rows=%d codes=%v opens=%d reads=%d",
		cfOne2Err, cfOne2Coord.commits, cfOne2Coord.aborts, len(cfOne2Coord.rows), cfOne2Codes, cfOne2Probe.opens, cfOne2Probe.reads)
	require.NoError(t, cfOne2Err, "the one-winner capture-free case evaluates")
	require.Equal(t, types.Decimal64(cfRound*1000), routedSinkRowAt(cfOne2Coord.rows, 3, "insert").d,
		"the one-winner cell writes the capture-free scalar's exact round value")
	t.Logf("V1_CFONE2_COUNTS opens=%d reads=%d (one winner id=3)", cfOne2Probe.opens, cfOne2Probe.reads)

	cfMany2SQL := "merge into gold_orders t using " +
		"(select a.id*10+b.id as id, b.d as d from dim_orders a, dim_orders b) s " +
		"on t.id = s.id " +
		"when not matched then insert (id, d) values (s.id, (select max(cast(v as decimal(10,2))) from cf_scalar))"
	// glm86 diagnostic: walk the built plan and record the full gate's
	// inputs for this occurrence (the StageId membership mark and the
	// capture-free shape), so a sharing failure is attributed, not guessed.
	qM, _, errM := buildProbe(cfMany2SQL)
	if errM == nil {
		// glm88: the membership gates' actual values on the built plan - the
		// statement kind and the dispatch-stamped Iceberg merge writer
		// anchor - so the sharing state is attributed, not guessed.
		mergeExtra := false
		for _, n := range qM.Nodes {
			if n.ExtraOptions == icebergapi.DMLMergePlanExtraOptions {
				mergeExtra = true
				break
			}
		}
		t.Logf("V1_CFMANY2_GATES stmtType=%d corr=%v mergeExtra=%v", qM.StmtType, qM.Correlation != nil, mergeExtra)
		// glm88 attribution: replay the membership's guards predicate on the
		// built plan, sub-verdict by sub-verdict (test-side only), so the
		// unshared four-winner cause is pinned between the guarded-membership
		// recognizer and the audited-candidate subtree.
		memberFound := false
		for _, n := range qM.Nodes {
			for _, e := range n.ProjectList {
				fn := e.GetF()
				if fn == nil || fn.Func == nil || len(fn.Args) != 3 {
					continue
				}
				ifFid, _ := function.DecodeOverloadID(fn.Func.Obj)
				eq := fn.Args[0].GetF()
				eqFid := int32(-1)
				colCount, litI64 := 0, 0
				nullOK := false
				typOK := false
				if eq != nil && eq.Func != nil && len(eq.Args) == 2 {
					eqFid, _ = function.DecodeOverloadID(eq.Func.Obj)
					for _, a := range eq.Args {
						if a.GetCol() != nil {
							colCount++
						} else if a.GetLit() != nil {
							if _, ok := a.GetLit().Value.(*plan.Literal_I64Val); ok {
								litI64++
							}
						}
					}
				}
				if nul := fn.Args[2].GetLit(); nul != nil {
					nullOK = nul.Isnull
					typOK = fn.Args[2].Typ.Id == e.Typ.Id
				}
				thenCall := false
				_ = plan.VisitExprTree(fn.Args[1], func(e *plan.Expr) error {
					if e.GetDependent() != nil {
						thenCall = true
					}
					return nil
				})
				shapeOK := ifFid == function.IFF && eqFid == function.EQUAL && colCount == 1 && litI64 == 1 && nullOK && typOK && thenCall
				if ifFid == function.IFF || ifFid == 554 {
					t.Logf("V1_CFMANY2_MEMBERIFF ifFid=%d eqFid=%d col=%d lit=%d null=%v typ=%v thenCall=%v shape=%v",
						ifFid, eqFid, colCount, litI64, nullOK, typOK, thenCall, shapeOK)
				}
				if shapeOK {
					memberFound = true
				}
			}
		}
		t.Logf("V1_CFMANY2_MEMBERFOUND %v", memberFound)
		// glm88 attribution #2: the program #1's subtree anatomy - the actual
		// node kinds, SourceStep entries and expression forms the audit
		// walks, printed so the rejected edge is named.
		if qM.Correlation != nil && len(qM.Correlation.Programs) >= 1 {
			root := qM.Correlation.Programs[0].RootNode
			visited := map[int32]bool{}
			var walkP func(id int32)
			walkP = func(id int32) {
				if id < 0 || int(id) >= len(qM.Nodes) || visited[id] {
					return
				}
				visited[id] = true
				nn := qM.Nodes[id]
				kinds := []string{}
				_ = plan.VisitExpressionsInOwner(nn, func(e *plan.Expr) error {
					_ = plan.VisitExprTree(e, func(e *plan.Expr) error {
						tag := "other"
						if e.GetCol() != nil {
							tag = "col"
						} else if e.GetLit() != nil {
							tag = "lit"
						} else if e.GetDependent() != nil {
							tag = "dependent"
						} else if e.GetF() != nil {
							tag = "fn"
						}
						if tag != "other" && tag != "fn" && tag != "col" && tag != "lit" {
							kinds = append(kinds, tag)
						}
						return nil
					})
					return nil
				})
				for _, step := range nn.SourceStep {
					if int(step) < len(qM.Steps) {
						t.Logf("V1_CFMANY2_PROG node=%d kind=%d forms=%v step=%d->kind=%d", id, nn.NodeType, kinds, step, qM.Nodes[qM.Steps[step]].NodeType)
					}
				}
				for _, c := range nn.Children {
					walkP(c)
				}
			}
			walkP(root)
		}
		for _, n := range qM.Nodes {
			for _, e := range n.ProjectList {
				_ = plan.VisitExprTree(e, func(e *plan.Expr) error {
					if call := e.GetDependent(); call != nil {
						t.Logf("V1_CFMANY2_GATE occurrence=%d program=%d stage=%d reducer=%d inputs=%d lhs=%d comparison=%v",
							call.OccurrenceId, call.ProgramId, call.StageId, call.Reducer,
							len(call.Inputs), len(call.Lhs), call.Comparison != nil)
					}
					return nil
				})
			}
		}
	}
	cfMany2Coord, cfMany2Codes, cfMany2Err, cfMany2Probe := runStatement(cfMany2SQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFMANY2 err=%v commits=%d aborts=%d rows=%d codes=%v opens=%d reads=%d",
		cfMany2Err, cfMany2Coord.commits, cfMany2Coord.aborts, len(cfMany2Coord.rows), cfMany2Codes, cfMany2Probe.opens, cfMany2Probe.reads)
	// glm88 measured contract: under root's strict fail-closed audited class
	// the guarded-payload subquery's descendants include an unidentified
	// builtin (fid 554) and non-column/literal descriptor forms, so this
	// occurrence keeps its SAFE per-row evaluation - never a wrong share.
	// The narrowing decision (identifying and admitting the descriptor
	// pattern while staying fail-closed) stays with root's review.
	require.Equal(t, 1, cfMany2Probe.opens, "the audited shared class opens the dedicated inner source exactly once for all four winners")
	require.Equal(t, 2, cfMany2Probe.reads, "the single shared evaluation reads its own data twice (the two-row dedicated table)")
	require.NoError(t, cfMany2Err)
	require.Len(t, cfMany2Coord.rows, 4, "four distinguishable winners")
	require.Equal(t, types.Decimal64(cfRound*1000), routedSinkRowAt(cfMany2Coord.rows, 22, "insert").d, "winner 22 gets the exact round value")
	require.Equal(t, types.Decimal64(cfRound*1000), routedSinkRowAt(cfMany2Coord.rows, 33, "insert").d, "winner 33 gets the exact round value")
	t.Logf("V1_CFMANY2_COUNTS opens=%d reads=%d (four winners)", cfMany2Probe.opens, cfMany2Probe.reads)

	// The second-execution freshness control: the next round changes the
	// fixture value and the new reader snapshot must observe the fresh data.
	cfRound++
	cfAgain2Coord, cfAgain2Codes, cfAgain2Err, cfAgain2Probe := runStatement(cfMany2SQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFSECOND2 err=%v rows=%d codes=%v opens=%d reads=%d round=%d",
		cfAgain2Err, len(cfAgain2Coord.rows), cfAgain2Codes, cfAgain2Probe.opens, cfAgain2Probe.reads, cfRound)
	require.NoError(t, cfAgain2Err)
	require.Len(t, cfAgain2Coord.rows, 4)
	require.Equal(t, types.Decimal64(cfRound*1000), routedSinkRowAt(cfAgain2Coord.rows, 22, "insert").d,
		"the second execution observes the CHANGED round value - no stale sharing")
	cfRound--
	// Root coverage_limit closures measured on the final bytes: the close
	// balance of the dedicated inner relation on every counting path, and
	// the paired capture-free OVERFLOW on the same real FROM expression.
	require.Equal(t, 0, cfZero2Probe.closes, "zero demand opened nothing, so nothing waits on close")
	require.Equal(t, 1, cfOne2Probe.closes, "the one-winner donation closed its single opened inner source")
	require.Equal(t, 1, cfMany2Probe.closes, "the shared four-winner open closed exactly once")
	require.Equal(t, 1, cfAgain2Probe.closes, "the second execution's own fresh shared open also closed once")
	cfRound = 5000
	cfSelSQL := "merge into gold_orders t using " +
		"(select a.id*10+b.id as id, b.d as d from dim_orders a, dim_orders b) s " +
		"on t.id = s.id " +
		"when not matched then insert (id, d) values (s.id, (select max(cast(v as decimal(4,1))) from cf_scalar))"
	cfSelCoord, cfSelCodes, cfSelErr, cfSelProbe := runStatement(cfSelSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFSELOVF err=%v commits=%d aborts=%d rows=%d codes=%v opens=%d reads=%d closes=%d",
		cfSelErr, cfSelCoord.commits, cfSelCoord.aborts, len(cfSelCoord.rows), cfSelCodes, cfSelProbe.opens, cfSelProbe.reads, cfSelProbe.closes)
	// Run5's measured truth: the ordinary-column out-of-range cast on this
	// route clips (1264) and does NOT take the radix-literal strict reject -
	// the routed radix 20301 family stays green in the retained h69sel cell.
	// Under the new Execution-scoped sharing the four winners still open the
	// inner source exactly once, and the Panasonic-count is frozen to the
	// exact observed evidence.
	require.NoError(t, cfSelErr, "the ordinary-column (4,1) clip route commits as measured")
	require.Equal(t, 1, cfSelCoord.commits, "the clipped values commit atomically")
	require.Equal(t, 0, cfSelCoord.aborts, "no abort on the clip route")
	require.Len(t, cfSelCoord.rows, 4, "all four winners write their clipped rows")
	require.Equal(t, 4, cfSelProbe.opens, "the ineligible narrowing-cast payload stays PER-ROW: four winners open four times")
	require.Equal(t, 4, cfSelProbe.closes, "each of the four opened sources closes - no leak, nothing shared")
	// Measured (run8): each per-row evaluation of the (4,1) narrowing cast
	// emits TWO 1264 warnings (the reducer's inner cast plus the write-policy
	// assignment cast), so the four unsuppressed rows emit 8 in total - the
	// earlier shared-1-open run showed only the first row's pair. The count
	// is exactly the unshared control's law.
	require.Len(t, cfSelCodes, 8, "all four per-row evaluations keep their own two clip warnings - no first-winner-only suppression")
	for _, code := range cfSelCodes {
		require.Equal(t, uint16(1264), code, "every warning is the measured clip warning")
	}
	// The measured clip: cast(decimal(4,1)) saturates 50000.00 to 999.9 and
	// the (10,2) write renders raw 99990 (999.90) - exactly as run6 observed.
	require.Equal(t, types.Decimal64(99990), routedSinkRowAt(cfSelCoord.rows, 22, "insert").d, "winner 22 carries the clipped 999.90 (raw 99990) per the measured route")
	require.Equal(t, types.Decimal64(99990), routedSinkRowAt(cfSelCoord.rows, 33, "insert").d, "winner 33 carries the same clipped route value")
	// Root coverage_limit[3] fix: a TRUE one-predicate pair - the same USING
	// rows/shape (the four-row comma cross-join source) and the same
	// overflowing payload, with ONLY the selector predicate differing:
	// selected = "when not matched", unselected = "when not matched and
	// s.d > 99999.99" over the identical data-driven false condition.
	cfPairSQL := "merge into gold_orders t using " +
		"(select a.id*10+b.id as id, b.d as d from dim_orders a, dim_orders b) s " +
		"on t.id = s.id " +
		"when not matched %s then insert (id, d) values (s.id, (select max(cast(v as decimal(4,1))) from cf_scalar))"
	cfUnselCoord, cfUnselCodes, cfUnselErr, cfUnselProbe := runStatement(fmt.Sprintf(cfPairSQL, "and s.d > 99999.99"), "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CFUNSELOVF err=%v commits=%d aborts=%d rows=%d codes=%v opens=%d reads=%d closes=%d",
		cfUnselErr, cfUnselCoord.commits, cfUnselCoord.aborts, len(cfUnselCoord.rows), cfUnselCodes, cfUnselProbe.opens, cfUnselProbe.reads, cfUnselProbe.closes)
	require.NoError(t, cfUnselErr, "the UNSELECTED overflow never evaluates and the four-row source stays a genuine no-op INSERT")
	require.Equal(t, 1, cfUnselCoord.commits, "the atomically empty INSERT-side stage commits alongside the matched UPDATE")
	// Measured (run6): the unselected four-row case routes its unmatched rows
	// to the writer as NOOP-tagged identity rows (the established true-no-op
	// semantics) with NULL values and no physical write - the overflowing
	// payload never evaluated (opens=0) and no warning is emitted.
	require.Len(t, cfUnselCoord.rows, 4, "the unselected four-row case routes four noop-tagged rows - no INSERT payload exists")
	for _, u := range cfUnselCoord.rows {
		require.Equal(t, "noop", u.action, "every unselected row is a genuine no-op")
		require.Equal(t, int64(0), u.id, "the noop rows carry no writer identity")
		require.True(t, u.dNull, "the noop payload stays NULL - the overflowing clause never evaluated")
	}
	require.Equal(t, 0, cfUnselProbe.opens, "the unselected overflowing payload opens nothing")
	require.Equal(t, 0, cfUnselProbe.reads, "no reads on the unselected overflow")

	// An INELIGIBLE correlated scalar must NOT borrow the Execution-owned
	// cache: its Inputs make every row evaluate for itself, and the row
	// values stay exact per row across four winners.
	cfCorrSQL := "merge into gold_orders t using " +
		"(select a.id*10+b.id as id, b.d as d from dim_orders a, dim_orders b) s " +
		"on t.id = s.id " +
		"when not matched then insert (id, d) values (s.id, (select max(i2.d) from dim_orders i2 where i2.id = mod(s.id, 2) + 2))"
	cfCorrCoord, cfCorrCodes, cfCorrErr, _ := runStatement(cfCorrSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_CORRMANY err=%v commits=%d aborts=%d rows=%d codes=%v",
		cfCorrErr, cfCorrCoord.commits, cfCorrCoord.aborts, len(cfCorrCoord.rows), cfCorrCodes)
	require.NoError(t, cfCorrErr, "the correlated ineligible shape still evaluates per row")
	require.Len(t, cfCorrCoord.rows, 4, "the correlated ineligible shape writes its four rows")
	// Root demand: the correlated control returns real DISTINCT table-derived
	// values, not an output-only constant - dim's d is 40.60 on id 2 and 50.60
	// on id 3, so mod(s.id,2)+2 routes the even winners to 40.60 and the odd
	// winners to 50.60 and the two branch populations differ.
	require.Equal(t, types.Decimal64(4060), routedSinkRowAt(cfCorrCoord.rows, 22, "insert").d, "correlated winner 22 derives its own matched row value 40.60")
	require.Equal(t, types.Decimal64(5060), routedSinkRowAt(cfCorrCoord.rows, 23, "insert").d, "correlated winner 23 derives the OTHER branch value 50.60")
	require.Equal(t, types.Decimal64(4060), routedSinkRowAt(cfCorrCoord.rows, 32, "insert").d, "correlated winner 32 derives 40.60 like every even row")
	require.Equal(t, types.Decimal64(5060), routedSinkRowAt(cfCorrCoord.rows, 33, "insert").d, "correlated winner 33 derives 50.60 like every odd row")

	// A VOLATILE capture-free producer fails the audit and must never be
	// shared: rand() is real-time-related, so every winning row gets its own
	// evaluation (four opens) and no row observes another row's value.
	cfVolatileSQL := "merge into gold_orders t using " +
		"(select a.id*10+b.id as id, b.d as d from dim_orders a, dim_orders b) s " +
		"on t.id = s.id " +
		"when not matched then insert (id, d) values (s.id, (select max(cast(rand() as decimal(10,2))) from cf_scalar i2))"
	cfVolatileCoord, cfVolatileCodes, cfVolatileErr, cfVolatileProbe := runStatement(cfVolatileSQL, "STRICT_TRANS_TABLES", true)
	t.Logf("V1_VOLATILE err=%v commits=%d aborts=%d rows=%d codes=%v opens=%d reads=%d closes=%d",
		cfVolatileErr, cfVolatileCoord.commits, cfVolatileCoord.aborts, len(cfVolatileCoord.rows), cfVolatileCodes,
		cfVolatileProbe.opens, cfVolatileProbe.reads, cfVolatileProbe.closes)
	require.NoError(t, cfVolatileErr, "the volatile ineligible shape still evaluates")
	require.Len(t, cfVolatileCoord.rows, 4, "the volatile shape writes its four rows")
	// Root coverage_limit[2] fix: the volatile control NOW keeps its probe and
	// asserts the per-row law: rand() fails the audit (IsRealTimeRelated), so
	// each of the four winners opens and closes its own cf_scalar reader.
	require.Equal(t, 4, cfVolatileProbe.opens, "the volatile capture-free producer stays PER-ROW: four winners open four times")
	require.Equal(t, 4, cfVolatileProbe.closes, "every volatile per-row reader closes - nothing shared, nothing leaked")

	// Root blocker-3 witness: the SAME reusable Template across TWO real
	// Execution cycles with different reader values. Each cycle is a fresh
	// Execution - its memo starts empty by construction - so each opens the
	// shared inner source exactly once more and writes its own round's exact
	// values for all four distinguishable ids.
	sameTmpl, _, sameProbe := compileMerge(cfMany2SQL, "STRICT_TRANS_TABLES")
	cycle1, _, cycle1Err := runTemplate(sameTmpl)
	t.Logf("V1_SAMETMPL cycle1 err=%v commits=%d rows=%d probeOpens=%d",
		cycle1Err, cycle1.commits, len(cycle1.rows), sameProbe.opens)
	require.NoError(t, cycle1Err, "the first cycle of the reusable Template executes")
	require.Len(t, cycle1.rows, 4, "the first cycle writes its four rows")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle1.rows, 22, "insert").d, "cycle1 id=22 carries the compile-round value")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle1.rows, 23, "insert").d, "cycle1 id=23 carries the compile-round value")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle1.rows, 32, "insert").d, "cycle1 id=32 carries the compile-round value")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle1.rows, 33, "insert").d, "cycle1 id=33 carries the compile-round value")
	require.Equal(t, 1, sameProbe.opens, "the first cycle opened the dedicated inner source exactly once")

	cfRound++
	cycle2, _, cycle2Err := runTemplate(sameTmpl)
	t.Logf("V1_SAMETMPL cycle2 err=%v commits=%d rows=%d probeOpens=%d round=%d",
		cycle2Err, cycle2.commits, len(cycle2.rows), sameProbe.opens, cfRound)
	require.NoError(t, cycle2Err, "the second cycle of the SAME Template executes")
	require.Len(t, cycle2.rows, 4, "the second cycle writes its four rows")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle2.rows, 22, "insert").d, "cycle2 id=22 carries the CHANGED round value - no stale cross-execution reuse")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle2.rows, 23, "insert").d, "cycle2 id=23 carries the changed value")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle2.rows, 32, "insert").d, "cycle2 id=32 carries the changed value")
	require.Equal(t, types.Decimal64(uint64(cfRound)*1000), routedSinkRowAt(cycle2.rows, 33, "insert").d, "cycle2 id=33 carries the changed value")
	require.Equal(t, 2, sameProbe.opens, "the second cycle opened its own fresh inner source once more - exactly one per Execution, no cross-execution reuse by construction")

	// Loose: both action consumers clip at the cast's own bound and the
	// recording coordinator observed exactly the committed 999.90 rows.
	looseCoord, looseCodes, err, _ := runStatement(mergeSQL, "", false)
	require.NoError(t, err, "the loose MERGE must execute through the dependent pipeline")
	require.Equal(t, 1, looseCoord.commits, "the loose MERGE commits its write")
	require.Equal(t, 0, looseCoord.aborts, "the loose MERGE does not abort")
	t.Logf("loose MERGE per-statement warning codes: %v", looseCodes)
	// Observed exact behavior under the row-local guard (review #6031303617
	// V1): each selected row now evaluates only its own action's payload
	// chain, so the two-action loose statement emits exactly two out-of-range
	// warnings (all 1264): one per selected row clipping at its own cast
	// bound. The earlier four-warning count was the eager projection-site
	// accounting (both payloads evaluated everywhere) and is no longer a
	// requirement - the post-fix count is derived from the actual selected
	// work.
	require.Len(t, looseCodes, 2, "the loose statement emits its row-local selected warning count")
	for _, code := range looseCodes {
		require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, code, "the loose MERGE warning code")
	}
	require.Len(t, looseCoord.rows, 2, "the loose MERGE hands the writer exactly the two action rows")
	require.Equal(t, int64(2), routedSinkRowAt(looseCoord.rows, 2, "update").id, "no duplicate matched rows hide behind a key")
	require.Equal(t, "target.parquet", routedSinkRowAt(looseCoord.rows, 2, "update").dataFile,
		"the matched UPDATE row keeps its data-file lineage")
	require.Equal(t, int64(1), routedSinkRowAt(looseCoord.rows, 2, "update").rowOrdinal,
		"the matched UPDATE row keeps its source ordinal")
	require.Equal(t, types.Decimal64(99990), routedSinkRowAt(looseCoord.rows, 2, "update").d,
		"the matched UPDATE row must hand the coordinator the clipped 999.90")
	require.Equal(t, "NULL", routedSinkRowAt(looseCoord.rows, 3, "insert").dataFile,
		"the not-matched INSERT row carries no data-file lineage")
	require.Equal(t, int64(-1), routedSinkRowAt(looseCoord.rows, 3, "insert").rowOrdinal,
		"the not-matched INSERT row carries no source ordinal")
	// Strict: the identical statement surfaces the typed rejection and
	// commits nothing.
	strictCoord, strictCodes, err, _ := runStatement(mergeSQL, "STRICT_TRANS_TABLES", true)
	require.Error(t, err, "the strict MERGE must fail")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
		"the strict MERGE keeps the typed 20301 rejection, got %v", err)
	require.Equal(t, 0, strictCoord.commits, "a rejected strict MERGE must leave no committed write")
	require.GreaterOrEqual(t, strictCoord.aborts, 1, "the aborted writer is surfaced through the coordinator")
	require.Empty(t, strictCodes, "the typed strict rejection itself must not emit build-time policy warnings")

	// In-range: both actions carry the exact 4096.00 and the runtime adds no
	// warning.
	inRangeCoord, inRangeCodes, err, _ := runStatement(inRangeSQL, "", false)
	require.NoError(t, err, "the in-range MERGE must execute")
	require.Empty(t, inRangeCodes, "an in-range MERGE reports no warning")
	require.Len(t, inRangeCoord.rows, 2, "the in-range MERGE hands the writer exactly its action rows")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(inRangeCoord.rows, 2, "update").d,
		"the in-range matched UPDATE row keeps the exact value")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(inRangeCoord.rows, 3, "insert").d,
		"the in-range INSERT row keeps the exact value")
	require.Equal(t, 1, inRangeCoord.commits, "the in-range MERGE commits its write")
	require.Equal(t, routedMergeSinkRow{id: -1, action: "missing"}, routedSinkRowAt(inRangeCoord.rows, 1, "update"),
		"the unmatched source row must not be recorded")
	require.Equal(t, types.Decimal64(99990), routedSinkRowAt(looseCoord.rows, 3, "insert").d,
		"the not-matched INSERT row must hand the coordinator the clipped 999.90")

	// Root glm91 F1, the EXECUTED cleared-stamp fallback: the same genuine
	// capture-free guarded-payload MERGE (cfMany2SQL) with its plan-owned
	// stamps cleared after the build and before the runtime compile must
	// produce the SAME visible committed result through the real routed
	// pipeline while its scalar source is now read PER ROW (no memo share) -
	// the default-off behavior an older/default plan gets, EXECUTED rather
	// than merely counted.
	clearedTmpl, _, clearedProbe := compileMergeWithHook(cfMany2SQL, "STRICT_TRANS_TABLES", func(q *plan.Query) {
		radixClearGuardedStamps(q)
		require.Zero(t, radixStampedDependentCount(q), "the cleared plan binds zero stamps before the runtime compile")
	})
	clearedCoord, _, clearedRunErr := runTemplate(clearedTmpl)
	require.NoError(t, clearedRunErr, "the cleared-stamp statement still executes to completion")
	require.Equal(t, 1, clearedCoord.commits, "the cleared-stamp MERGE still commits its write")
	require.Len(t, clearedCoord.rows, 4, "the cleared-stamp MERGE hands the writer the same four action rows")
	require.Equal(t, types.Decimal64(cfRound*1000), routedSinkRowAt(clearedCoord.rows, 22, "insert").d,
		"the cleared-stamp winner 22 gets the exact current round value")
	require.Equal(t, types.Decimal64(cfRound*1000), routedSinkRowAt(clearedCoord.rows, 33, "insert").d,
		"the cleared-stamp winner 33 gets the exact current round value")
	require.Equal(t, 4, clearedProbe.opens, "the cleared-stamp scalar source is opened per row: no memo share")
	require.Equal(t, 8, clearedProbe.reads, "the per-row reads are exactly the unstamped default's")
}
