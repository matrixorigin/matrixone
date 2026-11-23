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
	if name != "gold_orders" && name != "dim_orders" {
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
			d: vector.GetFixedAtWithTypeCheck[types.Decimal64](dVec, row),
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

// TestDependentIcebergMergeRadixWritePolicy drives the real specialized MERGE
// plan, the dependent compile pipeline and the recording iceberg write
// coordinator over the exact matched-UPDATE and not-matched-INSERT action
// consumers targeting the writable DECIMAL column d:
//   - loose sessions clip the radix literal at the cast's own bound, the sink
//     receives 999.90 and the runtime reports out-of-range warnings;
//   - strict sessions surface the typed 20301 rejection and commit nothing;
//   - in-range radix literals preserve their exact value with no warning.
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

	runStatement := func(sql, mode string, strict bool) (*radixMergeCoordinator, []uint16, error) {
		// The statement's SQL mode is recorded before BuildPlan so the
		// binding and the runtime share one discriminating session, and each
		// statement gets its own warning sink: counts stay bounded to the
		// expressions this statement actually evaluates.
		proc.SetResolveVariableFunc(func(_ string, _, _ bool) (interface{}, error) { return mode, nil })
		statementSink := &radixAssignmentWarningSink{}
		proc.WarningSink = statementSink
		q, c := build(sql)
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
				for i, col := range node.TableDef.Cols {
					typ, err := types.TypeFromPlan(col.Typ)
					if err != nil {
						return nil, err
					}
					b.Vecs[i] = vector.NewVec(typ, dependentFixtureMP(child, allocation))
					for row := 0; row < 2; row++ {
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
				return &dependentFixtureSource{batch: b, rows: 1, close: func() { b.Clean(dependentFixtureMP(child, allocation)) }}, nil
			}, nil
		}, c.compileDependentKernel)
		require.NoError(t, err)
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

	sourceCoord, sourceCodes, sourceErr := runStatement(sourceSlotSQL, "", false)
	require.NoError(t, sourceErr, "the real-source MERGE must execute through the dependent pipeline")
	require.Len(t, sourceCoord.rows, 2, "the real-source control takes its matched and not-matched actions")
	require.Equal(t, types.Decimal64(409600), routedSinkRowAt(sourceCoord.rows, 2, "update").d,
		"the matched in-range radix row keeps its exact value")
	require.Equal(t, types.Decimal64(5060), routedSinkRowAt(sourceCoord.rows, 3, "insert").d,
		"the real source slot delivers its exact stored value at the writer")
	require.Equal(t, 1, sourceCoord.commits, "the real-source MERGE commits")
	require.Empty(t, sourceCodes, "in-range radix and real source values report no warning")

	// Loose: both action consumers clip at the cast's own bound and the
	// recording coordinator observed exactly the committed 999.90 rows.
	looseCoord, looseCodes, err := runStatement(mergeSQL, "", false)
	require.NoError(t, err, "the loose MERGE must execute through the dependent pipeline")
	require.Equal(t, 1, looseCoord.commits, "the loose MERGE commits its write")
	require.Equal(t, 0, looseCoord.aborts, "the loose MERGE does not abort")
	t.Logf("loose MERGE per-statement warning codes: %v", looseCodes)
	// Observed exact behavior: the dependent pipeline evaluates the radix
	// write chain at each of its projection sites for the two action rows, so
	// this statement emits exactly four out-of-range warnings (all 1264)
	// under per-statement scoping with no other warning codes.
	require.Len(t, looseCodes, 4, "the loose statement emits its exact observed warning count")
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
	strictCoord, strictCodes, err := runStatement(mergeSQL, "STRICT_TRANS_TABLES", true)
	require.Error(t, err, "the strict MERGE must fail")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
		"the strict MERGE keeps the typed 20301 rejection, got %v", err)
	require.Equal(t, 0, strictCoord.commits, "a rejected strict MERGE must leave no committed write")
	require.GreaterOrEqual(t, strictCoord.aborts, 1, "the aborted writer is surfaced through the coordinator")
	require.Empty(t, strictCodes, "the typed strict rejection itself must not emit build-time policy warnings")

	// In-range: both actions carry the exact 4096.00 and the runtime adds no
	// warning.
	inRangeCoord, inRangeCodes, err := runStatement(inRangeSQL, "", false)
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

}
