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

package plan

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func workTestEntries(oid types.T, dimension int32) *plan.TableDef {
	return &plan.TableDef{Name: "entries", Cols: []*plan.ColDef{{
		Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
		Typ:  plan.Type{Id: int32(oid), Width: dimension},
	}}}
}

func TestIvfScanWorkUsesStoredEntries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		oid       types.T
		dimension int32
		bytes     float64
	}{
		{"f32", types.T_array_float32, 768, 3072},
		{"f64", types.T_array_float64, 768, 6144},
		{"f16", types.T_array_float16, 768, 1536},
		{"bf16", types.T_array_bf16, 768, 1536},
		{"int8", types.T_array_int8, 768, 768},
		{"uint8", types.T_array_uint8, 768, 768},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := &statsinfo.StatsInfo{TableCnt: 10_000_000, BlockNumber: 1221, AccurateObjectNumber: 20}
			work := ivfScanWorkFromStats(workTestEntries(tc.oid, tc.dimension), stats, 3162, 5)
			require.NotNil(t, work)
			require.Equal(t, int32(2), work.Blocks)
			require.Equal(t, tc.bytes, work.VectorBytesPerRow)
			require.InDelta(t, 10_000_000.0*5/3162, work.Rows, 0.00001)
			require.Equal(t, int32(20), work.Objects)
			require.Equal(t, int32(2), vectorScanDOP(16, &plan.VectorIndexScan{ScanWork: work}, false))
		})
	}
	stats := &statsinfo.StatsInfo{TableCnt: 10, BlockNumber: 4, ApproxObjectNumber: 2,
		SizeMap: map[string]uint64{catalog.SystemSI_IVFFLAT_TblCol_Entries_entry: 640}}
	work := ivfScanWorkFromStats(workTestEntries(types.T_array_float32, 768), stats, 1, 5)
	require.Equal(t, float64(64), work.VectorBytesPerRow, "measured entry-column bytes take precedence")
	require.Equal(t, int32(4), work.Blocks, "probes cannot exceed the complete index")
	require.Equal(t, int32(1), vectorScanDOP(16, &plan.VectorIndexScan{ScanWork: work}, false))
	work = ivfScanWorkFromStats(workTestEntries(types.T_array_float32, 768), stats, 4, 0)
	require.Equal(t, int32(1), work.Blocks, "zero probe uses the execution minimum of one")
}

func TestIvfScanWorkUnknownAndBounds(t *testing.T) {
	table := workTestEntries(types.T_array_float32, 768)
	for _, stats := range []*statsinfo.StatsInfo{
		nil, {}, {TableCnt: math.NaN(), BlockNumber: 1},
		{TableCnt: math.Inf(1), BlockNumber: 1}, {TableCnt: -1, BlockNumber: 1},
		{TableCnt: 1, BlockNumber: -1},
	} {
		require.Nil(t, ivfScanWorkFromStats(table, stats, 3162, 5))
	}
	stats := &statsinfo.StatsInfo{TableCnt: 10, BlockNumber: 4}
	require.Nil(t, ivfScanWorkFromStats(nil, stats, 1, 1))
	require.Nil(t, ivfScanWorkFromStats(&plan.TableDef{}, stats, 1, 1))
	require.Nil(t, ivfScanWorkFromStats(workTestEntries(types.T_int64, 768), stats, 1, 1))
	require.Nil(t, ivfScanWorkFromStats(workTestEntries(types.T_array_float32, 0), stats, 1, 1))
	require.Nil(t, ivfScanWorkFromStats(table, stats, 0, 5))
	stats.BlockNumber = math.MaxInt64
	stats.ApproxObjectNumber = math.MaxInt64
	work := ivfScanWorkFromStats(table, stats, 1, math.MaxInt64)
	require.Equal(t, int32(math.MaxInt32), work.Blocks)
	require.Equal(t, int32(math.MaxInt32), work.Objects)
	require.Equal(t, int32(3), vectorScanDOP(3, &plan.VectorIndexScan{ScanWork: work}, false))
}

func TestVectorScanDOPPreservesWorkAndOperatorCaps(t *testing.T) {
	spec := &plan.VectorIndexScan{ScanWork: &plan.VectorIndexScanWork{Rows: 100, Blocks: 40, VectorBytesPerRow: 3072, Objects: 5}}
	require.Equal(t, int32(5), vectorScanDOP(16, spec, false))
	require.Equal(t, int32(2), vectorScanDOP(2, spec, false))
	require.Equal(t, int32(1), vectorScanDOP(0, spec, false))
	spec.ScanWork.Objects = 0
	require.Equal(t, int32(16), vectorScanDOP(16, spec, true))
	spec.ScanWork.VectorBytesPerRow = 64
	require.Equal(t, int32(3), vectorScanDOP(16, spec, false))
	require.Equal(t, int32(1), vectorScanDOP(16, spec, true))
	for _, invalid := range []*plan.VectorIndexScan{
		nil, {}, {BucketExpandStep: 1, ScanWork: spec.ScanWork},
		{FirstRoundLimit: makePlan2Uint64ConstExprWithType(10), ScanWork: spec.ScanWork},
		{ScanWork: &plan.VectorIndexScanWork{Rows: math.NaN(), Blocks: 2, VectorBytesPerRow: 3072}},
		{ScanWork: &plan.VectorIndexScanWork{Rows: 10, Blocks: 2, VectorBytesPerRow: math.Inf(1)}},
		{ScanWork: &plan.VectorIndexScanWork{Rows: 10, Blocks: -2, VectorBytesPerRow: 3072}},
		{ScanWork: &plan.VectorIndexScanWork{Rows: 10, Blocks: 2, VectorBytesPerRow: 3072, Objects: -1}},
	} {
		require.Equal(t, int32(1), vectorScanDOP(16, invalid, false))
	}
	node := &plan.Node{NodeType: plan.Node_VECTOR_INDEX_SCAN, Stats: &plan.Stats{Rowsize: 16}, VectorIndexScan: spec}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{node}, Steps: []int32{0}}}}
	CalcQueryDOP(p, 16, 1, ExecTypeAP_ONECN)
	require.Equal(t, int32(3), node.Stats.Dop)
	setNodeDOP(p, 0, 16)
	require.Equal(t, int32(3), node.Stats.Dop, "ancestor cannot widen beyond scan work")
	setNodeDOP(p, 0, 1)
	require.Equal(t, int32(1), node.Stats.Dop)
	CalcQueryDOP(p, 16, 1, ExecTypeTP)
	require.Equal(t, int32(1), node.Stats.Dop)
	require.Equal(t, float64(16), node.Stats.Rowsize)
}

type scanWorkCompilerContext struct {
	CompilerContext
	t          *testing.T
	table      *plan.TableDef
	stats      *statsinfo.StatsInfo
	source     *plan.ObjectRef
	snapshot   *plan.Snapshot
	resolveErr error
	statsErr   error
	calls      int
	ctx        context.Context
}

func (c *scanWorkCompilerContext) GetContext() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return c.CompilerContext.GetContext()
}

func (c *scanWorkCompilerContext) ResolveIndexTableByRef(source *plan.ObjectRef, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	c.calls++
	require.Equal(c.t, c.source, source)
	require.Equal(c.t, c.snapshot, snapshot)
	require.Equal(c.t, "entries", name)
	return &plan.ObjectRef{Obj: 42, PubInfo: source.PubInfo}, c.table, c.resolveErr
}

func (c *scanWorkCompilerContext) StatsWithTableDef(obj *plan.ObjectRef, table *plan.TableDef, snapshot *plan.Snapshot) (*statsinfo.StatsInfo, error) {
	require.Equal(c.t, int64(42), obj.Obj)
	require.Same(c.t, c.table, table)
	require.Equal(c.t, c.snapshot, snapshot)
	require.Equal(c.t, c.source.PubInfo, obj.PubInfo)
	return c.stats, c.statsErr
}

func TestIvfScanWorkLookupIdentityAndErrors(t *testing.T) {
	builder, _, scan, _, _ := newIvfIncludeModeTestBuilder(t)
	scan.ObjRef.PubInfo = &plan.PubInfo{TenantId: 7}
	snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 20}}
	c := &scanWorkCompilerContext{CompilerContext: builder.compCtx, t: t, source: scan.ObjRef, snapshot: snapshot,
		table: workTestEntries(types.T_array_float32, 768), stats: &statsinfo.StatsInfo{TableCnt: 100, BlockNumber: 4}}
	builder.compCtx = c
	work, err := builder.estimateIvfScanWork(scan.ObjRef, snapshot, "entries", 1, 1)
	require.NoError(t, err)
	require.NotNil(t, work)
	c.resolveErr = errors.New("catalog unavailable")
	work, err = builder.estimateIvfScanWork(scan.ObjRef, snapshot, "entries", 1, 1)
	require.NoError(t, err)
	require.Nil(t, work)
	c.resolveErr = nil
	c.statsErr = errors.New("statistics unavailable")
	work, err = builder.estimateIvfScanWork(scan.ObjRef, snapshot, "entries", 1, 1)
	require.NoError(t, err)
	require.Nil(t, work)
	c.statsErr = context.DeadlineExceeded
	_, err = builder.estimateIvfScanWork(scan.ObjRef, snapshot, "entries", 1, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	c.statsErr = nil
	c.resolveErr = context.Canceled
	_, err = builder.estimateIvfScanWork(scan.ObjRef, snapshot, "entries", 1, 1)
	require.ErrorIs(t, err, context.Canceled)
	c.resolveErr = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.ctx = ctx
	calls := c.calls
	_, err = builder.estimateIvfScanWork(scan.ObjRef, snapshot, "entries", 1, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, calls, c.calls)
}

func TestIvfRewriteCostsEntriesNotOutput(t *testing.T) {
	for _, sourceBlocks := range []int32{1, 1221, 8192} {
		builder, _, scan, scanID, indexes := newIvfIncludeModeTestBuilder(t)
		mock := builder.compCtx.(*customMockCompilerContext)
		resolve := mock.resolveVarFunc
		mock.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
			if name == "probe_limit" {
				return int64(5), nil
			}
			return resolve(name, system, global)
		}
		c := &scanWorkCompilerContext{CompilerContext: builder.compCtx, t: t, source: scan.ObjRef,
			table: workTestEntries(types.T_array_float32, 768),
			stats: &statsinfo.StatsInfo{TableCnt: 10_000_000, BlockNumber: 1221, AccurateObjectNumber: 20}}
		builder.compCtx = c
		for _, idx := range indexes.IndexDefs {
			idx.IndexAlgoParams = `{"op_type":"vector_l2_ops","lists":"3162"}`
			idx.IncludedColumns = nil
		}
		scan.Stats = &plan.Stats{TableCnt: 100_000, BlockNum: sourceBlocks, Rowsize: 16}
		scan.FilterList = []*plan.Expr{makeIvfHelperFnExpr("=", plan.Type{Id: int32(types.T_bool)},
			makeIvfHelperColExpr(scan.BindingTags[0], 3, scan.TableDef), MakePlan2Int32ConstExprWithType(20))}
		vc := newIvfIncludeModeVectorSortContext(scan, scanID, "pre", 0, 3)
		_, err := builder.applyIndicesForSortUsingIvfflat(scanID, vc, indexes, nil, nil)
		require.NoError(t, err)
		n := findIvfTableFunctionNode(builder, vc.projNode.Children[0])
		require.NotNil(t, n)
		require.NotNil(t, n.VectorIndexScan.ScanWork)
		require.True(t, n.Stats.ForceOneCN)
		require.True(t, n.RuntimeFilterProbeList[0].RequiredVectorSearchDomain)
		require.Equal(t, float64(16), n.Stats.Rowsize)
		require.Equal(t, float64(12), n.Stats.Outcnt, "preserve the existing filtered candidate budget")
		require.Equal(t, int32(2), n.Stats.BlockNum)
		require.InDelta(t, 10_000_000.0*5/3162, n.Stats.Cost, 0.00001)
		ReCalcNodeStats(n.NodeId, builder, false, true, false)
		p := &plan.Plan{Plan: &plan.Plan_Query{Query: builder.qry}}
		CalcNodeDOP(p, n.NodeId, 16, 1)
		require.Equal(t, int32(2), n.Stats.Dop)
		require.Equal(t, float64(16), n.Stats.Rowsize)
		require.Equal(t, 1, c.calls, "one entries lookup per rewrite, not per reader")
	}
}
