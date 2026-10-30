// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	clusterTenantCol = "tenant_id"
	clusterDeviceCol = "device_id"
	clusterMetricCol = "metric_id"
	clusterTsCol     = "event_ts"
)

var clusterByCompoundName = planutil.BuildCompositeClusterByColumnName([]string{
	clusterTenantCol, clusterDeviceCol, clusterMetricCol, clusterTsCol,
})

// makeCompositeClusterTableDef mirrors the table definition the catalog cache
// builds for the reported table: visible time-series columns, a fake primary
// key (the table has no primary key) and the hidden compound cluster column.
func makeCompositeClusterTableDef(charsetOfPart func(name string) int32) *plan.TableDef {
	mkCol := func(name string, id int32, oid types.T, seqnum uint32, hidden, clusterBy bool) *plan.ColDef {
		return &plan.ColDef{
			Name:      name,
			ColId:     uint64(id),
			Seqnum:    seqnum,
			Hidden:    hidden,
			ClusterBy: clusterBy,
			Typ: plan.Type{
				Id:       int32(oid),
				Charset:  uint32(charsetOfPart(name)),
				Width:    64,
				AutoIncr: false,
			},
		}
	}
	cols := []*plan.ColDef{
		mkCol(clusterTenantCol, 0, types.T_int32, 0, false, false),
		mkCol(clusterDeviceCol, 1, types.T_int32, 1, false, false),
		mkCol(clusterMetricCol, 2, types.T_int32, 2, false, false),
		mkCol(clusterTsCol, 3, types.T_datetime, 3, false, false),
		mkCol("value", 4, types.T_float64, 4, false, false),
		mkCol("quality", 5, types.T_int16, 5, false, false),
		mkCol(catalog.FakePrimaryKeyColName, 6, types.T_uint64, 6, true, false),
		mkCol(clusterByCompoundName, 7, types.T_varchar, 7, true, true),
	}
	name2Idx := make(map[string]int32, len(cols))
	for i, col := range cols {
		name2Idx[col.Name] = int32(i)
	}
	compoundCol := cols[len(cols)-1]
	return &plan.TableDef{
		TblId:         22,
		Name:          "metrics",
		Cols:          cols,
		Name2ColIndex: name2Idx,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
			Names:       []string{catalog.FakePrimaryKeyColName},
		},
		ClusterBy: &plan.ClusterByDef{
			Name:         clusterByCompoundName,
			CompCbkeyCol: compoundCol,
		},
	}
}

func binaryCharset(string) int32 { return int32(types.CharsetLegacy) }

func unicodeCharsetFor(name string) int32 {
	if name == clusterDeviceCol {
		return int32(types.CharsetUTF8MB4UnicodeCI)
	}
	return int32(types.CharsetLegacy)
}

func newTestPackerPool() *fileservice.Pool[*types.Packer] {
	return fileservice.NewPool(
		8,
		func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() },
		func(p *types.Packer) { p.Close() },
	)
}

// serialConstForTest binds and constant-folds a serial() call the same way
// the planner does for compound-key bounds, returning both the serialized
// literal expression and the raw serial bytes it evaluated to.
func serialConstForTest(
	t *testing.T,
	proc *process.Process,
	args ...*plan.Expr,
) (*plan.Expr, []byte) {
	t.Helper()
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "serial", args)
	require.NoError(t, err)
	folded, err := plan2.ConstantFold(
		batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded)
	lit := folded.GetLit()
	require.NotNil(t, lit)
	require.True(t, lit.IsSerialized, "serial must fold to an opaque serialized literal")
	return folded, []byte(lit.GetSval())
}

func colExprForTest(pos int32, oid types.T, name string) *plan.Expr {
	containerType := oid.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&containerType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: pos,
				Name:   name,
			},
		},
	}
}

func clusterColExpr(pos int32) *plan.Expr {
	containerType := types.T_varchar.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&containerType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: pos,
				Name:   clusterByCompoundName,
			},
		},
	}
}

func bindAndFold(
	t *testing.T,
	proc *process.Process,
	name string,
	args ...*plan.Expr,
) *plan.Expr {
	t.Helper()
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
	require.NoError(t, err)
	foldExpressionForTest(t, proc, expr)
	return expr
}

// TestNewMemClusterKeyFilterShapes covers every predicate shape the planner
// emits for a composite cluster key, asserting the derived op and that the
// packed keys live in the same encoded domain the replay index uses.
func TestNewMemClusterKeyFilterShapes(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	tableDef := makeCompositeClusterTableDef(binaryCharset)
	packerPool := newTestPackerPool()
	ts := timestamp.Timestamp{PhysicalTime: 42}
	packer := types.NewPacker()
	defer packer.Close()

	encode := func(serial []byte, stripped bool) []byte {
		packer.Reset()
		packer.EncodeStringType(serial)
		key := packer.Bytes()
		if stripped {
			key = key[:len(key)-1]
		}
		return key
	}

	intConst := func(v int32) *plan.Expr {
		return plan2.MakePlan2Int32ConstExprWithType(v)
	}
	tsConst := func(s string) *plan.Expr {
		datetime, err := types.ParseDatetime(s, 6)
		require.NoError(t, err)
		return plan2.MakePlan2DateTimeConstExprWithType(int64(datetime))
	}

	type testCase struct {
		name     string
		expr     func() *plan.Expr
		wantOp   int
		wantKeys int
		// wantStripped reports whether every packed key drops the trailing
		// terminator, the prefix-predicate domain.
		wantStripped bool
		serialOf     func() [][]byte
	}

	lo, loSerial := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0), tsConst("2026-01-01 00:03:00"))
	hi, hiSerial := serialConstForTest(t, proc, intConst(9), intConst(9), intConst(9), tsConst("2026-01-02 00:00:00"))
	p0, p0Serial := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0))
	p0Ts, p0TsSerial := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0), tsConst("2026-01-01 00:00:10"))

	cases := []testCase{
		{
			name: "prefix_in_range closed, the reported predicate",
			expr: func() *plan.Expr {
				return bindAndFold(t, proc, "prefix_in_range",
					clusterColExpr(7), lo, p0,
					plan2.MakePlan2Uint8ConstExprWithType(0))
			},
			wantOp:       function.PREFIX_BETWEEN,
			wantKeys:     2,
			wantStripped: true,
			serialOf:     func() [][]byte { return [][]byte{loSerial, p0Serial} },
		},
		{
			name: "prefix_eq",
			expr: func() *plan.Expr {
				return bindAndFold(t, proc, "prefix_eq", clusterColExpr(7), p0)
			},
			wantOp:       function.PREFIX_EQ,
			wantKeys:     1,
			wantStripped: true,
			serialOf:     func() [][]byte { return [][]byte{p0Serial} },
		},
		{
			name: "equal",
			expr: func() *plan.Expr {
				return bindAndFold(t, proc, "=", clusterColExpr(7), p0Ts)
			},
			wantOp:   function.EQUAL,
			wantKeys: 1,
			serialOf: func() [][]byte { return [][]byte{p0TsSerial} },
		},
		{
			name: "prefix_between",
			expr: func() *plan.Expr {
				return bindAndFold(t, proc, "prefix_between", clusterColExpr(7), lo, hi)
			},
			wantOp:       function.PREFIX_BETWEEN,
			wantKeys:     2,
			wantStripped: true,
			serialOf:     func() [][]byte { return [][]byte{loSerial, hiSerial} },
		},
		{
			name: "great_equal",
			expr: func() *plan.Expr {
				return bindAndFold(t, proc, ">=", clusterColExpr(7), p0Ts)
			},
			wantOp:   function.GREAT_EQUAL,
			wantKeys: 1,
			serialOf: func() [][]byte { return [][]byte{p0TsSerial} },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := NewMemClusterKeyFilter(
				tableDef, tc.expr(), ts, proc.Mp(), packerPool)
			require.NoError(t, err)
			require.True(t, filter.Valid())
			require.Equal(t, clusterByCompoundName, filter.Descriptor.Name)
			wantTS := types.TimestampToTS(ts)
			require.True(t, filter.TS.EQ(&wantTS))

			specs := filter.Specs()
			require.Len(t, specs, 1)
			if tc.wantOp != 0 {
				require.Equal(t, tc.wantOp, specs[0].Op)
			}
			require.Len(t, specs[0].Keys, tc.wantKeys)
			for i, key := range specs[0].Keys {
				require.Equal(t, encode(tc.serialOf()[i], tc.wantStripped), key)
			}
		})
	}
}

// TestNewMemClusterKeyFilterPrefixInRangeFlags covers every interval
// openness flag of prefix_in_range.
func TestNewMemClusterKeyFilterPrefixInRangeFlags(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	tableDef := makeCompositeClusterTableDef(binaryCharset)
	packerPool := newTestPackerPool()
	ts := timestamp.Timestamp{PhysicalTime: 42}

	intConst := func(v int32) *plan.Expr {
		return plan2.MakePlan2Int32ConstExprWithType(v)
	}
	tsConst := func(s string) *plan.Expr {
		datetime, err := types.ParseDatetime(s, 6)
		require.NoError(t, err)
		return plan2.MakePlan2DateTimeConstExprWithType(int64(datetime))
	}
	lo, _ := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0), tsConst("2026-01-01 00:03:00"))
	ub, _ := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0))

	wantOps := []int{
		function.PREFIX_BETWEEN,
		PrefixRangeLeftOpen,
		PrefixRangeRightOpen,
		PrefixRangeBothOpen,
	}
	for flag, wantOp := range wantOps {
		expr := bindAndFold(t, proc, "prefix_in_range",
			clusterColExpr(7), lo, ub,
			plan2.MakePlan2Uint8ConstExprWithType(uint8(flag)))
		filter, err := NewMemClusterKeyFilter(
			tableDef, expr, ts, proc.Mp(), packerPool)
		require.NoError(t, err)
		require.True(t, filter.Valid())
		specs := filter.Specs()
		require.Len(t, specs, 1)
		require.Equal(t, wantOp, specs[0].Op)
		require.Len(t, specs[0].Keys, 2)
	}
}

// TestNewMemClusterKeyFilterInvalidCases covers the fail-open contract: any
// table or expression the composite cluster path cannot interpret safely
// must leave the filter invalid so the read falls back to full iteration.
func TestNewMemClusterKeyFilterInvalidCases(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	tableDef := makeCompositeClusterTableDef(binaryCharset)
	packerPool := newTestPackerPool()
	ts := timestamp.Timestamp{PhysicalTime: 42}

	intConst := func(v int32) *plan.Expr {
		return plan2.MakePlan2Int32ConstExprWithType(v)
	}
	p0, _ := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0))

	singleClusterTable := makeCompositeClusterTableDef(binaryCharset)
	singleClusterTable.ClusterBy = &plan.ClusterByDef{Name: clusterDeviceCol}

	unicodeTable := makeCompositeClusterTableDef(unicodeCharsetFor)

	missingCompoundCol := makeCompositeClusterTableDef(binaryCharset)
	missingCompoundCol.Cols = missingCompoundCol.Cols[:len(missingCompoundCol.Cols)-1]
	delete(missingCompoundCol.Name2ColIndex, clusterByCompoundName)
	missingCompoundCol.ClusterBy.CompCbkeyCol = nil

	clusterPredicate := func() *plan.Expr {
		return bindAndFold(t, proc, "prefix_eq", clusterColExpr(7), p0)
	}
	nonClusterPredicate := func() *plan.Expr {
		return bindAndFold(t, proc, "=",
			colExprForTest(0, types.T_int32, clusterTenantCol), intConst(0))
	}

	// The fold pass may only run once over a whole tree, exactly like the
	// runtime pipeline: bind the complete OR first, fold afterwards.
	bindOnly := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	orWithUnsupportedArm := func() *plan.Expr {
		left := bindOnly("prefix_eq", clusterColExpr(7), p0)
		right := bindOnly("<",
			colExprForTest(4, types.T_float64, "value"),
			plan2.MakePlan2Float64ConstExprWithType(0.5))
		expr := bindOnly("or", left, right)
		foldExpressionForTest(t, proc, expr)
		return expr
	}
	orOfTwoClusterArms := func() *plan.Expr {
		p1, _ := serialConstForTest(t, proc, intConst(1), intConst(1), intConst(1))
		left := bindOnly("prefix_eq", clusterColExpr(7), p0)
		right := bindOnly("prefix_eq", clusterColExpr(7), p1)
		expr := bindOnly("or", left, right)
		foldExpressionForTest(t, proc, expr)
		return expr
	}

	tests := []struct {
		name     string
		tableDef *plan.TableDef
		expr     func() *plan.Expr
	}{
		{"no cluster by", &plan.TableDef{}, clusterPredicate},
		{"single-column cluster by", singleClusterTable, clusterPredicate},
		{"unicode collated cluster part", unicodeTable, clusterPredicate},
		{"compound column missing from cols", missingCompoundCol, clusterPredicate},
		{"predicate on non-cluster column", tableDef, nonClusterPredicate},
		{"nil expression", tableDef, func() *plan.Expr { return nil }},
		{"or with unsupported arm", tableDef, orWithUnsupportedArm},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := NewMemClusterKeyFilter(
				tc.tableDef, tc.expr(), ts, proc.Mp(), packerPool)
			require.NoError(t, err)
			require.False(t, filter.Valid())
			require.Nil(t, filter.Specs())
		})
	}

	t.Run("or of two cluster arms stays valid", func(t *testing.T) {
		filter, err := NewMemClusterKeyFilter(
			tableDef, orOfTwoClusterArms(), ts, proc.Mp(), packerPool)
		require.NoError(t, err)
		require.True(t, filter.Valid())
		require.NotEmpty(t, filter.Specs())
	})
}

// TestMemPKFilterCarriesClusterSection proves the embedding contract: a
// primary-key filter for a fake-pk cluster table stays invalid while the
// attached cluster section drives the cluster accessors.
func TestMemPKFilterCarriesClusterSection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	tableDef := makeCompositeClusterTableDef(binaryCharset)
	packerPool := newTestPackerPool()
	ts := timestamp.Timestamp{PhysicalTime: 42}

	intConst := func(v int32) *plan.Expr {
		return plan2.MakePlan2Int32ConstExprWithType(v)
	}
	p0, p0Serial := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0))
	expr := bindAndFold(t, proc, "prefix_eq", clusterColExpr(7), p0)

	// The primary-key filter of a fake-pk table is invalid for the compound
	// predicate, exactly the reported gap.
	pkFilter, err := NewMemPKFilter(tableDef, ts, packerPool, BasePKFilter{}, engine.FilterHint{})
	require.NoError(t, err)
	require.False(t, pkFilter.Valid())
	require.False(t, pkFilter.ClusterKeyValid())
	require.Nil(t, pkFilter.ClusterKeySpecs())

	clusterFilter, err := NewMemClusterKeyFilter(
		tableDef, expr, ts, proc.Mp(), packerPool)
	require.NoError(t, err)
	require.True(t, clusterFilter.Valid())

	pkFilter.SetClusterKeyFilter(&clusterFilter)
	require.True(t, pkFilter.ClusterKeyValid())
	require.False(t, pkFilter.Valid(), "the cluster section must not change PK validity")
	exact, hit := pkFilter.Exact()
	require.False(t, exact)
	require.False(t, hit)

	specs := pkFilter.ClusterKeySpecs()
	require.Len(t, specs, 1)
	require.Equal(t, function.PREFIX_EQ, specs[0].Op)
	wantTS := types.TimestampToTS(ts)
	gotTS := pkFilter.ClusterKeyTS()
	require.True(t, gotTS.EQ(&wantTS))

	// The embedded cluster keys share the index encoding domain.
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeStringType(p0Serial)
	want := packer.Bytes()[:len(packer.Bytes())-1]
	require.Equal(t, want, specs[0].Keys[0])

	pkFilter.SetClusterKeyFilter(nil)
	require.False(t, pkFilter.ClusterKeyValid())
	require.Nil(t, pkFilter.ClusterKeySpecs())
}

// clusterCaptureSource records a copy of the mem filter the reader passes
// down before ending the scan immediately. A copy is required because
// reader.Close resets its own filter state afterwards.
type clusterCaptureSource struct {
	captured *MemPKFilter
}

func (s *clusterCaptureSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ int32,
	filter any,
	_ *mpool.MPool,
	_ *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	if s.captured == nil {
		if ff, ok := filter.(*MemPKFilter); ok {
			copied := *ff
			s.captured = &copied
		}
	}
	return nil, engine.End, nil
}

func (s *clusterCaptureSource) ApplyTombstones(
	context.Context, *objectio.Blockid, []int64, engine.TombstoneApplyPolicy,
) ([]int64, error) {
	return nil, nil
}
func (s *clusterCaptureSource) GetTombstones(
	context.Context, *objectio.Blockid,
) (objectio.Bitmap, error) {
	return objectio.Bitmap{}, nil
}
func (s *clusterCaptureSource) SetOrderBy([]*plan.OrderBySpec)  {}
func (s *clusterCaptureSource) GetOrderBy() []*plan.OrderBySpec { return nil }
func (s *clusterCaptureSource) SetFilterZM(objectio.ZoneMap)    {}
func (s *clusterCaptureSource) Close()                          {}
func (s *clusterCaptureSource) String() string                  { return "clusterCaptureSource" }

// TestNewReaderAttachesClusterFilter proves the reader carries the cluster
// predicate into the data source through the existing mem-filter channel.
func TestNewReaderAttachesClusterFilter(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	tableDef := makeCompositeClusterTableDef(binaryCharset)
	packerPool := newTestPackerPool()
	ts := timestamp.Timestamp{PhysicalTime: 42}

	intConst := func(v int32) *plan.Expr {
		return plan2.MakePlan2Int32ConstExprWithType(v)
	}
	tsConst := func(s string) *plan.Expr {
		datetime, err := types.ParseDatetime(s, 6)
		require.NoError(t, err)
		return plan2.MakePlan2DateTimeConstExprWithType(int64(datetime))
	}
	lo, _ := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0), tsConst("2026-01-01 00:03:00"))
	ub, _ := serialConstForTest(t, proc, intConst(0), intConst(0), intConst(0))
	expr := bindAndFold(t, proc, "prefix_in_range",
		clusterColExpr(7), lo, ub, plan2.MakePlan2Uint8ConstExprWithType(0))

	source := &clusterCaptureSource{}
	reader, err := NewReader(
		context.Background(),
		proc.Mp(),
		packerPool,
		nil,
		tableDef,
		ts,
		expr,
		source,
		engine.FilterHint{},
	)
	require.NoError(t, err)

	isEnd, err := reader.Read(
		context.Background(),
		[]string{clusterTenantCol},
		nil,
		proc.Mp(),
		batch.NewOffHeapWithSize(1),
	)
	require.NoError(t, err)
	require.True(t, isEnd)
	require.NoError(t, reader.Close())

	require.NotNil(t, source.captured)
	memFilter := source.captured
	require.False(t, memFilter.Valid(), "fake-pk table: the PK filter stays invalid")
	require.True(t, memFilter.ClusterKeyValid())
	specs := memFilter.ClusterKeySpecs()
	require.Len(t, specs, 1)
	require.Equal(t, function.PREFIX_BETWEEN, specs[0].Op)
	require.Len(t, specs[0].Keys, 2)
}

// TestPrimaryKeyFilterUnchangedByClusterPath guards the PK path: for a
// normal primary-key predicate the mem PK filter keeps its meaning while the
// cluster filter stays invalid.
func TestPrimaryKeyFilterUnchangedByClusterPath(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	tableDef := makeCompositeClusterTableDef(binaryCharset)
	packerPool := newTestPackerPool()
	ts := timestamp.Timestamp{PhysicalTime: 42}

	expr := bindAndFold(t, proc, "=",
		colExprForTest(0, types.T_int32, clusterTenantCol),
		plan2.MakePlan2Int32ConstExprWithType(0))

	base, err := ConstructBasePKFilter(expr, tableDef, proc.Mp())
	require.NoError(t, err)
	require.False(t, base.Valid, "fake-pk table cannot produce a PK predicate")

	clusterFilter, err := NewMemClusterKeyFilter(
		tableDef, expr, ts, proc.Mp(), packerPool)
	require.NoError(t, err)
	require.False(t, clusterFilter.Valid())
}
