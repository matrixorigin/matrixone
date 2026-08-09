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

package readutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

type decimalZoneMapCompilerContext struct {
	*plan2.MockCompilerContext
}

func (c *decimalZoneMapCompilerContext) Resolve(
	dbName string,
	tableName string,
	_ *plan2.Snapshot,
) (*plan2.ObjectRef, *plan2.TableDef, error) {
	typ := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}
	switch tableName {
	case "decimal_scan64":
		typ = plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}
	case "decimal_scan128":
	default:
		return c.MockCompilerContext.Resolve(dbName, tableName, nil)
	}
	return &plan.ObjectRef{SchemaName: dbName, ObjName: tableName}, &plan.TableDef{
		Name:          tableName,
		DbName:        dbName,
		Name2ColIndex: map[string]int32{"amount": 0},
		Cols: []*plan.ColDef{{
			Name:   "amount",
			ColId:  1,
			Seqnum: 0,
			Typ:    typ,
		}},
	}, nil
}

func TestCompileFilterExprDecimalScaleMatchesPublicPlan(t *testing.T) {
	tests := []struct {
		name             string
		table            string
		literalScale     int32
		literalRaw       any
		blockMin         string
		blockMax         string
		selectedExpected bool
	}{
		{
			name:             "decimal64 planner rescales literal",
			table:            "decimal_scan64",
			literalScale:     4,
			literalRaw:       types.Decimal64(20000000),
			blockMin:         "1000.0000",
			blockMax:         "2500.0000",
			selectedExpected: true,
		},
		{
			name:             "decimal128 planner preserves low scale literal",
			table:            "decimal_scan128",
			literalScale:     0,
			literalRaw:       types.Decimal128{B0_63: 2000},
			blockMin:         "1000.0000",
			blockMax:         "2500.0000",
			selectedExpected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := &decimalZoneMapCompilerContext{MockCompilerContext: plan2.NewMockCompilerContext(true)}
			ctx.SetContext(context.Background())
			stmt, err := mysql.ParseOne(ctx.GetContext(), "select count(*) from "+test.table+" where amount < 2000", 1)
			require.NoError(t, err)
			queryPlan, err := plan2.BuildPlan(ctx, stmt, false)
			require.NoError(t, err)

			var scan *plan.Node
			for _, node := range queryPlan.GetQuery().Nodes {
				if node.NodeType == plan.Node_TABLE_SCAN {
					scan = node
					break
				}
			}
			require.NotNil(t, scan)
			require.Len(t, scan.FilterList, 1)
			filter := scan.FilterList[0]
			require.Equal(t, test.literalScale, filter.GetF().Args[1].Typ.Scale)
			require.Equal(t, test.literalRaw, decimalLiteralValue(filter.GetF().Args[1]))

			meta := makeDecimalBlockMeta(t, scan.TableDef.Cols[0].Typ, test.blockMin, test.blockMax)
			proc := testutil.NewProcess(t)
			need := plan2.AssignAuxIdForExpr(filter, 0)
			selectedByGeneralPath := colexec.EvaluateFilterByZoneMap(
				proc.Ctx,
				proc,
				filter,
				meta,
				map[int]int{0: 0},
				make([]objectio.ZoneMap, need),
				make([]*vector.Vector, need),
			)
			require.Equal(t, test.selectedExpected, selectedByGeneralPath)

			compiledFilter := plan2.DeepCopyExpr(filter)
			var executors []colexec.ExpressionExecutor
			_, err = plan2.ReplaceFoldExpr(proc, compiledFilter, &executors)
			require.NoError(t, err)
			require.NoError(t, plan2.EvalFoldExpr(proc, compiledFilter, &executors))
			for _, executor := range executors {
				executor.Free()
			}

			_, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(compiledFilter, scan.TableDef, nil)
			require.True(t, canCompile)
			require.NotNil(t, blockFilter)
			_, selectedByFastPath, err := blockFilter(0, meta, nil)
			require.NoError(t, err)
			require.Equal(t, selectedByGeneralPath, selectedByFastPath)
		})
	}
}

type decimalBound struct {
	text  string
	scale int32
}

type decimalScalePruningCase struct {
	name     string
	colType  plan.Type
	min      string
	max      string
	op       string
	bounds   []decimalBound
	primary  bool
	nullOnly bool
	want     bool
}

func TestCompileFilterExprDecimalScaleMatrix(t *testing.T) {
	decimalTypes := []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}},
	}

	var tests []decimalScalePruningCase
	for _, decimalType := range decimalTypes {
		add := func(name, min, max, op string, want bool, bounds ...decimalBound) {
			tests = append(tests, decimalScalePruningCase{
				name: decimalType.name + "/" + name, colType: decimalType.typ,
				min: min, max: max, op: op, bounds: bounds, want: want,
			})
		}
		add("lt_scale0_keeps", "1000.0000", "2500.0000", "<", true, decimalBound{"2000", 0})
		add("lt_scale0_prunes", "2000.0000", "2500.0000", "<", false, decimalBound{"2000", 0})
		add("lt_scale1_keeps", "1000.0000", "2500.0000", "<", true, decimalBound{"2000.0", 1})
		add("lt_scale4_prunes", "2000.0000", "2500.0000", "<", false, decimalBound{"2000.0000", 4})
		add("le_scale0_boundary", "2000.0000", "2500.0000", "<=", true, decimalBound{"2000", 0})
		add("gt_scale0_keeps", "1000.0000", "2500.0000", ">", true, decimalBound{"2000", 0})
		add("gt_scale0_prunes", "1000.0000", "2000.0000", ">", false, decimalBound{"2000", 0})
		add("ge_scale0_boundary", "1000.0000", "2000.0000", ">=", true, decimalBound{"2000", 0})
		add("eq_scale0_keeps", "1999.9000", "2000.1000", "=", true, decimalBound{"2000", 0})
		add("eq_scale0_prunes", "2000.0001", "2500.0000", "=", false, decimalBound{"2000", 0})
		add("between_scale0_keeps", "500.0000", "1500.0000", "between", true,
			decimalBound{"1000", 0}, decimalBound{"2000", 0})
		add("between_scale0_prunes_upper", "2000.0001", "2500.0000", "between", false,
			decimalBound{"1000", 0}, decimalBound{"2000", 0})
		add("between_scale0_prunes_lower", "500.0000", "999.9999", "between", false,
			decimalBound{"1000", 0}, decimalBound{"2000", 0})
		add("between_mixed_scale_keeps", "500.0000", "1500.0000", "between", true,
			decimalBound{"1000.0", 1}, decimalBound{"2000.0000", 4})
		add("negative_lt_keeps", "-2500.0000", "-1000.0000", "<", true, decimalBound{"-2000", 0})
		add("negative_lt_prunes", "-1999.9999", "-1000.0000", "<", false, decimalBound{"-2000", 0})
		add("zero_eq_keeps", "-0.0001", "0.0001", "=", true, decimalBound{"0", 0})
	}

	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128} {
		width := int32(12)
		name := "decimal64"
		if oid == types.T_decimal128 {
			width = 20
			name = "decimal128"
		}
		colType := plan.Type{Id: int32(oid), Width: width, Scale: 2}
		tests = append(tests,
			decimalScalePruningCase{
				name: name + "/lossy_upper_lt_keeps", colType: colType,
				min: "2.00", max: "2.00", op: "<", bounds: []decimalBound{{"2.0010", 4}}, want: true,
			},
			decimalScalePruningCase{
				name: name + "/lossy_lower_gt_keeps", colType: colType,
				min: "2.00", max: "2.00", op: ">", bounds: []decimalBound{{"1.9990", 4}}, want: true,
			},
			decimalScalePruningCase{
				name: name + "/lossy_between_lower_keeps", colType: colType,
				min: "1.01", max: "1.01", op: "between",
				bounds: []decimalBound{{"1.0050", 4}, {"2", 0}}, want: true,
			},
			decimalScalePruningCase{
				name: name + "/lossy_between_lower_prunes", colType: colType,
				min: "1.00", max: "1.00", op: "between",
				bounds: []decimalBound{{"1.0050", 4}, {"2", 0}}, want: false,
			},
			decimalScalePruningCase{
				name: name + "/lossy_between_upper_keeps", colType: colType,
				min: "2.00", max: "2.00", op: "between",
				bounds: []decimalBound{{"1", 0}, {"2.0050", 4}}, want: true,
			},
			decimalScalePruningCase{
				name: name + "/lossy_between_upper_prunes", colType: colType,
				min: "2.01", max: "2.01", op: "between",
				bounds: []decimalBound{{"1", 0}, {"2.0050", 4}}, want: false,
			},
			decimalScalePruningCase{
				name: name + "/null_only_prunes", colType: colType, op: "<",
				bounds: []decimalBound{{"2", 0}}, nullOnly: true, want: false,
			},
			decimalScalePruningCase{
				name: name + "/primary_eq_mismatched_scale_skips_bloom", colType: colType,
				min: "1.00", max: "3.00", op: "=", bounds: []decimalBound{{"2.0000", 4}}, primary: true, want: true,
			},
		)
	}

	tests = append(tests,
		decimalScalePruningCase{
			name: "decimal64/positive_extreme_scale", colType: plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 17},
			min: "9.99999999999999998", max: "9.99999999999999999", op: "<",
			bounds: []decimalBound{{"10", 0}}, want: true,
		},
		decimalScalePruningCase{
			name: "decimal64/negative_extreme_scale", colType: plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 17},
			min: "-9.99999999999999999", max: "-9.99999999999999998", op: ">",
			bounds: []decimalBound{{"-10", 0}}, want: true,
		},
		decimalScalePruningCase{
			name: "decimal64/positive_scale_overflow", colType: plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 17},
			min: "9.99999999999999998", max: "9.99999999999999999", op: "<",
			bounds: []decimalBound{{"100", 0}}, want: true,
		},
		decimalScalePruningCase{
			name: "decimal64/negative_scale_overflow", colType: plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 17},
			min: "-9.99999999999999999", max: "-9.99999999999999998", op: ">",
			bounds: []decimalBound{{"-100", 0}}, want: true,
		},
		decimalScalePruningCase{
			name: "decimal128/positive_scale_overflow", colType: plan.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 37},
			min: "9.9999999999999999999999999999999999998", max: "9.9999999999999999999999999999999999999", op: "<",
			bounds: []decimalBound{{"100", 0}}, want: true,
		},
		decimalScalePruningCase{
			name: "decimal128/negative_scale_overflow", colType: plan.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 37},
			min: "-9.9999999999999999999999999999999999999", max: "-9.9999999999999999999999999999999999998", op: ">",
			bounds: []decimalBound{{"-100", 0}}, want: true,
		},
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDef := decimalTableDef(test.colType, test.primary)
			expr := decimalFoldedFilter(t, test.colType, test.op, test.bounds...)
			_, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.True(t, canCompile)
			require.NotNil(t, blockFilter)

			var meta objectio.BlockObject
			if test.nullOnly {
				meta = makeDecimalBlockMeta(t, test.colType)
			} else {
				meta = makeDecimalBlockMeta(t, test.colType, test.min, test.max)
			}
			_, selected, err := blockFilter(0, meta, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, selected)
		})
	}
}

func TestDecimalZoneMapScaleAwareComparisons(t *testing.T) {
	decimalTypes := []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}},
	}
	for _, decimalType := range decimalTypes {
		t.Run(decimalType.name, func(t *testing.T) {
			zm := decimalZoneMap(t, decimalType.typ, "-2.0000", "2.0000")
			oneValue, one := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"1", 0})
			minusTwoValue, minusTwo := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"-2", 0})
			twoValue, two := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"2", 0})
			threeValue, three := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"3", 0})
			minusOneValue, minusOne := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"-1", 0})
			oneScale1Value, oneScale1 := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"1.0", 1})
			fourValue, four := decimalBoundZoneMap(t, decimalType.typ, decimalBound{"4.00", 2})

			require.True(t, anyLTByBound(zm, oneValue, one))
			require.False(t, anyLTByBound(zm, minusTwoValue, minusTwo))
			require.True(t, anyLEByBound(zm, minusTwoValue, minusTwo))
			require.True(t, anyGTByBound(zm, oneValue, one))
			require.False(t, anyGTByBound(zm, twoValue, two))
			require.True(t, anyGEByBound(zm, twoValue, two))
			require.True(t, intersectsBound(zm, oneValue, one))
			require.False(t, intersectsBound(zm, threeValue, three))
			require.True(t, anyBetweenBounds(zm, minusOneValue, oneScale1Value, minusOne, oneScale1))
			require.False(t, anyBetweenBounds(zm, threeValue, fourValue, three, four))

			mismatchType := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 0}
			if types.T(decimalType.typ.Id) == types.T_decimal128 {
				mismatchType = plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 0}
			}
			mismatch := decimalZoneMap(t, mismatchType, "1")
			// A persisted type mismatch makes index.ZM comparison return ok=false.
			// Every decimal pruning helper must fail open in that case.
			require.True(t, anyLTByBound(zm, nil, mismatch))
			require.True(t, anyLEByBound(zm, nil, mismatch))
			require.True(t, anyGTByBound(zm, nil, mismatch))
			require.True(t, anyGEByBound(zm, nil, mismatch))
			require.True(t, intersectsBound(zm, nil, mismatch))
			require.True(t, anyBetweenBounds(zm, nil, nil, mismatch, mismatch))
			for hint := uint8(0); hint < 4; hint++ {
				require.True(t, inRangeBounds(zm, nil, nil, mismatch, mismatch, hint))
			}
		})
	}
}

func TestCompileFilterExprDecimalComparisonFailureSkipsBloom(t *testing.T) {
	decimalTypes := []struct {
		name         string
		colType      plan.Type
		metadataType plan.Type
	}{
		{
			name:         "decimal64_column_decimal128_metadata",
			colType:      plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 0},
			metadataType: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 0},
		},
		{
			name:         "decimal128_column_decimal64_metadata",
			colType:      plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 0},
			metadataType: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 0},
		},
	}
	for _, decimalType := range decimalTypes {
		t.Run(decimalType.name, func(t *testing.T) {
			tableDef := decimalTableDef(decimalType.colType, true)
			expr := decimalFoldedFilter(t, decimalType.colType, "=", decimalBound{"1", 0})
			_, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.True(t, canCompile)

			dataMeta := objectio.BuildMetaData(1, 1)
			meta := dataMeta.GetBlockMeta(0)
			meta.MustGetColumn(0).SetZoneMap(decimalZoneMap(t, decimalType.metadataType, "1"))
			quickBreak, selected, err := blockFilter(0, meta, nil)
			require.NoError(t, err)
			require.False(t, quickBreak)
			require.True(t, selected)
		})
	}
}

func TestCompileFilterExprDecimalSortedBetweenPaths(t *testing.T) {
	decimalTypes := []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}},
	}
	for _, decimalType := range decimalTypes {
		t.Run(decimalType.name, func(t *testing.T) {
			tableDef := decimalTableDef(decimalType.typ, false)
			tableDef.Cols[0].ClusterBy = true
			expr := decimalFoldedFilter(t, decimalType.typ, "between",
				decimalBound{"1", 0}, decimalBound{"2.0", 1})
			fastFilter, _, objectFilter, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.True(t, canCompile)
			require.NotNil(t, fastFilter)
			require.NotNil(t, objectFilter)
			require.NotNil(t, blockFilter)
			require.NotNil(t, seek)

			dataMeta := decimalObjectDataMeta(t, decimalType.typ, "0.0000", "1.0000", "2.0000", "3.0000")
			stats := decimalObjectStats(t, decimalType.typ, "0.0000", "3.0000")
			selected, err := fastFilter(stats)
			require.NoError(t, err)
			require.True(t, selected)
			selected, err = objectFilter(nil, nil)
			require.NoError(t, err)
			require.True(t, selected)
			require.Equal(t, 1, seek(dataMeta))

			quickBreak, selected, err := blockFilter(1, dataMeta.GetBlockMeta(1), nil)
			require.NoError(t, err)
			require.False(t, quickBreak)
			require.True(t, selected)
			quickBreak, selected, err = blockFilter(3, dataMeta.GetBlockMeta(3), nil)
			require.NoError(t, err)
			require.True(t, quickBreak)
			require.False(t, selected)
		})
	}
}

func TestCompileFilterExprDecimalSortedInRangeHints(t *testing.T) {
	decimalTypes := []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2}},
	}
	for _, decimalType := range decimalTypes {
		for hint := uint8(0); hint < 4; hint++ {
			t.Run(decimalType.name+"/hint_"+string(rune('0'+hint)), func(t *testing.T) {
				tableDef := decimalTableDef(decimalType.typ, false)
				tableDef.Cols[0].ClusterBy = true
				expr := decimalInRangeFoldedFilter(t, decimalType.typ,
					decimalBound{"1", 0}, decimalBound{"3.0", 1}, hint)
				fastFilter, _, objectFilter, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
				require.True(t, canCompile)
				require.NotNil(t, fastFilter)
				require.NotNil(t, objectFilter)
				require.NotNil(t, blockFilter)
				require.NotNil(t, seek)

				dataMeta := decimalObjectDataMeta(t, decimalType.typ, "1.00", "2.00", "3.00", "4.00")
				stats := decimalObjectStats(t, decimalType.typ, "1.00", "4.00")
				selected, err := fastFilter(stats)
				require.NoError(t, err)
				require.True(t, selected)
				selected, err = objectFilter(nil, nil)
				require.NoError(t, err)
				require.True(t, selected)

				wantSeek := 0
				if hint == 1 || hint == 3 {
					wantSeek = 1
				}
				require.Equal(t, wantSeek, seek(dataMeta))

				quickBreak, selected, err := blockFilter(2, dataMeta.GetBlockMeta(2), nil)
				require.NoError(t, err)
				if hint == 2 || hint == 3 {
					require.True(t, quickBreak)
					require.False(t, selected)
				} else {
					require.False(t, quickBreak)
					require.True(t, selected)
				}
				quickBreak, selected, err = blockFilter(3, dataMeta.GetBlockMeta(3), nil)
				require.NoError(t, err)
				require.True(t, quickBreak)
				require.False(t, selected)
			})
		}
	}
}

func TestCompileFilterExprDecimalScaleNonDecimalControl(t *testing.T) {
	tableDef := &plan.TableDef{
		Name:          "int_scan",
		Name2ColIndex: map[string]int32{"amount": 0},
		Cols: []*plan.ColDef{{
			Name: "amount", ColId: 1, Seqnum: 0,
			Typ: plan.Type{Id: int32(types.T_int64)},
		}},
	}
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)
	zm := index.NewZM(types.T_int64, 0)
	minValue, maxValue := int64(10), int64(20)
	index.UpdateZM(zm, types.EncodeInt64(&minValue))
	index.UpdateZM(zm, types.EncodeInt64(&maxValue))
	meta.MustGetColumn(0).SetZoneMap(zm)

	tests := []struct {
		name   string
		op     string
		bounds []int64
		want   bool
	}{
		{name: "lt prunes", op: "<", bounds: []int64{5}, want: false},
		{name: "le keeps", op: "<=", bounds: []int64{10}, want: true},
		{name: "gt prunes", op: ">", bounds: []int64{20}, want: false},
		{name: "ge keeps", op: ">=", bounds: []int64{20}, want: true},
		{name: "eq keeps", op: "=", bounds: []int64{15}, want: true},
		{name: "between prunes", op: "between", bounds: []int64{21, 30}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr := int64FoldedFilter(test.op, test.bounds...)
			_, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.True(t, canCompile)
			_, selected, err := blockFilter(0, meta, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, selected)
		})
	}
}

func decimalTableDef(typ plan.Type, primary bool) *plan.TableDef {
	return &plan.TableDef{
		Name:          "decimal_scan",
		Name2ColIndex: map[string]int32{"amount": 0},
		Cols: []*plan.ColDef{{
			Name: "amount", ColId: 1, Seqnum: 0, Primary: primary, Typ: typ,
		}},
	}
}

func decimalFoldedFilter(t *testing.T, colType plan.Type, op string, bounds ...decimalBound) *plan.Expr {
	t.Helper()
	args := []*plan.Expr{{
		Typ: colType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 0, ColPos: 0, Name: "amount",
		}},
	}}
	for _, bound := range bounds {
		boundType := colType
		boundType.Scale = bound.scale
		if types.T(boundType.Id) == types.T_decimal64 {
			boundType.Width = 18
		} else {
			boundType.Width = 38
		}
		args = append(args, &plan.Expr{
			Typ:  boundType,
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: encodeDecimal(t, boundType, bound.text)}},
		})
	}
	return foldedFunction(op, args)
}

func decimalInRangeFoldedFilter(
	t *testing.T,
	colType plan.Type,
	lower, upper decimalBound,
	hint uint8,
) *plan.Expr {
	expr := decimalFoldedFilter(t, colType, "in_range", lower, upper)
	hintValue := hint
	expr.GetF().Args = append(expr.GetF().Args, &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_uint8)},
		Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeUint8(&hintValue)}},
	})
	return expr
}

func int64FoldedFilter(op string, bounds ...int64) *plan.Expr {
	args := []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 0, ColPos: 0, Name: "amount",
		}},
	}}
	for _, value := range bounds {
		value := value
		args = append(args, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt64(&value)}},
		})
	}
	return foldedFunction(op, args)
}

func foldedFunction(name string, args []*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name}, Args: args,
		}},
	}
}

func decimalLiteralValue(expr *plan.Expr) any {
	switch types.T(expr.Typ.Id) {
	case types.T_decimal64:
		return types.Decimal64(expr.GetLit().GetDecimal64Val().A)
	case types.T_decimal128:
		value := expr.GetLit().GetDecimal128Val()
		return types.Decimal128{B0_63: uint64(value.A), B64_127: uint64(value.B)}
	default:
		return nil
	}
}

func encodeDecimal(t *testing.T, typ plan.Type, text string) []byte {
	t.Helper()
	switch types.T(typ.Id) {
	case types.T_decimal64:
		value, err := types.ParseDecimal64(text, typ.Width, typ.Scale)
		require.NoError(t, err)
		return types.EncodeDecimal64(&value)
	case types.T_decimal128:
		value, err := types.ParseDecimal128(text, typ.Width, typ.Scale)
		require.NoError(t, err)
		return types.EncodeDecimal128(&value)
	default:
		t.Fatalf("unsupported decimal type %v", types.T(typ.Id))
		return nil
	}
}

func decimalBoundZoneMap(
	t *testing.T,
	colType plan.Type,
	bound decimalBound,
) ([]byte, objectio.ZoneMap) {
	t.Helper()
	boundType := colType
	boundType.Scale = bound.scale
	if types.T(boundType.Id) == types.T_decimal64 {
		boundType.Width = 18
	} else {
		boundType.Width = 38
	}
	value := encodeDecimal(t, boundType, bound.text)
	zoneMap, ok := makeDecimalZoneMapBound(
		&plan.ColDef{Typ: colType},
		value,
		&plan.Expr{Typ: boundType},
	)
	require.True(t, ok)
	return value, zoneMap
}

func decimalZoneMap(t *testing.T, typ plan.Type, values ...string) objectio.ZoneMap {
	t.Helper()
	zm := index.NewZM(types.T(typ.Id), typ.Scale)
	for _, text := range values {
		index.UpdateZM(zm, encodeDecimal(t, typ, text))
	}
	return zm
}

func decimalObjectDataMeta(t *testing.T, typ plan.Type, values ...string) objectio.ObjectDataMeta {
	t.Helper()
	dataMeta := objectio.BuildMetaData(uint16(len(values)), 1)
	objectZM := decimalZoneMap(t, typ, values...)
	dataMeta.MustGetColumn(0).SetZoneMap(objectZM)
	for i, text := range values {
		dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(decimalZoneMap(t, typ, text))
	}
	return dataMeta
}

func decimalObjectStats(t *testing.T, typ plan.Type, values ...string) *objectio.ObjectStats {
	t.Helper()
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, decimalZoneMap(t, typ, values...)))
	return stats
}

func makeDecimalBlockMeta(t *testing.T, typ plan.Type, values ...string) objectio.BlockObject {
	t.Helper()
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)
	zm := index.NewZM(types.T(typ.Id), typ.Scale)
	for _, text := range values {
		index.UpdateZM(zm, encodeDecimal(t, typ, text))
	}
	meta.MustGetColumn(0).SetZoneMap(zm)
	return meta
}
