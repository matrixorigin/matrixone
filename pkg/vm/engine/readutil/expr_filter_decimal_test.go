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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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
	name          string
	colType       plan.Type
	min           string
	max           string
	op            string
	bounds        []decimalBound
	primary       bool
	uninitialized bool
	want          bool
}

func TestCompileFilterExprDecimalScaleMatrix(t *testing.T) {
	decimalTypes := []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 4}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}},
	}

	tests := make([]decimalScalePruningCase, 0, len(decimalTypes)*25+6)
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
				name: name + "/unknown_zm_fails_open", colType: colType, op: "<",
				bounds: []decimalBound{{"2", 0}}, uninitialized: true, want: true,
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
			if test.uninitialized {
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

			columnType := types.T(decimalType.typ.Id)
			require.True(t, anyLTByBound(zm, oneValue, one, columnType).mayMatch())
			require.True(t, anyLTByBound(zm, minusTwoValue, minusTwo, columnType).excludes())
			require.True(t, anyLEByBound(zm, minusTwoValue, minusTwo, columnType).mayMatch())
			require.True(t, anyGTByBound(zm, oneValue, one, columnType).mayMatch())
			require.True(t, anyGTByBound(zm, twoValue, two, columnType).excludes())
			require.True(t, anyGEByBound(zm, twoValue, two, columnType).mayMatch())
			require.True(t, intersectsBound(zm, oneValue, one, columnType).mayMatch())
			require.True(t, intersectsBound(zm, threeValue, three, columnType).excludes())
			require.True(t, anyBetweenBounds(
				zm, minusOneValue, oneScale1Value, minusOne, oneScale1, columnType,
			).mayMatch())
			require.True(t, anyBetweenBounds(zm, threeValue, fourValue, three, four, columnType).excludes())

			mismatchType := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 0}
			if types.T(decimalType.typ.Id) == types.T_decimal128 {
				mismatchType = plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 0}
			}
			mismatch := decimalZoneMap(t, mismatchType, "1")
			// A persisted type mismatch makes index.ZM comparison return ok=false.
			// Every decimal pruning helper must fail open in that case.
			require.True(t, anyLTByBound(zm, nil, mismatch, columnType).mayMatch())
			require.True(t, anyLEByBound(zm, nil, mismatch, columnType).mayMatch())
			require.True(t, anyGTByBound(zm, nil, mismatch, columnType).mayMatch())
			require.True(t, anyGEByBound(zm, nil, mismatch, columnType).mayMatch())
			require.True(t, intersectsBound(zm, nil, mismatch, columnType).mayMatch())
			require.True(t, anyBetweenBounds(zm, nil, nil, mismatch, mismatch, columnType).mayMatch())
			for hint := uint8(0); hint < 4; hint++ {
				require.True(t, inRangeBounds(zm, nil, nil, mismatch, mismatch, hint, columnType).mayMatch())
			}
		})
	}
}

func TestJSONZoneMapValueComparisonsFailOpen(t *testing.T) {
	columnType := types.T_json
	low := encodeReadutilJSON(t, `false`)
	high := encodeReadutilJSON(t, `true`)
	zm := index.NewZM(columnType, 0)
	index.UpdateZM(zm, low)
	index.UpdateZM(zm, high)

	for name, match := range map[string]zoneMapMatch{
		"lt":      anyLTByBound(zm, low, nil, columnType),
		"le":      anyLEByBound(zm, low, nil, columnType),
		"gt":      anyGTByBound(zm, high, nil, columnType),
		"ge":      anyGEByBound(zm, high, nil, columnType),
		"equal":   intersectsBound(zm, low, nil, columnType),
		"between": anyBetweenBounds(zm, low, high, nil, nil, columnType),
	} {
		t.Run(name, func(t *testing.T) {
			require.False(t, match.comparable)
			require.True(t, match.mayMatch())
			require.False(t, match.excludes())
		})
	}

	for hint := uint8(0); hint < 4; hint++ {
		match := inRangeBounds(zm, low, high, nil, nil, hint, columnType)
		require.Falsef(t, match.comparable, "range hint %d", hint)
		require.Truef(t, match.mayMatch(), "range hint %d", hint)
	}

	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(vec, low, false, mp))
	match := anyInVector(zm, vec, columnType)
	require.False(t, match.comparable)
	require.True(t, match.mayMatch())
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestCompileFilterExprJSONZoneMapsFailOpen(t *testing.T) {
	jsonType := plan.Type{Id: int32(types.T_json)}
	operations := []struct {
		name   string
		op     string
		bounds []string
		hints  []uint8
	}{
		{name: "lt", op: "<", bounds: []string{"25"}},
		{name: "le", op: "<=", bounds: []string{"25"}},
		{name: "gt", op: ">", bounds: []string{"15"}},
		{name: "ge", op: ">=", bounds: []string{"15"}},
		{name: "eq", op: "=", bounds: []string{"20"}},
		{name: "between", op: "between", bounds: []string{"15", "25"}},
		{name: "in_range", op: "in_range", bounds: []string{"15", "25"}, hints: []uint8{0, 1, 2, 3}},
		{name: "in", op: "in", bounds: []string{"20", "25"}},
	}

	for _, operation := range operations {
		hints := operation.hints
		if len(hints) == 0 {
			hints = []uint8{0}
		}
		for _, hint := range hints {
			name := operation.name
			if operation.op == "in_range" {
				name += "/hint_" + strconv.Itoa(int(hint))
			}
			t.Run(name, func(t *testing.T) {
				tableDef := decimalTableDef(jsonType, false)
				tableDef.Cols[0].ClusterBy = true
				expr := sortedUnknownFilter(t, jsonType, operation.op, operation.bounds, hint)
				fastFilter, _, _, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
				require.True(t, canCompile)
				require.NotNil(t, fastFilter)
				require.NotNil(t, blockFilter)

				stats := objectio.NewObjectStats()
				require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
					stats, sortedUnknownZoneMap(t, jsonType, "10", "30")))
				selected, err := fastFilter(stats)
				require.NoError(t, err)
				require.True(t, selected, "JSON object zone map must fail open")

				dataMeta := objectio.BuildMetaData(3, 1)
				dataMeta.MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, jsonType, "10", "30"))
				for block, value := range []string{"10", "20", "30"} {
					dataMeta.GetBlockMeta(uint32(block)).MustGetColumn(0).SetZoneMap(
						sortedUnknownZoneMap(t, jsonType, value))
					quickBreak, selected, err := blockFilter(block, dataMeta.GetBlockMeta(uint32(block)), nil)
					require.NoError(t, err)
					require.False(t, quickBreak, "JSON block zone map must not stop the scan")
					require.True(t, selected, "JSON block zone map must fail open")
				}
				if seek != nil {
					require.Equal(t, 0, seek(dataMeta), "JSON zone maps must not skip leading blocks")
				}
			})
		}
	}
}

func TestCompileFilterExprJSONIsNullUsesNullCount(t *testing.T) {
	jsonType := plan.Type{Id: int32(types.T_json)}
	tableDef := decimalTableDef(jsonType, false)
	expr := sortedUnknownFilter(t, jsonType, "isnull", []string{"unused"}, 0)
	_, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
	require.True(t, canCompile)
	require.NotNil(t, blockFilter)

	dataMeta := objectio.BuildMetaData(1, 1)
	block := dataMeta.GetBlockMeta(0)
	block.MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, jsonType, "value"))
	block.MustGetColumn(0).SetNullCnt(0)
	quickBreak, selected, err := blockFilter(0, block, nil)
	require.NoError(t, err)
	require.False(t, quickBreak)
	require.False(t, selected)

	block.MustGetColumn(0).SetNullCnt(1)
	quickBreak, selected, err = blockFilter(0, block, nil)
	require.NoError(t, err)
	require.False(t, quickBreak)
	require.True(t, selected)
}

func encodeReadutilJSON(t *testing.T, input string) []byte {
	t.Helper()
	value, err := bytejson.ParseFromString(input)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(value)
	require.NoError(t, err)
	return encoded
}

var zoneMapMatchSink zoneMapMatch
var zoneMapSeekSink int

func TestZoneMapMatchHelpersDoNotAllocate(t *testing.T) {
	intType := plan.Type{Id: int32(types.T_int64)}
	intZM := sortedUnknownZoneMap(t, intType, "10", "20")
	intBound := encodeSortedUnknownValue(t, intType, "15")
	unknownIntZM := index.NewZM(types.T_int64, 0)

	decimalType := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}
	decimalZM := decimalZoneMap(t, decimalType, "10.0000", "20.0000")
	decimalValue, decimalBoundZM := decimalBoundZoneMap(t, decimalType, decimalBound{"15", 0})
	unknownDecimalZM := index.NewZM(types.T_decimal128, decimalType.Scale)
	intInExpr := sortedUnknownVectorFilter(t, intType, []string{"15", "20"}, false)
	intInVec := vector.NewVec(types.T_any.ToType())
	require.NoError(t, intInVec.UnmarshalBinary(intInExpr.GetF().Args[1].GetVec().Data))
	prefixType := plan.Type{Id: int32(types.T_varchar), Width: 8}
	prefixZM := sortedUnknownZoneMap(t, prefixType, "10", "20")
	unknownPrefixZM := index.NewZM(types.T_varchar, 0)
	prefixInExpr := sortedUnknownVectorFilter(t, prefixType, []string{"15", "20"}, true)
	prefixInVec := vector.NewVec(types.T_any.ToType())
	require.NoError(t, prefixInVec.UnmarshalBinary(prefixInExpr.GetF().Args[1].GetVec().Data))
	prefixValue := encodeSortedUnknownValue(t, prefixType, "15")

	tests := []struct {
		name string
		fn   func() zoneMapMatch
	}{
		{
			name: "raw initialized",
			fn: func() zoneMapMatch {
				return anyLTByBound(intZM, intBound, nil, types.T_int64)
			},
		},
		{
			name: "raw unknown",
			fn: func() zoneMapMatch {
				return anyLTByBound(unknownIntZM, intBound, nil, types.T_int64)
			},
		},
		{
			name: "decimal initialized",
			fn: func() zoneMapMatch {
				return anyLTByBound(decimalZM, decimalValue, decimalBoundZM, types.T_decimal128)
			},
		},
		{
			name: "decimal unknown",
			fn: func() zoneMapMatch {
				return anyLTByBound(unknownDecimalZM, decimalValue, decimalBoundZM, types.T_decimal128)
			},
		},
		{
			name: "in initialized",
			fn: func() zoneMapMatch {
				return anyInVector(intZM, intInVec, types.T_int64)
			},
		},
		{
			name: "in unknown",
			fn: func() zoneMapMatch {
				return anyInVector(unknownIntZM, intInVec, types.T_int64)
			},
		},
		{
			name: "prefix initialized",
			fn: func() zoneMapMatch {
				return prefixEqByValue(prefixZM, prefixValue, types.T_varchar)
			},
		},
		{
			name: "prefix unknown",
			fn: func() zoneMapMatch {
				return prefixInVector(unknownPrefixZM, prefixInVec, types.T_varchar)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(1000, func() {
				zoneMapMatchSink = test.fn()
			})
			require.Zero(t, allocations)
		})
	}

	tableDef := decimalTableDef(decimalType, false)
	tableDef.Cols[0].ClusterBy = true
	expr := decimalFoldedFilter(t, decimalType, ">=", decimalBound{"15", 0})
	_, _, _, _, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
	require.True(t, canCompile)
	require.NotNil(t, seek)
	dataMeta := decimalObjectDataMeta(t, decimalType, "10.0000", "20.0000", "30.0000")
	allocations := testing.AllocsPerRun(1000, func() {
		zoneMapSeekSink = seek(dataMeta)
	})
	require.Zero(t, allocations)
}

func BenchmarkSeekFirstBlockByZoneMap(b *testing.B) {
	for _, blockCount := range []int{1, 16, 256, 4096} {
		b.Run(strconv.Itoa(blockCount)+"_blocks", func(b *testing.B) {
			dataMeta := objectio.BuildMetaData(uint16(blockCount), 1)
			objectZM := index.NewZM(types.T_int64, 0)
			for i := range blockCount {
				value := int64(i)
				valueBytes := types.EncodeInt64(&value)
				index.UpdateZM(objectZM, valueBytes)
				blockZM := index.NewZM(types.T_int64, 0)
				index.UpdateZM(blockZM, valueBytes)
				dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(blockZM)
			}
			dataMeta.MustGetColumn(0).SetZoneMap(objectZM)
			boundValue := int64(blockCount / 2)
			boundBytes := types.EncodeInt64(&boundValue)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				zoneMapSeekSink = seekFirstBlockByZoneMap(
					dataMeta, 0, nil, types.T_int64,
					func(zm objectio.ZoneMap) zoneMapMatch {
						return anyGEByBound(zm, boundBytes, nil, types.T_int64)
					},
				)
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

func TestCompileFilterExprSortedUnknownZoneMapDoesNotExcludeLaterBlocks(t *testing.T) {
	typesUnderTest := []struct {
		name string
		typ  plan.Type
	}{
		{name: "decimal64", typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}},
		{name: "decimal128", typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2}},
		{name: "int64", typ: plan.Type{Id: int32(types.T_int64)}},
		{name: "varchar", typ: plan.Type{Id: int32(types.T_varchar), Width: 8}},
	}
	operations := []struct {
		name       string
		op         string
		bounds     []string
		quickBreak bool
		hints      []uint8
		prefixOnly bool
	}{
		{name: "lt", op: "<", bounds: []string{"25"}, quickBreak: true},
		{name: "le", op: "<=", bounds: []string{"20"}, quickBreak: true},
		{name: "gt", op: ">", bounds: []string{"15"}},
		{name: "ge", op: ">=", bounds: []string{"20"}},
		{name: "eq", op: "=", bounds: []string{"20"}, quickBreak: true},
		{name: "between", op: "between", bounds: []string{"15", "25"}, quickBreak: true},
		{name: "in_range", op: "in_range", bounds: []string{"15", "25"}, quickBreak: true,
			hints: []uint8{0, 1, 2, 3}},
		{name: "in", op: "in", bounds: []string{"20", "25"}, quickBreak: true},
		{name: "prefix_eq", op: "prefix_eq", bounds: []string{"20"}, quickBreak: true, prefixOnly: true},
		{name: "prefix_between", op: "prefix_between", bounds: []string{"15", "25"}, quickBreak: true, prefixOnly: true},
		{name: "prefix_in_range", op: "prefix_in_range", bounds: []string{"15", "25"}, quickBreak: true,
			hints: []uint8{0, 1, 2, 3}, prefixOnly: true},
		{name: "prefix_in", op: "prefix_in", bounds: []string{"20", "25"}, quickBreak: true, prefixOnly: true},
	}

	for _, typ := range typesUnderTest {
		for _, operation := range operations {
			if operation.prefixOnly && types.T(typ.typ.Id) != types.T_varchar {
				continue
			}
			hints := operation.hints
			if len(hints) == 0 {
				hints = []uint8{0}
			}
			for _, hint := range hints {
				name := typ.name + "/" + operation.name
				if operation.op == "in_range" || operation.op == "prefix_in_range" {
					name += "/hint_" + strconv.Itoa(int(hint))
				}
				t.Run(name, func(t *testing.T) {
					tableDef := decimalTableDef(typ.typ, false)
					tableDef.Cols[0].ClusterBy = true
					expr := sortedUnknownFilter(t, typ.typ, operation.op, operation.bounds, hint)
					fastFilter, _, _, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
					require.True(t, canCompile)
					require.NotNil(t, fastFilter)
					require.NotNil(t, blockFilter)

					dataMeta := sortedUnknownDataMeta(t, typ.typ)
					stats := objectio.NewObjectStats()
					require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
						stats, index.NewZM(types.T(typ.typ.Id), typ.typ.Scale)))
					selected, err := fastFilter(stats)
					require.NoError(t, err)
					require.True(t, selected, "unknown object ZM must fail open")

					quickBreak, selected, err := blockFilter(0, dataMeta.GetBlockMeta(0), nil)
					require.NoError(t, err)
					require.False(t, quickBreak, "unknown first block must not stop the scan")
					require.True(t, selected, "unknown first block must fail open")

					quickBreak, selected, err = blockFilter(1, dataMeta.GetBlockMeta(1), nil)
					require.NoError(t, err)
					require.False(t, quickBreak)
					require.True(t, selected, "later matching block must remain reachable")

					quickBreak, _, err = blockFilter(2, dataMeta.GetBlockMeta(2), nil)
					require.NoError(t, err)
					require.Equal(t, operation.quickBreak, quickBreak)

					if seek != nil {
						require.Equal(t, 0, seek(dataMeta), "unknown leading block must make seek fail open")
					}
				})
			}
		}
	}
}

func TestCompileFilterExprSortedIncompatibleZoneMapFailsOpen(t *testing.T) {
	tests := []struct {
		name         string
		columnType   plan.Type
		metadataType plan.Type
	}{
		{
			name:         "int64 column with varchar metadata",
			columnType:   plan.Type{Id: int32(types.T_int64)},
			metadataType: plan.Type{Id: int32(types.T_varchar), Width: 8},
		},
		{
			name:         "varchar column with int64 metadata",
			columnType:   plan.Type{Id: int32(types.T_varchar), Width: 8},
			metadataType: plan.Type{Id: int32(types.T_int64)},
		},
		{
			name:         "varchar column with text metadata",
			columnType:   plan.Type{Id: int32(types.T_varchar), Width: 8},
			metadataType: plan.Type{Id: int32(types.T_text)},
		},
		{
			name:         "varchar column with blob metadata",
			columnType:   plan.Type{Id: int32(types.T_varchar), Width: 8},
			metadataType: plan.Type{Id: int32(types.T_blob)},
		},
		{
			name:         "varchar column with json metadata",
			columnType:   plan.Type{Id: int32(types.T_varchar), Width: 8},
			metadataType: plan.Type{Id: int32(types.T_json)},
		},
	}
	operations := []struct {
		op         string
		bounds     []string
		hint       uint8
		prefixOnly bool
	}{
		{op: "=", bounds: []string{"20"}},
		{op: "in", bounds: []string{"20", "25"}},
		{op: "prefix_eq", bounds: []string{"20"}, prefixOnly: true},
		{op: "prefix_between", bounds: []string{"15", "25"}, prefixOnly: true},
		{op: "prefix_in_range", bounds: []string{"15", "25"}, hint: 3, prefixOnly: true},
		{op: "prefix_in", bounds: []string{"20", "25"}, prefixOnly: true},
	}
	for _, test := range tests {
		for _, operation := range operations {
			if operation.prefixOnly && types.T(test.columnType.Id) != types.T_varchar {
				continue
			}
			t.Run(test.name+"/"+operation.op, func(t *testing.T) {
				tableDef := decimalTableDef(test.columnType, false)
				tableDef.Cols[0].ClusterBy = true
				expr := sortedUnknownFilter(t, test.columnType, operation.op, operation.bounds, operation.hint)
				fastFilter, _, _, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
				require.True(t, canCompile)

				mismatch := sortedUnknownZoneMap(t, test.metadataType, "20")
				stats := objectio.NewObjectStats()
				require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, mismatch))
				selected, err := fastFilter(stats)
				require.NoError(t, err)
				require.True(t, selected)

				dataMeta := objectio.BuildMetaData(2, 1)
				dataMeta.MustGetColumn(0).SetZoneMap(mismatch)
				dataMeta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(mismatch.Clone())
				dataMeta.GetBlockMeta(1).MustGetColumn(0).SetZoneMap(mismatch.Clone())
				quickBreak, selected, err := blockFilter(0, dataMeta.GetBlockMeta(0), nil)
				require.NoError(t, err)
				require.False(t, quickBreak)
				require.True(t, selected)
				require.Equal(t, 0, seek(dataMeta))
			})
		}
	}
}

func TestCompileFilterExprInVectorMetadataMismatchFailsOpen(t *testing.T) {
	columnType := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2}
	vectorType := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}
	tableDef := decimalTableDef(columnType, true)
	expr := sortedUnknownVectorFilterWithType(
		t, columnType, vectorType, []string{"20.0000", "25.0000"}, false,
	)
	fastFilter, _, _, blockFilter, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
	require.True(t, canCompile)

	stats := decimalObjectStats(t, columnType, "20.00", "25.00")
	selected, err := fastFilter(stats)
	require.NoError(t, err)
	require.True(t, selected, "scale-mismatched IN vector must fail open")

	dataMeta := decimalObjectDataMeta(t, columnType, "20.00", "25.00")
	quickBreak, selected, err := blockFilter(0, dataMeta.GetBlockMeta(0), nil)
	require.NoError(t, err)
	require.False(t, quickBreak)
	require.True(t, selected, "unsafe raw-byte Bloom lookup must be skipped")
}

func TestCompileFilterExprMalformedVectorDoesNotCompile(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar), Width: 8}
	tableDef := decimalTableDef(typ, false)
	for _, prefix := range []bool{false, true} {
		name := "in"
		if prefix {
			name = "prefix_in"
		}
		t.Run(name, func(t *testing.T) {
			expr := sortedUnknownVectorFilter(t, typ, []string{"20"}, prefix)
			expr.GetF().Args[1].GetVec().Data = []byte{1, 2, 3}
			_, _, _, _, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
			require.False(t, canCompile)
		})
	}

	t.Run("prefix_in physical type mismatch", func(t *testing.T) {
		expr := sortedUnknownVectorFilterWithType(
			t, typ, plan.Type{Id: int32(types.T_int64)}, []string{"20"}, true,
		)
		_, _, _, _, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
		require.False(t, canCompile)
	})
}

func TestCompileFilterExprPrefixInNullableVectorFailsOpen(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar), Width: 8}
	proc := testutil.NewProcess(t)
	vec := vector.NewVec(plan2.MakeTypeByPlan2Type(typ))
	defer vec.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(vec, nil, true, proc.Mp()))
	require.NoError(t, vector.AppendBytes(vec, []byte("00000020"), false, proc.Mp()))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	expr := plan2.MakeInExpr(
		context.Background(),
		&plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: 0, ColPos: 0, Name: "amount",
			}},
		},
		int32(vec.Length()),
		data,
		true,
	)
	tableDef := decimalTableDef(typ, false)
	tableDef.Cols[0].ClusterBy = true
	fastFilter, _, _, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
	require.True(t, canCompile)
	require.Nil(t, seek)

	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
		stats, sortedUnknownZoneMap(t, typ, "40"),
	))
	selected, err := fastFilter(stats)
	require.NoError(t, err)
	require.True(t, selected)

	dataMeta := objectio.BuildMetaData(1, 1)
	dataMeta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ, "40"))
	quickBreak, selected, err := blockFilter(0, dataMeta.GetBlockMeta(0), nil)
	require.NoError(t, err)
	require.False(t, quickBreak)
	require.True(t, selected)
}

func TestSeekFirstBlockFailsOpenForUnsampledUnknownZoneMap(t *testing.T) {
	typesUnderTest := []struct {
		name         string
		columnType   plan.Type
		mismatchType plan.Type
	}{
		{
			name:         "decimal64",
			columnType:   plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2},
			mismatchType: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2},
		},
		{
			name:         "decimal128",
			columnType:   plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2},
			mismatchType: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2},
		},
		{
			name:         "int64",
			columnType:   plan.Type{Id: int32(types.T_int64)},
			mismatchType: plan.Type{Id: int32(types.T_varchar), Width: 8},
		},
		{
			name:         "varchar",
			columnType:   plan.Type{Id: int32(types.T_varchar), Width: 8},
			mismatchType: plan.Type{Id: int32(types.T_int64)},
		},
	}
	for _, typ := range typesUnderTest {
		for _, state := range []string{"uninitialized", "incompatible"} {
			t.Run(typ.name+"/"+state, func(t *testing.T) {
				tableDef := decimalTableDef(typ.columnType, false)
				tableDef.Cols[0].ClusterBy = true
				expr := sortedUnknownFilter(t, typ.columnType, ">=", []string{"35"}, 0)
				_, _, _, _, seek, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
				require.True(t, canCompile)
				require.NotNil(t, seek)

				dataMeta := objectio.BuildMetaData(5, 1)
				dataMeta.MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ.columnType, "10", "50"))
				dataMeta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ.columnType, "10"))
				if state == "uninitialized" {
					dataMeta.GetBlockMeta(1).MustGetColumn(0).SetZoneMap(
						index.NewZM(types.T(typ.columnType.Id), typ.columnType.Scale))
				} else {
					dataMeta.GetBlockMeta(1).MustGetColumn(0).SetZoneMap(
						sortedUnknownZoneMap(t, typ.mismatchType, "15"))
				}
				dataMeta.GetBlockMeta(2).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ.columnType, "20"))
				dataMeta.GetBlockMeta(3).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ.columnType, "40"))
				dataMeta.GetBlockMeta(4).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ.columnType, "50"))

				// sort.Search for the ordinary boundary probes blocks 2, 4 and 3;
				// block 1 is deliberately unsampled and must still force seek=0.
				require.Equal(t, 0, seek(dataMeta))
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
		args = append(args, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt64(&value)}},
		})
	}
	return foldedFunction(op, args)
}

func sortedUnknownFilter(
	t *testing.T,
	typ plan.Type,
	op string,
	bounds []string,
	hint uint8,
) *plan.Expr {
	t.Helper()
	if op == "in" || op == "prefix_in" {
		return sortedUnknownVectorFilter(t, typ, bounds, op == "prefix_in")
	}
	if types.T(typ.Id).IsDecimal() {
		decimalBounds := make([]decimalBound, len(bounds))
		for i, bound := range bounds {
			decimalBounds[i] = decimalBound{text: bound, scale: 0}
		}
		if op == "in_range" {
			return decimalInRangeFoldedFilter(t, typ, decimalBounds[0], decimalBounds[1], hint)
		}
		return decimalFoldedFilter(t, typ, op, decimalBounds...)
	}

	args := []*plan.Expr{{
		Typ: typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 0, ColPos: 0, Name: "amount",
		}},
	}}
	for _, bound := range bounds {
		args = append(args, &plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
				IsConst: true,
				Data:    encodeSortedUnknownValue(t, typ, bound),
			}},
		})
	}
	if op == "in_range" || op == "prefix_in_range" {
		hintValue := hint
		args = append(args, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_uint8)},
			Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeUint8(&hintValue)}},
		})
	}
	return foldedFunction(op, args)
}

func sortedUnknownVectorFilter(
	t *testing.T,
	typ plan.Type,
	values []string,
	prefix bool,
) *plan.Expr {
	return sortedUnknownVectorFilterWithType(t, typ, typ, values, prefix)
}

func sortedUnknownVectorFilterWithType(
	t *testing.T,
	columnType plan.Type,
	vectorType plan.Type,
	values []string,
	prefix bool,
) *plan.Expr {
	t.Helper()
	proc := testutil.NewProcess(t)
	vec := vector.NewVec(plan2.MakeTypeByPlan2Type(vectorType))
	defer vec.Free(proc.Mp())
	for _, value := range values {
		encoded := encodeSortedUnknownValue(t, vectorType, value)
		switch types.T(vectorType.Id) {
		case types.T_decimal64:
			require.NoError(t, vector.AppendFixed(vec, types.DecodeDecimal64(encoded), false, proc.Mp()))
		case types.T_decimal128:
			require.NoError(t, vector.AppendFixed(vec, types.DecodeDecimal128(encoded), false, proc.Mp()))
		case types.T_int64:
			require.NoError(t, vector.AppendFixed(vec, types.DecodeInt64(encoded), false, proc.Mp()))
		case types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_json:
			require.NoError(t, vector.AppendBytes(vec, encoded, false, proc.Mp()))
		default:
			t.Fatalf("unsupported vector zonemap test type %v", types.T(vectorType.Id))
		}
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	return plan2.MakeInExpr(
		context.Background(),
		&plan.Expr{
			Typ: columnType,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: 0, ColPos: 0, Name: "amount",
			}},
		},
		int32(len(values)),
		data,
		prefix,
	)
}

func sortedUnknownDataMeta(t *testing.T, typ plan.Type) objectio.ObjectDataMeta {
	t.Helper()
	dataMeta := objectio.BuildMetaData(3, 1)
	unknown := index.NewZM(types.T(typ.Id), typ.Scale)
	dataMeta.MustGetColumn(0).SetZoneMap(unknown)
	dataMeta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(unknown.Clone())
	dataMeta.GetBlockMeta(1).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ, "20"))
	dataMeta.GetBlockMeta(2).MustGetColumn(0).SetZoneMap(sortedUnknownZoneMap(t, typ, "40"))
	return dataMeta
}

func sortedUnknownZoneMap(t *testing.T, typ plan.Type, values ...string) objectio.ZoneMap {
	t.Helper()
	zm := index.NewZM(types.T(typ.Id), typ.Scale)
	for _, value := range values {
		index.UpdateZM(zm, encodeSortedUnknownValue(t, typ, value))
	}
	return zm
}

func encodeSortedUnknownValue(t *testing.T, typ plan.Type, value string) []byte {
	t.Helper()
	switch types.T(typ.Id) {
	case types.T_decimal64, types.T_decimal128:
		return encodeDecimal(t, typ, value)
	case types.T_int64:
		parsed, err := strconv.ParseInt(value, 10, 64)
		require.NoError(t, err)
		return types.EncodeInt64(&parsed)
	case types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_json:
		return []byte("000000" + value)
	default:
		t.Fatalf("unsupported sorted zonemap test type %v", types.T(typ.Id))
		return nil
	}
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
