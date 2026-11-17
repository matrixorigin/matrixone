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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

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

func foldedFunction(name string, args []*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name}, Args: args,
		}},
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

// An unknown intermediate block invalidates a binary seek, even when the
// first and last block have comparable summaries.
func TestDecimalSeekRetainsUnknownBlocks(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}
	table := decimalTableDef(typ, false)
	table.Cols[0].ClusterBy = true
	expr := decimalFoldedFilter(t, typ, ">=", decimalBound{"2", 0})
	_, _, _, blockFilter, seek, canCompile, _ := CompileFilterExpr(expr, table, nil)
	require.True(t, canCompile)
	require.NotNil(t, seek)
	meta := decimalObjectDataMeta(t, typ, "1.00", "2.00", "3.00")
	meta.GetBlockMeta(1).MustGetColumn(0).SetZoneMap(index.NewZM(types.T_decimal64, 2))
	require.Equal(t, 1, seek(meta), "skip only the preceding proven nonmatching block")
	require.Zero(t, seek(objectio.BuildMetaData(0, 1)))
	stop, selected, err := blockFilter(1, meta.GetBlockMeta(1), nil)
	require.NoError(t, err)
	require.False(t, stop)
	require.True(t, selected)
}

// A nonmatching block below an equality bound must not stop a sorted scan.
func TestDecimalSortedEqualityKeepsLaterMatches(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}
	table := decimalTableDef(typ, false)
	table.Cols[0].ClusterBy = true
	expr := decimalFoldedFilter(t, typ, "=", decimalBound{"2", 0})
	_, _, _, filter, seek, canCompile, _ := CompileFilterExpr(expr, table, nil)
	require.True(t, canCompile)
	require.NotNil(t, seek)
	meta := decimalObjectDataMeta(t, typ, "1.00", "2.00", "3.00")
	require.Equal(t, 1, seek(meta))
	for i, want := range []struct{ stop, selected bool }{{false, false}, {false, true}, {true, false}} {
		stop, selected, err := filter(i, meta.GetBlockMeta(uint32(i)), nil)
		require.NoError(t, err)
		require.Equal(t, want.stop, stop)
		require.Equal(t, want.selected, selected)
	}
}

// The PK path owns raw iterator bounds independently of zonemap filtering.
func TestDecimalPrimaryKeyBoundsRequirePhysicalDomain(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}
	table := decimalTableDef(typ, true)
	table.Pkey = &plan.PrimaryKeyDef{Names: []string{"amount"}, PkeyColName: "amount"}
	for _, tc := range []struct {
		name  string
		bound decimalBound
		valid bool
	}{
		{"same scale", decimalBound{"2.00", 2}, true},
		{"different scale", decimalBound{"2.0000", 4}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := decimalFoldedFilter(t, typ, ">=", tc.bound)
			filter, err := ConstructBasePKFilter(expr, table, mp)
			require.NoError(t, err)
			require.Equal(t, tc.valid, filter.Valid)
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestDecimalINUsesVectorPhysicalDomain(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	colType := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}
	table := decimalTableDef(colType, true)
	table.Pkey = &plan.PrimaryKeyDef{Names: []string{"amount"}, PkeyColName: "amount"}
	for _, tc := range []struct {
		name        string
		scale       int32
		coefficient types.Decimal64
		valid       bool
	}{
		{"same scale", 2, 200, true},
		{"different scale", 4, 20000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(types.New(types.T_decimal64, 12, tc.scale))
			require.NoError(t, vector.AppendFixed(vec, tc.coefficient, false, mp))
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			vec.Free(mp)
			col := &plan.Expr{Typ: colType, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "amount"}}}
			rhs := &plan.Expr{Typ: colType, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: data}}}
			expr := foldedFunction("in", []*plan.Expr{col, rhs})
			pk, err := ConstructBasePKFilter(expr, table, mp)
			require.NoError(t, err)
			require.Equal(t, tc.valid, pk.Valid)
			if !tc.valid {
				_, _, _, blockFilter, seek, compiled, _ := CompileFilterExpr(expr, table, nil)
				require.True(t, compiled)
				require.NotNil(t, blockFilter)
				require.Nil(t, seek)
				stop, selected, err := blockFilter(0, makeDecimalBlockMeta(t, colType, "2.00"), nil)
				require.NoError(t, err)
				require.False(t, stop)
				require.True(t, selected)
			}
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestDecimalUnknownComposesWithOtherFilters(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}
	table := decimalTableDef(typ, false)
	table.Name2ColIndex["b"] = 1
	table.Cols = append(table.Cols, &plan.ColDef{Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}})
	decimal := decimalFoldedFilter(t, typ, ">=", decimalBound{"2.0000", 4})
	wide := plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}
	decimal.GetF().Args[1] = &plan.Expr{Typ: wide, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
		IsConst: true, Data: encodeDecimal(t, wide, "2.0000"),
	}}}
	two := int64(2)
	integer := foldedFunction("=", []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "b", ColPos: 1}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
			IsConst: true, Data: types.EncodeInt64(&two),
		}}},
	})
	meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
	meta.MustGetColumn(0).SetZoneMap(decimalZoneMap(t, typ, "2.00"))
	intZM := index.NewZM(types.T_int64, 0)
	one := int64(1)
	index.UpdateZM(intZM, types.EncodeInt64(&one))
	meta.MustGetColumn(1).SetZoneMap(intZM)
	for _, logic := range []string{"and", "or"} {
		expr := foldedFunction(logic, []*plan.Expr{decimal, integer})
		_, _, _, blockFilter, _, compiled, _ := CompileFilterExpr(expr, table, nil)
		require.True(t, compiled)
		_, selected, err := blockFilter(0, meta, nil)
		require.NoError(t, err)
		require.Equal(t, logic == "or", selected)
	}
}
