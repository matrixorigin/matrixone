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

package arrowbridge

import (
	"context"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestRenderStagingDecimalTextContract(t *testing.T) {
	cases := []struct {
		name      string
		coeff     int64
		precision int32
		scale     int32
		want      string
	}{
		{name: "plain fixed point", coeff: 12345, precision: 10, scale: 2, want: "123.45"},
		{name: "negative keeps trailing zeros", coeff: -150, precision: 10, scale: 2, want: "-1.50"},
		{name: "scale surplus is zero padded", coeff: -4, precision: 10, scale: 3, want: "-0.004"},
		{name: "integer coefficient with scale", coeff: 100, precision: 5, scale: 2, want: "1.00"},
		{name: "zero keeps scale digits", coeff: 0, precision: 10, scale: 2, want: "0.00"},
		{name: "sub-unit coefficient", coeff: 7, precision: 4, scale: 4, want: "0.0007"},
		{name: "zero scale", coeff: -42, precision: 10, scale: 0, want: "-42"},
		{name: "scale beyond fixed bound renders exponent", coeff: 123, precision: 10, scale: 77, want: "123e-77"},
		{name: "negative scale renders positive exponent", coeff: 123, precision: 10, scale: -2, want: "123e2"},
		{name: "scale above precision renders exponent", coeff: 1, precision: 2, scale: 3, want: "1e-3"},
		{name: "precision above fixed bound renders exponent", coeff: 5, precision: 77, scale: 2, want: "5e-2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want,
				RenderStagingDecimalText(big.NewInt(tc.coeff), tc.precision, tc.scale))
		})
	}
}

// Scale-boundary scientific rendering must widen the source scale BEFORE
// negating: with scale == math.MinInt32 the exact value coeff x 10^-scale is
// coeff x 10^2147483648 — an out-of-range magnitude the downstream decimal
// conversion must reject (or loudly clamp), never silently store. Negating
// in int32 would wrap the exponent's sign and render coeff=1 as
// "1e-2147483648", whose cast quantizer path rounds to an ACCEPTED zero.
func TestRenderStagingDecimalTextScaleWidening(t *testing.T) {
	require.Equal(t, "1e2147483648",
		RenderStagingDecimalText(big.NewInt(1), 76, math.MinInt32))
	require.Equal(t, "-1e2147483648",
		RenderStagingDecimalText(big.NewInt(-1), 76, math.MinInt32))
	require.Equal(t, "0e2147483648",
		RenderStagingDecimalText(big.NewInt(0), 76, math.MinInt32))
	require.Equal(t, "1e2147483647",
		RenderStagingDecimalText(big.NewInt(1), 76, math.MinInt32+1))
	require.Equal(t, "1e1",
		RenderStagingDecimalText(big.NewInt(1), 76, -1))

	// The rendered staging text is consumed by the engine's decimal parsers:
	// the correctly widened magnitude is out of range for every target and is
	// rejected loudly at this parse level too.
	_, err := types.ParseDecimal128("1e2147483648", 38, 10)
	require.Error(t, err, "an exponent past MaxInt32 is an invalid Decimal128 literal")
	_, err = types.ParseDecimal256("1e2147483648", 76, 10)
	require.Error(t, err, "an exponent past MaxInt32 is an invalid Decimal256 literal")
}

func TestArrowDecimalTypeInfoOf(t *testing.T) {
	info, ok := arrowDecimalTypeInfoOf(&arrow.Decimal128Type{Precision: 18, Scale: 2})
	require.True(t, ok)
	require.Equal(t, arrowDecimalTypeInfo{precision: 18, scale: 2}, info)

	info, ok = arrowDecimalTypeInfoOf(&arrow.Decimal256Type{Precision: 38, Scale: 4})
	require.True(t, ok)
	require.Equal(t, arrowDecimalTypeInfo{precision: 38, scale: 4}, info)

	_, ok = arrowDecimalTypeInfoOf(arrow.PrimitiveTypes.Int64)
	require.False(t, ok)
}

// stagedTextTarget mirrors the scan-owned staging varchar the reader plan
// installs for LOAD attributed-DECIMAL targets.
func stagedTextTarget(name string) TargetColumn {
	return TargetColumn{Name: name, Type: types.New(types.T_varchar, 0, 0), TextStage: true}
}

func TestBindLoadTextStageConversionMatrix(t *testing.T) {
	ctx := context.Background()

	// Eligible scalar sources bind the text materialization only when the
	// staging marker is set.
	eligible := []struct {
		name string
		typ  arrow.DataType
	}{
		{"decimal128", &arrow.Decimal128Type{Precision: 18, Scale: 2}},
		{"decimal256", &arrow.Decimal256Type{Precision: 38, Scale: 4}},
		{"int8", arrow.PrimitiveTypes.Int8},
		{"int16", arrow.PrimitiveTypes.Int16},
		{"int32", arrow.PrimitiveTypes.Int32},
		{"int64", arrow.PrimitiveTypes.Int64},
		{"uint8", arrow.PrimitiveTypes.Uint8},
		{"uint16", arrow.PrimitiveTypes.Uint16},
		{"uint32", arrow.PrimitiveTypes.Uint32},
		{"uint64", arrow.PrimitiveTypes.Uint64},
		{"float16", &arrow.Float16Type{}},
		{"float32", arrow.PrimitiveTypes.Float32},
		{"float64", arrow.PrimitiveTypes.Float64},
		{"bool", arrow.FixedWidthTypes.Boolean},
	}
	for _, tc := range eligible {
		t.Run("eligible/"+tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: tc.typ}}, nil)
			_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
			require.NoError(t, err)

			// The same pair keeps the exact rejection without the marker.
			_, err = BindLoad(ctx, schema, []TargetColumn{{
				Name: "v", Type: types.New(types.T_varchar, 0, 0),
			}}, MatchByName)
			require.ErrorContains(t, err, "no exact long-term conversion")
		})
	}

	// String sources already have the borrowed raw-varlen conversion; the
	// marker must not change their selection.
	t.Run("string keeps borrowed varlen", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, nil)
		staged, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.NoError(t, err)
		plain, err := BindLoad(ctx, schema, []TargetColumn{{
			Name: "v", Type: types.New(types.T_varchar, 0, 0),
		}}, MatchByName)
		require.NoError(t, err)
		require.NotEqual(t, staged.Fingerprint(), plain.Fingerprint(),
			"the staging marker joins the conversion fingerprint")
	})

	// Binary, temporal, and nested sources keep the existing LOAD rejection
	// even for staged targets.
	rejected := []struct {
		name string
		typ  arrow.DataType
	}{
		{"date32", arrow.FixedWidthTypes.Date32},
		{"timestamp micros", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"binary", arrow.BinaryTypes.Binary},
		{"time64", arrow.FixedWidthTypes.Time64us},
	}
	for _, tc := range rejected {
		t.Run("rejected/"+tc.name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: tc.typ}}, nil)
			_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
			require.ErrorContains(t, err, "no exact long-term conversion")
		})
	}

	// A staged marker on an exact-physical decimal target must not disturb
	// the borrowed fixed-layout selection.
	t.Run("marker leaves exact decimal layout borrowed", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{
			Name: "v", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2},
		}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{{
			Name: "v", Type: types.New(types.T_decimal128, 18, 2), TextStage: true,
		}}, MatchByName)
		require.NoError(t, err)
	})

	// Dictionary encodings: staged text supports the borrowed string value
	// path and the rendered decimal-text path; other value types keep the
	// existing rejection.
	t.Run("dictionary string staged binds", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String,
		}}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.NoError(t, err)
	})
	t.Run("dictionary decimal staged binds", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: &arrow.Decimal128Type{Precision: 18, Scale: 2},
		}}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.NoError(t, err)
	})
	t.Run("dictionary date staged rejected", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.FixedWidthTypes.Date32,
		}}}, nil)
		_, err := BindLoad(ctx, schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
		require.ErrorContains(t, err, "no exact long-term conversion")
	})
}

func TestConvertMaterializesStagedDecimalText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	builder := array.NewDecimal128Builder(alloc, decimalType)
	builder.AppendValues([]decimal128.Num{
		decimal128.FromBigInt(big.NewInt(12345)),
		decimal128.FromBigInt(big.NewInt(-150)),
		decimal128.FromBigInt(big.NewInt(0)),
	}, []bool{true, true, false})
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: decimalType, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	plan, err := BindLoad(context.Background(), schema, []TargetColumn{stagedTextTarget("d")}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, len(bat.Vecs))
	require.Equal(t, types.T_varchar, bat.Vecs[0].GetType().Oid)
	require.Equal(t, "123.45", bat.Vecs[0].GetStringAt(0))
	require.Equal(t, "-1.50", bat.Vecs[0].GetStringAt(1))
	require.True(t, bat.Vecs[0].IsNull(2))

	record.Release()
	values.Release()
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestConvertMaterializesStagedDictionaryDecimalText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8, ValueType: decimalType,
	}

	// Dictionary payload [123.45, -1.50] selected as [0, 1, 0] with the third
	// index NULL.
	builder := array.NewDecimal128Builder(alloc, decimalType)
	builder.AppendValues([]decimal128.Num{
		decimal128.FromBigInt(big.NewInt(12345)),
		decimal128.FromBigInt(big.NewInt(-150)),
	}, nil)
	dictValues := builder.NewArray()
	builder.Release()
	indexBuilder := array.NewInt8Builder(alloc)
	indexBuilder.AppendValues([]int8{0, 1, 0}, []bool{true, true, false})
	indices := indexBuilder.NewArray()
	indexBuilder.Release()
	dictionary := array.NewDictionaryArray(dictionaryType, indices, dictValues)
	indices.Release()
	dictValues.Release()
	dictSchema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: dictionaryType, Nullable: true}}, nil)

	// The plain array carrying the same logical rows.
	plainBuilder := array.NewDecimal128Builder(alloc, decimalType)
	plainBuilder.AppendValues([]decimal128.Num{
		decimal128.FromBigInt(big.NewInt(12345)),
		decimal128.FromBigInt(big.NewInt(-150)),
		{},
	}, []bool{true, true, false})
	plainValues := plainBuilder.NewArray()
	plainBuilder.Release()
	plainSchema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: decimalType, Nullable: true}}, nil)

	mp := mpool.MustNewZero()
	targets := []TargetColumn{stagedTextTarget("d")}

	dictRecord := array.NewRecordBatch(dictSchema, []arrow.Array{dictionary}, 3)
	dictPlan, err := BindLoad(context.Background(), dictSchema, targets, MatchByName)
	require.NoError(t, err)
	dictBat, _, err := dictPlan.Convert(context.Background(), dictRecord, mp, ConvertOptions{})
	require.NoError(t, err)

	plainRecord := array.NewRecordBatch(plainSchema, []arrow.Array{plainValues}, 3)
	plainPlan, err := BindLoad(context.Background(), plainSchema, targets, MatchByName)
	require.NoError(t, err)
	plainBat, _, err := plainPlan.Convert(context.Background(), plainRecord, mp, ConvertOptions{})
	require.NoError(t, err)

	// Encoding equivalence: a dictionary encoding materializes the identical
	// rows the plain array produces, values and NULLs alike.
	for row := uint64(0); row < 3; row++ {
		require.Equal(t, plainBat.Vecs[0].IsNull(row), dictBat.Vecs[0].IsNull(row),
			"row %d null handling", row)
		if !plainBat.Vecs[0].IsNull(row) {
			require.Equal(t, plainBat.Vecs[0].GetStringAt(int(row)), dictBat.Vecs[0].GetStringAt(int(row)),
				"row %d stored value", row)
		}
	}
	require.Equal(t, "123.45", dictBat.Vecs[0].GetStringAt(0))
	require.Equal(t, "-1.50", dictBat.Vecs[0].GetStringAt(1))

	dictRecord.Release()
	plainRecord.Release()
	dictionary.Release()
	plainValues.Release()
	dictBat.Clean(mp)
	plainBat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestMaxOutputRowsAccountsRenderedStagedText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	decimalType := &arrow.Decimal256Type{Precision: 76, Scale: 0}
	// 10^75 renders 76 digits.
	coeff := new(big.Int).Exp(big.NewInt(10), big.NewInt(75), nil)
	builder := array.NewDecimal256Builder(alloc, decimalType)
	builder.AppendValues([]decimal256.Num{
		decimal256.FromBigInt(coeff),
		decimal256.FromBigInt(coeff),
	}, nil)
	values := builder.NewArray()
	builder.Release()

	// The same payload once as a plain array and once under a dictionary
	// encoding: both drive the rendered-text budget.
	dictionaryType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8, ValueType: decimalType,
	}
	indexBuilder := array.NewInt8Builder(alloc)
	indexBuilder.AppendValues([]int8{0, 0}, nil)
	indices := indexBuilder.NewArray()
	indexBuilder.Release()
	dictionary := array.NewDictionaryArray(dictionaryType, indices, values)
	indices.Release()

	// A rendered DECIMAL(76) row costs its varchar descriptor plus at most
	// precision+3 payload bytes; a budget fitting two descriptors alone must
	// admit only one staged row.
	rowBytes := uint64(types.VarlenaSize + 76 + 3)
	for _, tc := range []struct {
		name   string
		schema *arrow.Schema
		values arrow.Array
	}{
		{name: "plain", schema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: decimalType}}, nil), values: values},
		{name: "dictionary", schema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: dictionaryType, Nullable: true}}, nil), values: dictionary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := array.NewRecordBatch(tc.schema, []arrow.Array{tc.values}, 2)
			plan, err := BindLoad(context.Background(), tc.schema, []TargetColumn{stagedTextTarget("v")}, MatchByName)
			require.NoError(t, err)

			rows, err := plan.MaxOutputRows(context.Background(), record, 0, 2, rowBytes)
			require.NoError(t, err)
			require.Equal(t, 1, rows,
				"the rendered payload counts toward the byte budget")
			rows, err = plan.MaxOutputRows(context.Background(), record, 0, 2, 2*rowBytes)
			require.NoError(t, err)
			require.Equal(t, 2, rows,
				"a budget for both rendered rows admits both")
			rows, err = plan.MaxOutputRows(context.Background(), record, 0, 2, 1)
			require.NoError(t, err)
			require.Equal(t, 1, rows, "one oversized row still makes progress")

			record.Release()
		})
	}

	dictionary.Release()
	values.Release()
	alloc.AssertSize(t, 0)
}

func TestConvertMaterializesStagedScalarText(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(alloc, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{-42, 0}, nil)
	builder.Field(1).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.5, -0.25}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	targets := []TargetColumn{
		stagedTextTarget("i"), stagedTextTarget("b"), stagedTextTarget("f"),
	}
	plan, err := BindLoad(context.Background(), schema, targets, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, "-42", bat.Vecs[0].GetStringAt(0))
	require.Equal(t, "0", bat.Vecs[0].GetStringAt(1))
	require.Equal(t, "1", bat.Vecs[1].GetStringAt(0))
	require.Equal(t, "0", bat.Vecs[1].GetStringAt(1))
	require.Equal(t, "1.5", bat.Vecs[2].GetStringAt(0))
	require.Equal(t, "-0.25", bat.Vecs[2].GetStringAt(1))

	record.Release()
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}
