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
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBindByNameAndPosition(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "B", Type: arrow.PrimitiveTypes.Int64},
		{Name: "a", Type: arrow.BinaryTypes.String},
	}, nil)
	targets := []TargetColumn{
		{Name: "A", Type: types.T_varchar.ToType(), MOIndex: 1, AttrName: "a_out"},
		{Name: "b", Type: types.T_int64.ToType(), MOIndex: 0, AttrName: "b_out"},
	}
	plan, err := Bind(context.Background(), schema, targets, MatchByName)
	require.NoError(t, err)
	require.Equal(t, []string{"b_out", "a_out"}, plan.attrs)
	require.Equal(t, 0, plan.columns[0].source)
	require.Equal(t, 1, plan.columns[1].source)

	positionTargets := []TargetColumn{
		{Name: "first", Type: types.T_int64.ToType()},
		{Name: "second", Type: types.T_varchar.ToType()},
	}
	positionPlan, err := Bind(context.Background(), schema, positionTargets, MatchByPosition)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, positionPlan.attrs)
}

func TestPlanFingerprintCoversSchemaMetadataAndConversionContract(t *testing.T) {
	ctx := context.Background()
	fieldMetadata := arrow.NewMetadata([]string{"field-key"}, []string{"field-value"})
	schemaMetadata := arrow.NewMetadata([]string{"schema-key"}, []string{"schema-value"})
	baseSchema := arrow.NewSchema([]arrow.Field{{
		Name: "v", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldMetadata,
	}}, &schemaMetadata)
	baseTarget := []TargetColumn{{
		Name: "v", AttrName: "v", Type: types.New(types.T_varchar, 100, 0), NotNull: false,
	}}
	base, err := Bind(ctx, baseSchema, baseTarget, MatchByName)
	require.NoError(t, err)
	require.NotEqual(t, [32]byte{}, base.Fingerprint())

	// Metadata ordering is not a semantic schema change.
	reorderedMetadata := arrow.NewMetadata(
		[]string{"second", "schema-key"}, []string{"two", "schema-value"},
	)
	reorderedSchema := arrow.NewSchema([]arrow.Field{{
		Name: "v", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldMetadata,
	}}, &reorderedMetadata)
	reorderedBaseMetadata := arrow.NewMetadata(
		[]string{"schema-key", "second"}, []string{"schema-value", "two"},
	)
	reorderedBaseSchema := arrow.NewSchema([]arrow.Field{{
		Name: "v", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldMetadata,
	}}, &reorderedBaseMetadata)
	first, err := Bind(ctx, reorderedSchema, baseTarget, MatchByName)
	require.NoError(t, err)
	second, err := Bind(ctx, reorderedBaseSchema, baseTarget, MatchByName)
	require.NoError(t, err)
	require.Equal(t, first.Fingerprint(), second.Fingerprint())

	changedFieldMetadata := arrow.NewMetadata([]string{"field-key"}, []string{"changed"})
	changedSchema := arrow.NewSchema([]arrow.Field{{
		Name: "v", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: changedFieldMetadata,
	}}, &schemaMetadata)
	changed, err := Bind(ctx, changedSchema, baseTarget, MatchByName)
	require.NoError(t, err)
	require.NotEqual(t, base.Fingerprint(), changed.Fingerprint())

	widerTarget := append([]TargetColumn(nil), baseTarget...)
	widerTarget[0].Type = types.New(types.T_varchar, 101, 0)
	wider, err := Bind(ctx, baseSchema, widerTarget, MatchByName)
	require.NoError(t, err)
	require.NotEqual(t, base.Fingerprint(), wider.Fingerprint())

	position, err := Bind(ctx, baseSchema, baseTarget, MatchByPosition)
	require.NoError(t, err)
	require.NotEqual(t, base.Fingerprint(), position.Fingerprint())
}

func TestBindRejectsAmbiguousMissingAndUnsupported(t *testing.T) {
	ctx := context.Background()
	_, err := Bind(ctx, arrow.NewSchema([]arrow.Field{
		{Name: "A", Type: arrow.PrimitiveTypes.Int64},
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	}, nil), []TargetColumn{
		{Name: "a", Type: types.T_int64.ToType()},
		{Name: "x", Type: types.T_int64.ToType()},
	}, MatchByName)
	require.ErrorContains(t, err, "ambiguous")

	_, err = Bind(ctx, arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]TargetColumn{{Name: "missing", Type: types.T_int64.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "missing")

	_, err = Bind(ctx, arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]TargetColumn{{Name: "a", Type: types.T_int32.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "no exact long-term conversion")

	_, err = Bind(ctx, arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]TargetColumn{{Name: "a", Type: types.T_int64.ToType()}, {Name: "b", Type: types.T_int64.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "target has 2")
}

func TestBindBoundsTotalFieldsAndNestingBeforeFingerprint(t *testing.T) {
	allowed := arrow.DataType(arrow.PrimitiveTypes.Int64)
	for range MaxNestingDepth - 1 {
		allowed = arrow.ListOf(allowed)
	}
	_, err := Bind(context.Background(), arrow.NewSchema([]arrow.Field{{Name: "v", Type: allowed}}, nil),
		[]TargetColumn{{Name: "v", Type: types.T_json.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "no exact long-term conversion",
		"a schema at the limit must reach ordinary type validation")

	tooDeep := arrow.ListOf(allowed)
	_, err = Bind(context.Background(), arrow.NewSchema([]arrow.Field{{Name: "v", Type: tooDeep}}, nil),
		[]TargetColumn{{Name: "v", Type: types.T_json.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "nesting depth exceeds")

	children := make([]arrow.Field, MaxFields)
	for index := range children {
		children[index] = arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int8}
	}
	tooMany := arrow.StructOf(children...)
	_, err = Bind(context.Background(), arrow.NewSchema([]arrow.Field{{Name: "v", Type: tooMany}}, nil),
		[]TargetColumn{{Name: "v", Type: types.T_json.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "total field count exceeds")
}

func TestFixedBorrowValidityLifetimeAndWindow(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewInt64Builder(alloc)
	builder.AppendValues([]int64{10, 20, 30, 40}, []bool{true, false, true, true})
	base := builder.NewArray()
	builder.Release()
	sliced := array.NewSlice(base, 1, 4)
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{sliced}, 3)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "v", Type: types.T_int64.ToType()}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(24), stats.BorrowedPayloadBytes)
	require.True(t, bat.Vecs[0].HasBorrowedBacking())
	require.True(t, bat.Vecs[0].GetNulls().HasBorrowedValidity())
	expectedData := sliced.Data().Buffers()[1].Bytes()[sliced.Data().Offset()*8:]
	require.Equal(t,
		uintptr(unsafe.Pointer(unsafe.SliceData(expectedData))),
		uintptr(unsafe.Pointer(unsafe.SliceData(bat.Vecs[0].GetData()))),
	)
	require.Equal(t, []int64{20, 30, 40}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
	require.True(t, bat.Vecs[0].IsNull(0))
	require.True(t, bat.Vecs[0].GetNulls().HasBorrowedValidity(), "Contains must preserve the validity view")

	window, err := bat.Vecs[0].Window(1, 3)
	require.NoError(t, err)
	record.Release()
	sliced.Release()
	base.Release()
	bat.Clean(mp)
	require.Equal(t, []int64{30, 40}, vector.MustFixedColNoTypeCheck[int64](window))
	require.False(t, window.IsNull(0))
	window.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestExactFixedWidthTypeMatrixBorrowsPayload(t *testing.T) {
	tests := []struct {
		name   string
		source arrow.DataType
		target types.Type
		append func(array.Builder)
	}{
		{name: "int8", source: arrow.PrimitiveTypes.Int8, target: types.T_int8.ToType(), append: func(b array.Builder) { b.(*array.Int8Builder).Append(-8) }},
		{name: "int16", source: arrow.PrimitiveTypes.Int16, target: types.T_int16.ToType(), append: func(b array.Builder) { b.(*array.Int16Builder).Append(-16) }},
		{name: "int32", source: arrow.PrimitiveTypes.Int32, target: types.T_int32.ToType(), append: func(b array.Builder) { b.(*array.Int32Builder).Append(-32) }},
		{name: "int64", source: arrow.PrimitiveTypes.Int64, target: types.T_int64.ToType(), append: func(b array.Builder) { b.(*array.Int64Builder).Append(-64) }},
		{name: "uint8", source: arrow.PrimitiveTypes.Uint8, target: types.T_uint8.ToType(), append: func(b array.Builder) { b.(*array.Uint8Builder).Append(8) }},
		{name: "uint16", source: arrow.PrimitiveTypes.Uint16, target: types.T_uint16.ToType(), append: func(b array.Builder) { b.(*array.Uint16Builder).Append(16) }},
		{name: "uint32", source: arrow.PrimitiveTypes.Uint32, target: types.T_uint32.ToType(), append: func(b array.Builder) { b.(*array.Uint32Builder).Append(32) }},
		{name: "uint64", source: arrow.PrimitiveTypes.Uint64, target: types.T_uint64.ToType(), append: func(b array.Builder) { b.(*array.Uint64Builder).Append(64) }},
		{name: "float32", source: arrow.PrimitiveTypes.Float32, target: types.T_float32.ToType(), append: func(b array.Builder) { b.(*array.Float32Builder).Append(3.25) }},
		{name: "float64", source: arrow.PrimitiveTypes.Float64, target: types.T_float64.ToType(), append: func(b array.Builder) { b.(*array.Float64Builder).Append(6.5) }},
		{
			name:   "decimal128",
			source: &arrow.Decimal128Type{Precision: 18, Scale: 2},
			target: types.New(types.T_decimal128, 18, 2),
			append: func(b array.Builder) { b.(*array.Decimal128Builder).Append(decimal128.FromI64(-12345)) },
		},
		{
			name:   "time64 microsecond",
			source: &arrow.Time64Type{Unit: arrow.Microsecond},
			target: types.New(types.T_time, 0, 6),
			append: func(b array.Builder) { b.(*array.Time64Builder).Append(12_345_678) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			builder := array.NewBuilder(alloc, test.source)
			test.append(builder)
			values := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: test.source}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "v", Type: test.target}}, MatchByName)
			require.NoError(t, err)
			mp := mpool.MustNewZero()

			bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
			require.NoError(t, err)
			require.True(t, bat.Vecs[0].HasBorrowedBacking())
			require.Equal(t, int64(test.target.TypeSize()), stats.BorrowedPayloadBytes)
			require.Zero(t, stats.MaterializedPayloadBytes)
			sourceData := values.Data().Buffers()[1].Bytes()
			require.Equal(t,
				uintptr(unsafe.Pointer(unsafe.SliceData(sourceData))),
				uintptr(unsafe.Pointer(unsafe.SliceData(bat.Vecs[0].GetData()))),
			)

			record.Release()
			values.Release()
			bat.Clean(mp)
			require.Zero(t, mp.CurrNB())
			alloc.AssertSize(t, 0)
		})
	}
}

func TestDecimalExactLayoutContractRejectsRescale(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "d", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2},
	}}, nil)
	_, err := Bind(context.Background(), schema, []TargetColumn{{
		Name: "d", Type: types.New(types.T_decimal128, 18, 3),
	}}, MatchByName)
	require.ErrorContains(t, err, "no exact long-term conversion")
}

func TestFixedExplicitCOW(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewInt32Builder(alloc)
	builder.AppendValues([]int32{1, 2}, nil)
	arr := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int32}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{arr}, 2)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "v", Type: types.T_int32.ToType()}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.NoError(t, bat.Vecs[0].MaterializeOwned(mp))
	require.False(t, bat.Vecs[0].HasBorrowedBacking())
	vector.SetFixedAtWithTypeCheck(bat.Vecs[0], 0, int32(9))
	require.Equal(t, []int32{9, 2}, vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0]))
	record.Release()
	arr.Release()
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestMaterializedConversionUsesStatementAllocationSelection(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account, mpool.AllocationOwnerExternal, 20, 21, 22, 23,
	)
	require.NoError(t, err)
	allocator := memory.NewGoAllocator()
	builder := array.NewBooleanBuilder(allocator)
	builder.AppendValues([]bool{true, false, true}, []bool{true, false, true})
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{
		Name: "v", Type: types.T_bool.ToType(),
	}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{Allocation: selection})
	require.NoError(t, err)
	require.Same(t, selection, bat.Vecs[0].AllocationAccountSelection())
	require.Positive(t, account.Snapshot().Used)
	record.Release()
	values.Release()
	bat.Clean(mp)
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestVarlenBorrowLongInlineShortAndLifetime(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(alloc)
	long := "this payload is definitely longer than twenty three bytes"
	builder.AppendValues([]string{"tiny", long, "also tiny"}, []bool{true, true, false})
	arr := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{arr}, 3)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "s", Type: types.T_varchar.ToType()}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{MaxPinAmplification: 100})
	require.NoError(t, err)
	require.Equal(t, int64(len(long)), stats.BorrowedPayloadBytes)
	require.True(t, bat.Vecs[0].HasBorrowedBacking())
	descriptors, area := vector.MustVarlenaRawData(bat.Vecs[0])
	require.True(t, descriptors[0].IsSmall())
	require.False(t, descriptors[1].IsSmall())
	require.Equal(t, uintptr(unsafe.Pointer(unsafe.SliceData(arr.(*array.String).ValueBytes()))),
		uintptr(unsafe.Pointer(unsafe.SliceData(area))))
	require.Equal(t, "tiny", string(bat.Vecs[0].GetBytesAt(0)))
	require.Equal(t, long, string(bat.Vecs[0].GetBytesAt(1)))
	require.True(t, bat.Vecs[0].IsNull(2))

	record.Release()
	arr.Release()
	require.Equal(t, long, string(bat.Vecs[0].GetBytesAt(1)), "vector lease must outlive Arrow record")
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestVarlenPinAmplificationMaterializes(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(alloc)
	long := "a single long payload longer than twenty three bytes"
	builder.Append(long)
	arr := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "s", Type: types.T_varchar.ToType()}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{MaxPinAmplification: 0.0001})
	require.NoError(t, err)
	require.False(t, bat.Vecs[0].HasBorrowedBacking())
	require.Zero(t, stats.BorrowedPayloadBytes)
	require.Equal(t, int64(len(long)), stats.MaterializedPayloadBytes)
	require.Equal(t, long, string(bat.Vecs[0].GetBytesAt(0)))
	record.Release()
	arr.Release()
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestVarlenSliceOffsetsAreNormalized(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	one := []byte("discarded-prefix-that-is-long")
	two := []byte("the-kept-value-is-longer-than-inline")
	three := []byte("another-kept-value-longer-than-inline")
	builder.AppendValues([][]byte{one, two, three}, nil)
	base := builder.NewArray()
	builder.Release()
	sliced := array.NewSlice(base, 1, 3)
	schema := arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.BinaryTypes.Binary}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{sliced}, 2)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "b", Type: types.T_varbinary.ToType()}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{MaxPinAmplification: 100})
	require.NoError(t, err)
	require.Equal(t, two, bat.Vecs[0].GetBytesAt(0))
	require.Equal(t, three, bat.Vecs[0].GetBytesAt(1))
	desc, _ := vector.MustVarlenaRawData(bat.Vecs[0])
	offset, _ := desc[0].OffsetLen()
	require.Zero(t, offset)
	record.Release()
	sliced.Release()
	base.Release()
	bat.Clean(mp)
	alloc.AssertSize(t, 0)
}

func TestVarlenRejectsInvalidUTF8AndLength(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  string
		target types.Type
	}{
		{name: "utf8", value: string([]byte{0xff}), target: types.T_varchar.ToType()},
		{name: "length", value: "abcd", target: types.New(types.T_varchar, 3, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			builder := array.NewStringBuilder(alloc)
			builder.Append(tc.value)
			arr := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "s", Type: tc.target}}, MatchByName)
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			_, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
			require.Error(t, err)
			require.Equal(t, int64(0), mp.CurrNB())
			record.Release()
			arr.Release()
			alloc.AssertSize(t, 0)
		})
	}
}

func TestVarlenIgnoresUnobservableNullPayload(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewStringBuilder(alloc)
	builder.AppendValues(
		[]string{string([]byte{0xff}), "visible"},
		[]bool{false, true},
	)
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "s", Type: arrow.BinaryTypes.String, Nullable: true,
	}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 2)
	plan, err := Bind(context.Background(), schema, []TargetColumn{{
		Name: "s", Type: types.New(types.T_varchar, 7, 0),
	}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	converted, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.True(t, converted.Vecs[0].IsNull(0))
	require.Equal(t, "visible", string(converted.Vecs[0].GetBytesAt(1)))

	converted.Clean(mp)
	record.Release()
	values.Release()
	require.Zero(t, mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestMaterializedBoolAndDates(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	boolBuilder := array.NewBooleanBuilder(alloc)
	boolBuilder.AppendValues([]bool{true, false}, []bool{true, false})
	bools := boolBuilder.NewArray()
	boolBuilder.Release()
	date32Builder := array.NewDate32Builder(alloc)
	date32Builder.AppendValues([]arrow.Date32{0, 1}, nil)
	date32s := date32Builder.NewArray()
	date32Builder.Release()
	date64Builder := array.NewDate64Builder(alloc)
	date64Builder.AppendValues([]arrow.Date64{0, 86_400_000}, nil)
	date64s := date64Builder.NewArray()
	date64Builder.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "d32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "d64", Type: arrow.FixedWidthTypes.Date64},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{bools, date32s, date64s}, 2)
	plan, err := Bind(context.Background(), schema, []TargetColumn{
		{Name: "b", Type: types.T_bool.ToType()},
		{Name: "d32", Type: types.T_date.ToType()},
		{Name: "d64", Type: types.T_date.ToType()},
	}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Zero(t, stats.BorrowedPayloadBytes)
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](bat.Vecs[0]))
	require.True(t, bat.Vecs[0].IsNull(1))
	expectedDates := []types.Date{
		types.DaysFromUnixEpochToDate(0),
		types.DaysFromUnixEpochToDate(1),
	}
	require.Equal(t, expectedDates, vector.MustFixedColNoTypeCheck[types.Date](bat.Vecs[1]))
	require.Equal(t, expectedDates, vector.MustFixedColNoTypeCheck[types.Date](bat.Vecs[2]))
	bat.Clean(mp)
	record.Release()
	bools.Release()
	date32s.Release()
	date64s.Release()
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestTemporalValidationAndTime64Borrow(t *testing.T) {
	t.Run("date64 fractional day", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		builder := array.NewDate64Builder(alloc)
		builder.Append(1)
		arr := builder.NewArray()
		builder.Release()
		schema := arrow.NewSchema([]arrow.Field{{Name: "d", Type: arrow.FixedWidthTypes.Date64}}, nil)
		record := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
		plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "d", Type: types.T_date.ToType()}}, MatchByName)
		require.NoError(t, err)
		mp := mpool.MustNewZero()
		_, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
		require.ErrorContains(t, err, "integral representable day")
		require.Equal(t, int64(0), mp.CurrNB())
		record.Release()
		arr.Release()
		alloc.AssertSize(t, 0)
	})

	t.Run("time64 range", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		timeType := &arrow.Time64Type{Unit: arrow.Microsecond}
		builder := array.NewTime64Builder(alloc, timeType)
		builder.Append(arrow.Time64(24 * 60 * 60 * 1_000_000))
		arr := builder.NewArray()
		builder.Release()
		schema := arrow.NewSchema([]arrow.Field{{Name: "t", Type: timeType}}, nil)
		target := types.T_time.ToType()
		target.Scale = 6
		plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "t", Type: target}}, MatchByName)
		require.NoError(t, err)
		mp := mpool.MustNewZero()
		record := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
		_, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
		require.ErrorContains(t, err, "outside [0,24h)")
		require.Equal(t, int64(0), mp.CurrNB())
		record.Release()
		arr.Release()
		alloc.AssertSize(t, 0)
	})
}

func TestMaterializedWidenTimeDateTimeAndNull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	intBuilder := array.NewInt8Builder(alloc)
	intBuilder.AppendValues([]int8{-1, 2}, nil)
	ints := intBuilder.NewArray()
	intBuilder.Release()
	timeType := &arrow.Time32Type{Unit: arrow.Millisecond}
	timeBuilder := array.NewTime32Builder(alloc, timeType)
	timeBuilder.AppendValues([]arrow.Time32{1_000, 1_234}, nil)
	times := timeBuilder.NewArray()
	timeBuilder.Release()
	dateBuilder := array.NewDate32Builder(alloc)
	dateBuilder.AppendValues([]arrow.Date32{0, 1}, nil)
	dates := dateBuilder.NewArray()
	dateBuilder.Release()
	nulls := array.NewNull(2)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int8},
		{Name: "t", Type: timeType},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
		{Name: "n", Type: arrow.Null, Nullable: true},
	}, nil)
	timeTarget := types.T_time.ToType()
	timeTarget.Scale = 3
	plan, err := Bind(context.Background(), schema, []TargetColumn{
		{Name: "i", Type: types.T_int64.ToType()},
		{Name: "t", Type: timeTarget},
		{Name: "d", Type: types.T_datetime.ToType()},
		{Name: "n", Type: types.T_varchar.ToType()},
	}, MatchByName)
	require.NoError(t, err)
	record := array.NewRecordBatch(schema, []arrow.Array{ints, times, dates, nulls}, 2)
	mp := mpool.MustNewZero()

	bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, []int64{-1, 2}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
	require.Equal(t, []types.Time{types.Time(1_000_000), types.Time(1_234_000)},
		vector.MustFixedColNoTypeCheck[types.Time](bat.Vecs[1]))
	require.Equal(t, []string{"1970-01-01 00:00:00", "1970-01-02 00:00:00"}, []string{
		vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[2])[0].String(),
		vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[2])[1].String(),
	})
	require.True(t, bat.Vecs[3].IsNull(0))
	require.True(t, bat.Vecs[3].IsNull(1))

	bat.Clean(mp)
	record.Release()
	ints.Release()
	times.Release()
	dates.Release()
	nulls.Release()
	alloc.AssertSize(t, 0)
}

func TestCheckedWideningTypeMatrix(t *testing.T) {
	tests := []struct {
		source arrow.DataType
		target types.Type
	}{
		{source: arrow.PrimitiveTypes.Int8, target: types.T_int16.ToType()},
		{source: arrow.PrimitiveTypes.Int8, target: types.T_int32.ToType()},
		{source: arrow.PrimitiveTypes.Int8, target: types.T_int64.ToType()},
		{source: arrow.PrimitiveTypes.Int16, target: types.T_int32.ToType()},
		{source: arrow.PrimitiveTypes.Int16, target: types.T_int64.ToType()},
		{source: arrow.PrimitiveTypes.Int32, target: types.T_int64.ToType()},
		{source: arrow.PrimitiveTypes.Uint8, target: types.T_uint16.ToType()},
		{source: arrow.PrimitiveTypes.Uint8, target: types.T_uint32.ToType()},
		{source: arrow.PrimitiveTypes.Uint8, target: types.T_uint64.ToType()},
		{source: arrow.PrimitiveTypes.Uint16, target: types.T_uint32.ToType()},
		{source: arrow.PrimitiveTypes.Uint16, target: types.T_uint64.ToType()},
		{source: arrow.PrimitiveTypes.Uint32, target: types.T_uint64.ToType()},
		{source: arrow.PrimitiveTypes.Float32, target: types.T_float64.ToType()},
	}

	for _, test := range tests {
		name := test.source.String() + " to " + test.target.String()
		t.Run(name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			builder := array.NewBuilder(alloc, test.source)
			switch typed := builder.(type) {
			case *array.Int8Builder:
				typed.Append(-7)
			case *array.Int16Builder:
				typed.Append(-7)
			case *array.Int32Builder:
				typed.Append(-7)
			case *array.Uint8Builder:
				typed.Append(7)
			case *array.Uint16Builder:
				typed.Append(7)
			case *array.Uint32Builder:
				typed.Append(7)
			case *array.Float32Builder:
				typed.Append(1.25)
			default:
				t.Fatalf("missing source builder %T", builder)
			}
			values := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: test.source}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "v", Type: test.target}}, MatchByName)
			require.NoError(t, err)
			mp := mpool.MustNewZero()

			bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
			require.NoError(t, err)
			require.False(t, bat.Vecs[0].HasBorrowedBacking())
			require.Equal(t, int64(test.target.TypeSize()), stats.MaterializedPayloadBytes)
			switch test.target.Oid {
			case types.T_int16:
				require.Equal(t, []int16{-7}, vector.MustFixedColNoTypeCheck[int16](bat.Vecs[0]))
			case types.T_int32:
				require.Equal(t, []int32{-7}, vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0]))
			case types.T_int64:
				require.Equal(t, []int64{-7}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
			case types.T_uint16:
				require.Equal(t, []uint16{7}, vector.MustFixedColNoTypeCheck[uint16](bat.Vecs[0]))
			case types.T_uint32:
				require.Equal(t, []uint32{7}, vector.MustFixedColNoTypeCheck[uint32](bat.Vecs[0]))
			case types.T_uint64:
				require.Equal(t, []uint64{7}, vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[0]))
			case types.T_float64:
				require.Equal(t, []float64{1.25}, vector.MustFixedColNoTypeCheck[float64](bat.Vecs[0]))
			}

			bat.Clean(mp)
			record.Release()
			values.Release()
			require.Zero(t, mp.CurrNB())
			alloc.AssertSize(t, 0)
		})
	}
}

func TestMaterializedTimeRejectsPrecisionLoss(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	timeType := &arrow.Time32Type{Unit: arrow.Millisecond}
	builder := array.NewTime32Builder(alloc, timeType)
	builder.Append(1)
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "t", Type: timeType}}, nil)
	target := types.T_time.ToType()
	target.Scale = 2
	plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "t", Type: target}}, MatchByName)
	require.NoError(t, err)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	mp := mpool.MustNewZero()
	_, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.ErrorContains(t, err, "precision")
	require.Zero(t, mp.CurrNB())
	record.Release()
	values.Release()
	alloc.AssertSize(t, 0)
}

func TestTimestampTimezoneSemantics(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		timezone   string
		target     types.Type
		input      arrow.Timestamp
		expected   string
		timestampS int64
	}{
		{name: "wall clock to timestamp", timezone: "", target: types.T_timestamp.ToType(), input: arrow.Timestamp((8*60*60 + 1) * 1_000_000), expected: "1970-01-01 08:00:01.000000", timestampS: 1},
		{name: "wall clock to datetime", timezone: "", target: types.T_datetime.ToType(), expected: "1970-01-01 00:00:00"},
		{name: "instant to datetime", timezone: "UTC", target: types.T_datetime.ToType(), expected: "1970-01-01 08:00:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tc.timezone}
			builder := array.NewTimestampBuilder(alloc, timestampType)
			builder.Append(tc.input)
			arr := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: timestampType}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "ts", Type: tc.target}}, MatchByName)
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{Location: location})
			require.NoError(t, err)
			if tc.target.Oid == types.T_timestamp {
				value := vector.MustFixedColNoTypeCheck[types.Timestamp](bat.Vecs[0])[0]
				require.Equal(t, tc.timestampS, value.Unix())
				require.Contains(t, value.String2(location, 6), tc.expected)
			} else {
				value := vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[0])[0]
				require.Equal(t, tc.expected, value.String())
			}
			bat.Clean(mp)
			record.Release()
			arr.Release()
			require.Equal(t, int64(0), mp.CurrNB())
			alloc.AssertSize(t, 0)
		})
	}
}

func TestTimestampUnitAndPrecisionMatrix(t *testing.T) {
	for _, test := range []struct {
		name  string
		unit  arrow.TimeUnit
		value arrow.Timestamp
	}{
		{name: "second", unit: arrow.Second, value: 2},
		{name: "millisecond", unit: arrow.Millisecond, value: 2_000},
		{name: "microsecond", unit: arrow.Microsecond, value: 2_000_000},
		{name: "nanosecond exact", unit: arrow.Nanosecond, value: 2_000_000_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			timestampType := &arrow.TimestampType{Unit: test.unit}
			builder := array.NewTimestampBuilder(alloc, timestampType)
			builder.Append(test.value)
			values := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: timestampType}}, nil)
			target := types.New(types.T_datetime, 0, 6)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "ts", Type: target}}, MatchByName)
			require.NoError(t, err)
			record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
			mp := mpool.MustNewZero()

			bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
			require.NoError(t, err)
			require.Equal(t, "1970-01-01 00:00:02", vector.MustFixedColNoTypeCheck[types.Datetime](bat.Vecs[0])[0].String())

			bat.Clean(mp)
			record.Release()
			values.Release()
			alloc.AssertSize(t, 0)
		})
	}

	for _, test := range []struct {
		name    string
		unit    arrow.TimeUnit
		value   arrow.Timestamp
		scale   int32
		valid   bool
		errText string
	}{
		{name: "nanosecond precision loss", unit: arrow.Nanosecond, value: 1, scale: 6, valid: true, errText: "out of range"},
		{name: "target precision loss", unit: arrow.Millisecond, value: 1, scale: 2, valid: true, errText: "precision"},
		{name: "overflow", unit: arrow.Second, value: arrow.Timestamp(^uint64(0) >> 1), scale: 6, valid: true, errText: "out of range"},
		{name: "null payload ignored", unit: arrow.Second, value: arrow.Timestamp(^uint64(0) >> 1), scale: 6, valid: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			timestampType := &arrow.TimestampType{Unit: test.unit}
			builder := array.NewTimestampBuilder(alloc, timestampType)
			builder.AppendValues([]arrow.Timestamp{test.value}, []bool{test.valid})
			values := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: timestampType, Nullable: true}}, nil)
			target := types.New(types.T_timestamp, 0, test.scale)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{Name: "ts", Type: target}}, MatchByName)
			require.NoError(t, err)
			record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
			mp := mpool.MustNewZero()

			bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
			if test.errText != "" {
				require.ErrorContains(t, err, test.errText)
				require.Nil(t, bat)
			} else {
				require.NoError(t, err)
				require.True(t, bat.Vecs[0].IsNull(0))
				bat.Clean(mp)
			}
			require.Zero(t, mp.CurrNB())
			record.Release()
			values.Release()
			alloc.AssertSize(t, 0)
		})
	}

	invalidScale := types.New(types.T_timestamp, 0, 7)
	_, err := Bind(context.Background(), arrow.NewSchema([]arrow.Field{{
		Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond},
	}}, nil), []TargetColumn{{Name: "ts", Type: invalidScale}}, MatchByName)
	require.ErrorContains(t, err, "no exact long-term conversion")
}

func TestDictionaryGatherMaterializesAndPropagatesLogicalNulls(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	indicesBuilder := array.NewInt8Builder(alloc)
	indicesBuilder.AppendValues([]int8{0, 1, 0, 2}, []bool{true, true, false, true})
	indices := indicesBuilder.NewArray()
	indicesBuilder.Release()
	valuesBuilder := array.NewStringBuilder(alloc)
	long := "dictionary payload longer than MatrixOne inline varlena"
	valuesBuilder.AppendValues([]string{long, "ignored", "tiny"}, []bool{true, false, true})
	values := valuesBuilder.NewArray()
	valuesBuilder.Release()
	dictionaryType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	dictionary := array.NewDictionaryArray(dictionaryType, indices, values)
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: dictionaryType, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{dictionary}, 4)
	plan, err := Bind(context.Background(), schema,
		[]TargetColumn{{Name: "s", Type: types.T_varchar.ToType()}}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.False(t, bat.Vecs[0].HasBorrowedBacking(), "dictionary indices must be gathered")
	require.Equal(t, int64(len(long)+len("tiny")), stats.MaterializedPayloadBytes)
	require.Equal(t, long, string(bat.Vecs[0].GetBytesAt(0)))
	require.True(t, bat.Vecs[0].IsNull(1), "a null dictionary value is a logical null")
	require.True(t, bat.Vecs[0].IsNull(2), "a null dictionary index is a logical null")
	require.Equal(t, "tiny", string(bat.Vecs[0].GetBytesAt(3)))

	notNullPlan, err := Bind(context.Background(), schema,
		[]TargetColumn{{Name: "s", Type: types.T_varchar.ToType(), NotNull: true}}, MatchByName)
	require.NoError(t, err)
	_, _, err = notNullPlan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.ErrorContains(t, err, "NOT NULL")

	bat.Clean(mp)
	record.Release()
	dictionary.Release()
	indices.Release()
	values.Release()
	require.Equal(t, int64(0), mp.CurrNB())
	alloc.AssertSize(t, 0)
}

func TestDictionaryFixedWidthAndTemporalGather(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	indicesBuilder := array.NewUint16Builder(alloc)
	indicesBuilder.AppendValues([]uint16{1, 0}, nil)
	indices := indicesBuilder.NewArray()
	indicesBuilder.Release()
	intBuilder := array.NewInt64Builder(alloc)
	intBuilder.AppendValues([]int64{11, 22}, nil)
	intValues := intBuilder.NewArray()
	intBuilder.Release()
	dateBuilder := array.NewDate64Builder(alloc)
	dateBuilder.AppendValues([]arrow.Date64{0, 86_400_000}, nil)
	dateValues := dateBuilder.NewArray()
	dateBuilder.Release()
	intType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.PrimitiveTypes.Int64}
	dateType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.FixedWidthTypes.Date64}
	intDictionary := array.NewDictionaryArray(intType, indices, intValues)
	dateDictionary := array.NewDictionaryArray(dateType, indices, dateValues)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: intType},
		{Name: "d", Type: dateType},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{intDictionary, dateDictionary}, 2)
	plan, err := Bind(context.Background(), schema, []TargetColumn{
		{Name: "i", Type: types.T_int64.ToType()},
		{Name: "d", Type: types.T_date.ToType()},
	}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	bat, stats, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, []int64{22, 11}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
	require.Equal(t, []types.Date{
		types.DaysFromUnixEpochToDate(1),
		types.DaysFromUnixEpochToDate(0),
	}, vector.MustFixedColNoTypeCheck[types.Date](bat.Vecs[1]))
	require.Equal(t, int64(2*(types.T_int64.ToType().TypeSize()+types.T_date.ToType().TypeSize())), stats.MaterializedPayloadBytes)

	bat.Clean(mp)
	record.Release()
	intDictionary.Release()
	dateDictionary.Release()
	indices.Release()
	intValues.Release()
	dateValues.Release()
	alloc.AssertSize(t, 0)
}

func TestDictionaryTimestampUsesCanonicalRangePrecisionAndNullSemantics(t *testing.T) {
	tests := []struct {
		name    string
		unit    arrow.TimeUnit
		value   arrow.Timestamp
		valid   bool
		scale   int32
		errText string
	}{
		{name: "valid millisecond", unit: arrow.Millisecond, value: 1_000, valid: true, scale: 3},
		{name: "target precision loss", unit: arrow.Millisecond, value: 1_001, valid: true, scale: 2, errText: "precision"},
		{name: "nanosecond precision loss", unit: arrow.Nanosecond, value: 1, valid: true, scale: 6, errText: "out of range"},
		{name: "overflow", unit: arrow.Second, value: arrow.Timestamp(^uint64(0) >> 1), valid: true, scale: 6, errText: "out of range"},
		{name: "null payload ignored", unit: arrow.Second, value: arrow.Timestamp(^uint64(0) >> 1), valid: false, scale: 6},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			indicesBuilder := array.NewInt8Builder(alloc)
			indicesBuilder.Append(0)
			indices := indicesBuilder.NewArray()
			indicesBuilder.Release()

			timestampType := &arrow.TimestampType{Unit: test.unit}
			valuesBuilder := array.NewTimestampBuilder(alloc, timestampType)
			valuesBuilder.AppendValues([]arrow.Timestamp{test.value}, []bool{test.valid})
			values := valuesBuilder.NewArray()
			valuesBuilder.Release()
			dictionaryType := &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int8,
				ValueType: timestampType,
			}
			dictionary := array.NewDictionaryArray(dictionaryType, indices, values)
			schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: dictionaryType, Nullable: true}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{dictionary}, 1)
			plan, err := Bind(context.Background(), schema, []TargetColumn{{
				Name: "ts",
				Type: types.New(types.T_timestamp, 0, test.scale),
			}}, MatchByName)
			require.NoError(t, err)
			mp := mpool.MustNewZero()

			bat, _, err := plan.Convert(context.Background(), record, mp, ConvertOptions{})
			if test.errText != "" {
				require.ErrorContains(t, err, test.errText)
				require.Nil(t, bat)
			} else {
				require.NoError(t, err)
				if test.valid {
					require.False(t, bat.Vecs[0].IsNull(0))
				} else {
					require.True(t, bat.Vecs[0].IsNull(0))
				}
				bat.Clean(mp)
			}
			require.Zero(t, mp.CurrNB())

			record.Release()
			dictionary.Release()
			indices.Release()
			values.Release()
			alloc.AssertSize(t, 0)
		})
	}
}

func TestDictionaryRejectsMalformedIndicesAndNestedValues(t *testing.T) {
	for _, test := range []struct {
		name      string
		indexType arrow.DataType
		build     func(memory.Allocator) arrow.Array
	}{
		{
			name:      "negative signed index",
			indexType: arrow.PrimitiveTypes.Int16,
			build: func(alloc memory.Allocator) arrow.Array {
				builder := array.NewInt16Builder(alloc)
				defer builder.Release()
				builder.Append(-1)
				return builder.NewArray()
			},
		},
		{
			name:      "too large unsigned index",
			indexType: arrow.PrimitiveTypes.Uint64,
			build: func(alloc memory.Allocator) arrow.Array {
				builder := array.NewUint64Builder(alloc)
				defer builder.Release()
				builder.Append(9)
				return builder.NewArray()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
			indices := test.build(alloc)
			valuesBuilder := array.NewInt32Builder(alloc)
			valuesBuilder.Append(7)
			values := valuesBuilder.NewArray()
			valuesBuilder.Release()
			dictionaryType := &arrow.DictionaryType{IndexType: test.indexType, ValueType: arrow.PrimitiveTypes.Int32}
			dictionary := array.NewDictionaryArray(dictionaryType, indices, values)
			schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: dictionaryType}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{dictionary}, 1)
			plan, err := Bind(context.Background(), schema,
				[]TargetColumn{{Name: "i", Type: types.T_int32.ToType()}}, MatchByName)
			require.NoError(t, err)
			mp := mpool.MustNewZero()

			require.NotPanics(t, func() {
				_, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
			})
			require.ErrorContains(t, err, "outside")
			require.Equal(t, int64(0), mp.CurrNB())

			record.Release()
			dictionary.Release()
			indices.Release()
			values.Release()
			alloc.AssertSize(t, 0)
		})
	}

	nested := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
	}
	_, err := Bind(context.Background(), arrow.NewSchema([]arrow.Field{{Name: "s", Type: nested}}, nil),
		[]TargetColumn{{Name: "s", Type: types.T_varchar.ToType()}}, MatchByName)
	require.ErrorContains(t, err, "nested")
}

func TestMaxOutputRowsHonorsByteBudgetAndProgress(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	intBuilder := array.NewInt64Builder(alloc)
	intBuilder.AppendValues([]int64{1, 2, 3, 4}, nil)
	ints := intBuilder.NewArray()
	intBuilder.Release()
	stringBuilder := array.NewStringBuilder(alloc)
	value := "0123456789012345678901234567890123456789"
	require.Len(t, value, 40)
	stringBuilder.AppendValues([]string{value, value, value, value}, nil)
	strings := stringBuilder.NewArray()
	stringBuilder.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{ints, strings}, 4)
	plan, err := Bind(context.Background(), schema, []TargetColumn{
		{Name: "i", Type: types.T_int64.ToType()},
		{Name: "s", Type: types.T_varchar.ToType()},
	}, MatchByName)
	require.NoError(t, err)

	const rowBytes = uint64(8 + types.VarlenaSize + 40)
	rows, err := plan.MaxOutputRows(context.Background(), record, 0, 4, 2*rowBytes)
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	rows, err = plan.MaxOutputRows(context.Background(), record, 1, 3, rowBytes)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = plan.MaxOutputRows(context.Background(), record, 0, 4, 1)
	require.NoError(t, err)
	require.Equal(t, 1, rows, "one oversized row must still make progress")

	record.Release()
	ints.Release()
	strings.Release()
	alloc.AssertSize(t, 0)
}

func TestConvertRollbackSchemaDriftNotNullAndCancel(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	intsBuilder := array.NewInt64Builder(alloc)
	intsBuilder.AppendValues([]int64{1, 2}, nil)
	ints := intsBuilder.NewArray()
	intsBuilder.Release()
	stringsBuilder := array.NewStringBuilder(alloc)
	stringsBuilder.AppendValues([]string{"ok", "bad"}, []bool{true, false})
	strings := stringsBuilder.NewArray()
	stringsBuilder.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{ints, strings}, 2)
	plan, err := Bind(context.Background(), schema, []TargetColumn{
		{Name: "i", Type: types.T_int64.ToType()},
		{Name: "s", Type: types.T_varchar.ToType(), NotNull: true},
	}, MatchByName)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	_, _, err = plan.Convert(context.Background(), record, mp, ConvertOptions{})
	require.ErrorContains(t, err, "NOT NULL")
	require.Equal(t, int64(0), mp.CurrNB())

	driftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "renamed", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	driftRecord := array.NewRecordBatch(driftSchema, []arrow.Array{ints, strings}, 2)
	_, _, err = plan.Convert(context.Background(), driftRecord, mp, ConvertOptions{})
	require.ErrorContains(t, err, "does not match")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = plan.Convert(canceled, record, mp, ConvertOptions{})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(0), mp.CurrNB())

	driftRecord.Release()
	record.Release()
	ints.Release()
	strings.Release()
	alloc.AssertSize(t, 0)
}
