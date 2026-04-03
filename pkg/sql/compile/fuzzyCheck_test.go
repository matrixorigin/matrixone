// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVectorToStringNullHandling(t *testing.T) {
	mp, err := mpool.NewMPool("test_vectorToString", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("per_row_null_check", func(t *testing.T) {
		// Vector with mix of NULLs and values
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(42), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(99), false, mp))
		defer vec.Free(mp)

		// Non-NULL row should return value
		s, err := vectorToString(vec, 0)
		require.NoError(t, err)
		require.Equal(t, "42", s)

		// NULL row should return empty string
		s, err = vectorToString(vec, 1)
		require.NoError(t, err)
		require.Equal(t, "", s)

		// Non-NULL row after NULL should still return value
		s, err = vectorToString(vec, 2)
		require.NoError(t, err)
		require.Equal(t, "99", s)
	})

	t.Run("all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		defer vec.Free(mp)

		s, err := vectorToString(vec, 0)
		require.NoError(t, err)
		require.Equal(t, "", s)

		s, err = vectorToString(vec, 1)
		require.NoError(t, err)
		require.Equal(t, "", s)
	})

	t.Run("varchar_values", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, []byte("hello"), false, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp)) // NULL
		defer vec.Free(mp)

		s, err := vectorToString(vec, 0)
		require.NoError(t, err)
		require.Equal(t, "hello", s)

		s, err = vectorToString(vec, 1)
		require.NoError(t, err)
		require.Equal(t, "", s)
	})

	// Cover more type branches in vectorToString
	t.Run("bool", func(t *testing.T) {
		vec := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(vec, true, false, mp))
		require.NoError(t, vector.AppendFixed(vec, false, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, false, false, mp))
		defer vec.Free(mp)

		s, _ := vectorToString(vec, 0)
		require.Equal(t, "true", s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s) // NULL
		s, _ = vectorToString(vec, 2)
		require.Equal(t, "false", s)
	})

	t.Run("int_types", func(t *testing.T) {
		// int8
		vec8 := vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixed(vec8, int8(7), false, mp))
		require.NoError(t, vector.AppendFixed(vec8, int8(0), true, mp))
		defer vec8.Free(mp)
		s, _ := vectorToString(vec8, 0)
		require.Equal(t, "7", s)
		s, _ = vectorToString(vec8, 1)
		require.Equal(t, "", s)

		// int16
		vec16 := vector.NewVec(types.T_int16.ToType())
		require.NoError(t, vector.AppendFixed(vec16, int16(16), false, mp))
		require.NoError(t, vector.AppendFixed(vec16, int16(0), true, mp))
		defer vec16.Free(mp)
		s, _ = vectorToString(vec16, 0)
		require.Equal(t, "16", s)
		s, _ = vectorToString(vec16, 1)
		require.Equal(t, "", s)

		// int32
		vec32 := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(vec32, int32(32), false, mp))
		require.NoError(t, vector.AppendFixed(vec32, int32(0), true, mp))
		defer vec32.Free(mp)
		s, _ = vectorToString(vec32, 0)
		require.Equal(t, "32", s)
		s, _ = vectorToString(vec32, 1)
		require.Equal(t, "", s)
	})

	t.Run("uint_types", func(t *testing.T) {
		vec8 := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(vec8, uint8(8), false, mp))
		require.NoError(t, vector.AppendFixed(vec8, uint8(0), true, mp))
		defer vec8.Free(mp)
		s, _ := vectorToString(vec8, 0)
		require.Equal(t, "8", s)
		s, _ = vectorToString(vec8, 1)
		require.Equal(t, "", s)

		vec16 := vector.NewVec(types.T_uint16.ToType())
		require.NoError(t, vector.AppendFixed(vec16, uint16(16), false, mp))
		require.NoError(t, vector.AppendFixed(vec16, uint16(0), true, mp))
		defer vec16.Free(mp)
		s, _ = vectorToString(vec16, 0)
		require.Equal(t, "16", s)

		vec32 := vector.NewVec(types.T_uint32.ToType())
		require.NoError(t, vector.AppendFixed(vec32, uint32(32), false, mp))
		require.NoError(t, vector.AppendFixed(vec32, uint32(0), true, mp))
		defer vec32.Free(mp)
		s, _ = vectorToString(vec32, 0)
		require.Equal(t, "32", s)

		vec64 := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(vec64, uint64(64), false, mp))
		require.NoError(t, vector.AppendFixed(vec64, uint64(0), true, mp))
		defer vec64.Free(mp)
		s, _ = vectorToString(vec64, 0)
		require.Equal(t, "64", s)
	})

	t.Run("float_types", func(t *testing.T) {
		vec32 := vector.NewVec(types.T_float32.ToType())
		require.NoError(t, vector.AppendFixed(vec32, float32(1.5), false, mp))
		require.NoError(t, vector.AppendFixed(vec32, float32(0), true, mp))
		defer vec32.Free(mp)
		s, _ := vectorToString(vec32, 0)
		require.Equal(t, "1.5", s)
		s, _ = vectorToString(vec32, 1)
		require.Equal(t, "", s)

		vec64 := vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(vec64, float64(2.5), false, mp))
		require.NoError(t, vector.AppendFixed(vec64, float64(0), true, mp))
		defer vec64.Free(mp)
		s, _ = vectorToString(vec64, 0)
		require.Equal(t, "2.5", s)
		s, _ = vectorToString(vec64, 1)
		require.Equal(t, "", s)
	})

	t.Run("decimal_types", func(t *testing.T) {
		dec64 := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		d64, _ := types.Decimal64FromFloat64(3.14, 64, 2)
		require.NoError(t, vector.AppendFixed(dec64, d64, false, mp))
		require.NoError(t, vector.AppendFixed(dec64, d64, true, mp)) // NULL
		defer dec64.Free(mp)
		s, _ := vectorToString(dec64, 0)
		require.NotEmpty(t, s)
		s, _ = vectorToString(dec64, 1)
		require.Equal(t, "", s)

		dec128 := vector.NewVec(types.New(types.T_decimal128, 20, 2))
		d128, _ := types.Decimal128FromFloat64(6.28, 128, 2)
		require.NoError(t, vector.AppendFixed(dec128, d128, false, mp))
		require.NoError(t, vector.AppendFixed(dec128, d128, true, mp))
		defer dec128.Free(mp)
		s, _ = vectorToString(dec128, 0)
		require.NotEmpty(t, s)
		s, _ = vectorToString(dec128, 1)
		require.Equal(t, "", s)
	})

	t.Run("bit_type", func(t *testing.T) {
		vec := vector.NewVec(types.T_bit.ToType())
		require.NoError(t, vector.AppendFixed(vec, uint64(0xFF), false, mp))
		require.NoError(t, vector.AppendFixed(vec, uint64(0), true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.Equal(t, "255", s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})

	t.Run("char_text_blob", func(t *testing.T) {
		for _, oid := range []types.T{types.T_char, types.T_text, types.T_blob, types.T_binary, types.T_varbinary} {
			vec := vector.NewVec(oid.ToType())
			require.NoError(t, vector.AppendBytes(vec, []byte("data"), false, mp))
			require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
			defer vec.Free(mp)
			s, _ := vectorToString(vec, 0)
			require.Equal(t, "data", s, "type %v", oid)
			s, _ = vectorToString(vec, 1)
			require.Equal(t, "", s, "NULL for type %v", oid)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		vec := vector.NewVec(types.T_uuid.ToType())
		u, _ := types.ParseUuid("12345678-1234-1234-1234-123456789abc")
		require.NoError(t, vector.AppendFixed(vec, u, false, mp))
		require.NoError(t, vector.AppendFixed(vec, u, true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.NotEmpty(t, s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})

	t.Run("date_time_types", func(t *testing.T) {
		// date
		dv := vector.NewVec(types.T_date.ToType())
		d, _ := types.ParseDateCast("2024-01-15")
		require.NoError(t, vector.AppendFixed(dv, d, false, mp))
		require.NoError(t, vector.AppendFixed(dv, d, true, mp))
		defer dv.Free(mp)
		s, _ := vectorToString(dv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(dv, 1)
		require.Equal(t, "", s)

		// time
		tv := vector.NewVec(types.T_time.ToType())
		tm, _ := types.ParseTime("12:30:45", 0)
		require.NoError(t, vector.AppendFixed(tv, tm, false, mp))
		require.NoError(t, vector.AppendFixed(tv, tm, true, mp))
		defer tv.Free(mp)
		s, _ = vectorToString(tv, 0)
		require.Contains(t, s, "12")
		s, _ = vectorToString(tv, 1)
		require.Equal(t, "", s)

		// datetime
		dtv := vector.NewVec(types.New(types.T_datetime, 0, 0))
		dt, _ := types.ParseDatetime("2024-01-15 12:30:45", 0)
		require.NoError(t, vector.AppendFixed(dtv, dt, false, mp))
		require.NoError(t, vector.AppendFixed(dtv, dt, true, mp))
		defer dtv.Free(mp)
		s, _ = vectorToString(dtv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(dtv, 1)
		require.Equal(t, "", s)

		// timestamp
		tsv := vector.NewVec(types.New(types.T_timestamp, 0, 0))
		ts, _ := types.ParseTimestamp(time.Local, "2024-01-15 12:30:45", 0)
		require.NoError(t, vector.AppendFixed(tsv, ts, false, mp))
		require.NoError(t, vector.AppendFixed(tsv, ts, true, mp))
		defer tsv.Free(mp)
		s, _ = vectorToString(tsv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(tsv, 1)
		require.Equal(t, "", s)

		// year
		yv := vector.NewVec(types.T_year.ToType())
		require.NoError(t, vector.AppendFixed(yv, types.MoYear(2024), false, mp))
		require.NoError(t, vector.AppendFixed(yv, types.MoYear(0), true, mp))
		defer yv.Free(mp)
		s, _ = vectorToString(yv, 0)
		require.Contains(t, s, "2024")
		s, _ = vectorToString(yv, 1)
		require.Equal(t, "", s)
	})

	t.Run("enum_type", func(t *testing.T) {
		vec := vector.NewVec(types.T_enum.ToType())
		require.NoError(t, vector.AppendFixed(vec, types.Enum(3), false, mp))
		require.NoError(t, vector.AppendFixed(vec, types.Enum(0), true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.Equal(t, "3", s)
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})

	t.Run("json_type", func(t *testing.T) {
		vec := vector.NewVec(types.T_json.ToType())
		bj, _ := types.ParseStringToByteJson(`{"key":"val"}`)
		bjBytes, _ := bj.Marshal()
		require.NoError(t, vector.AppendBytes(vec, bjBytes, false, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		defer vec.Free(mp)
		s, _ := vectorToString(vec, 0)
		require.Contains(t, s, "key")
		s, _ = vectorToString(vec, 1)
		require.Equal(t, "", s)
	})
}

func TestFirstlyCheckSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test_firstlyCheck", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("all_nulls_no_error", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "all-NULL rows must not trigger duplicate error")
	})

	t.Run("nulls_mixed_with_distinct_values", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "NULLs should be skipped, 1 and 2 are distinct")
	})

	t.Run("real_dup_still_caught", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))  // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(5), false, mp)) // dup!
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		err := f.firstlyCheck(context.Background(), vec)
		require.Error(t, err, "real duplicate must be caught")
		require.Contains(t, err.Error(), "Duplicate entry")
	})

	t.Run("compound_all_nulls_no_error", func(t *testing.T) {
		// Compound key: all rows are NULL (serial() propagated)
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "compound all-NULL rows must not trigger duplicate error")
	})

	t.Run("compound_mixed_nulls_distinct", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (1, 10) — non-NULL
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (2, 20) — non-NULL
		packer.Reset()
		packer.EncodeInt64(2)
		packer.EncodeInt64(20)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.firstlyCheck(context.Background(), vec)
		require.NoError(t, err, "NULLs should be skipped, (1,10) and (2,20) are distinct")
	})

	t.Run("compound_dup_caught", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (1, 10)
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (1, 10) — duplicate!
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.firstlyCheck(context.Background(), vec)
		require.Error(t, err, "compound duplicate must be caught")
		require.Contains(t, err.Error(), "Duplicate entry")
	})
}

func TestGenCollsionKeysSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test_genCollsionKeys", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("non_compound_filters_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(3), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		keys, err := f.genCollsionKeys(vec)
		require.NoError(t, err)
		require.Len(t, keys[0], 2, "should only have 2 non-NULL keys")
	})

	t.Run("non_compound_all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{attr: "pk"}
		keys, err := f.genCollsionKeys(vec)
		require.NoError(t, err)
		require.Len(t, keys[0], 0, "all NULLs should produce empty keys")
	})

	t.Run("compound_filters_nulls", func(t *testing.T) {
		// For compound keys, the packed tuple is a Varlena vector.
		// If ANY component column is NULL, serial() marks the tuple NULL.
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: non-NULL packed tuple
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL (component column was NULL)
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: non-NULL packed tuple
		packer.Reset()
		packer.EncodeInt64(2)
		packer.EncodeInt64(20)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		defer vec.Free(mp)

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		keys, err := f.genCollsionKeys(vec)
		require.NoError(t, err)
		// 2 columns, each should have 2 non-NULL entries
		require.Len(t, keys, 2)
		require.Len(t, keys[0], 2, "column a should have 2 non-NULL keys")
		require.Len(t, keys[1], 2, "column b should have 2 non-NULL keys")
	})
}

func TestFillAllNullsSkipsBackgroundCheck(t *testing.T) {
	mp, err := mpool.NewMPool("test_fill", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("non_compound_all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{attr: "pk"}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 0, f.cnt, "cnt should be 0 when all keys are NULL")
		require.Equal(t, "", f.condition, "condition should be empty")
	})

	t.Run("non_compound_mixed", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(42), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(vec, int64(99), false, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{attr: "pk"}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt, "cnt should only count non-NULL keys")
		require.NotEmpty(t, f.condition, "condition should be generated for non-NULL keys")
		// condition should contain the two non-NULL values
		require.Contains(t, f.condition, "42")
		require.Contains(t, f.condition, "99")
	})

	t.Run("compound_all_nulls", func(t *testing.T) {
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 0, f.cnt, "compound all-NULLs should have cnt=0")
		require.Equal(t, "", f.condition)
	})

	t.Run("compound_mixed_nulls", func(t *testing.T) {
		packer := types.NewPacker()
		defer packer.Close()

		vec := vector.NewVec(types.T_varchar.ToType())
		// Row 0: packed (1, 10) — non-NULL
		packer.Reset()
		packer.EncodeInt64(1)
		packer.EncodeInt64(10)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))
		// Row 1: NULL
		require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
		// Row 2: packed (2, 20) — non-NULL
		packer.Reset()
		packer.EncodeInt64(2)
		packer.EncodeInt64(20)
		require.NoError(t, vector.AppendBytes(vec, packer.GetBuf(), false, mp))

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		defer func() {
			vec.Free(mp)
			bat.Clean(mp)
		}()

		f := &fuzzyCheck{
			attr:       "__mo_cpkey_col",
			isCompound: true,
			compoundCols: []*plan.ColDef{
				{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}
		err := f.fill(context.Background(), bat)
		require.NoError(t, err)
		require.Equal(t, 2, f.cnt, "compound mixed should count only 2 non-NULL keys")
		require.NotEmpty(t, f.condition)
	})
}
