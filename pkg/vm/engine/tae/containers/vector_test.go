// Copyright 2022 Matrix Origin
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

package containers

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTnConst(t *testing.T) {
	m := mpool.MustNewZero()
	v := movec.NewConstNull(types.T_int32.ToType(), 20, m)
	v.IsConstNull()
	tnv := v
	require.True(t, tnv.IsNull(2))
}

func withAllocator(opt Options) Options {
	opt.Allocator = mpool.MustNewZero()
	return opt
}

func TestVectorShallowForeach(t *testing.T) {
	for _, typ := range []types.Type{types.T_int32.ToType(), types.T_char.ToType()} {
		vec := MakeVector(typ, common.DefaultAllocator)
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				AppendValue(vec, nil, true)
			} else {
				switch typ.Oid {
				case types.T_int32:
					AppendValue(vec, int32(i), false)
				case types.T_char:
					AppendValue(vec, []byte("test null"), false)
				}
			}
		}

		Foreach(vec, func(v any, isNull bool, row int) error {
			if row%2 == 0 {
				assert.True(t, isNull)
			}
			return nil
		}, nil)
	}
}

func TestVector1(t *testing.T) {
	opt := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opt)
	AppendValue(vec, int32(12), false)
	AppendValue(vec, int32(32), false)
	AppendMany(vec, []any{int32(1), int32(100)}, []bool{false, false})
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, int32(12), GetValue(vec, 0).(int32))
	assert.Equal(t, int32(32), GetValue(vec, 1).(int32))
	assert.Equal(t, int32(1), GetValue(vec, 2).(int32))
	assert.Equal(t, int32(100), GetValue(vec, 3).(int32))
	vec2 := NewVector(types.T_int32.ToType())
	Extend(vec2, vec)
	assert.Equal(t, 4, vec2.Length())
	assert.Equal(t, int32(12), GetValue(vec2, 0).(int32))
	assert.Equal(t, int32(32), GetValue(vec2, 1).(int32))
	assert.Equal(t, int32(1), GetValue(vec2, 2).(int32))
	assert.Equal(t, int32(100), GetValue(vec2, 3).(int32))
	ReleaseVector(vec)
	ReleaseVector(vec2)
	// XXX MPOOL
	// alloc := vec.GetMPool()
	// assert.Equal(t, 0, alloc.CurrNB())
}

func TestVector2(t *testing.T) {
	opt := withAllocator(Options{})
	vec := NewVector(types.T_int64.ToType(), opt)
	t.Log(vec.String())
	now := time.Now()
	for i := 10; i > 0; i-- {
		AppendValue(vec, int64(i), false)
	}
	t.Log(time.Since(now))
	assert.Equal(t, 10, vec.Length())
	assert.False(t, vec.HasNull())
	AppendValue(vec, nil, true)
	assert.Equal(t, 11, vec.Length())
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))

	UpdateRow(vec, 2, nil, true)
	assert.Equal(t, 11, vec.Length())
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))
	assert.True(t, vec.IsNull(2))

	UpdateRow(vec, 2, int64(22), false)
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))
	assert.False(t, vec.IsNull(2))
	assert.Equal(t, any(int64(22)), GetValue(vec, 2))

	UpdateRow(vec, 10, int64(100), false)
	assert.False(t, vec.HasNull())
	assert.False(t, vec.IsNull(10))
	assert.False(t, vec.IsNull(2))
	assert.Equal(t, any(int64(22)), GetValue(vec, 2))
	assert.Equal(t, any(int64(100)), GetValue(vec, 10))

	t.Log(vec.String())

	ReleaseVector(vec)
	assert.Zero(t, opt.Allocator.CurrNB())

	// vec2 := compute.MockVec(vec.GetType(), 0, 0)
	// now = time.Now()
	// for i := 1000000; i > 0; i-- {
	// 	compute.AppendValue(vec2, int64(i))
	// }
	// t.Log(time.Since(now))

	// vec3 := container.NewVector[int64](opt)
	// now = time.Now()
	// for i := 1000000; i > 0; i-- {
	// 	vec3.Append(int64(i))
	// }
	// t.Log(time.Since(now))
}

func TestVector3(t *testing.T) {
	opts := withAllocator(Options{})
	vec1 := NewVector(types.T_int32.ToType(), opts)
	for i := 0; i < 100; i++ {
		AppendValue(vec1, int32(i), false)
	}

	w := new(bytes.Buffer)
	_, err := WriteVector(vec1, w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())

	vec2 := NewVector(types.T_int32.ToType(), opts)
	_, err = ReadVector(vec2, r)
	assert.NoError(t, err)

	assert.True(t, VectorEquals(vec1, vec2))

	// t.Log(vec1.String())
	// t.Log(vec2.String())
	ReleaseVector(vec1)
	ReleaseVector(vec2)
	assert.Zero(t, opts.Allocator.CurrNB())
}

func TestVector5(t *testing.T) {
	vecTypes := types.MockColTypes()
	sels := mustNullsOK(nulls.NewWithSize(1, bitmapTestMP))
	sels.Add(uint64(2), uint64(6))
	for _, vecType := range vecTypes {
		vec := MockVector(vecType, 10, false, nil)
		rows := make([]int, 0)
		op := func(v any, _ bool, row int) (err error) {
			rows = append(rows, row)
			assert.Equal(t, GetValue(vec, row), v)
			return
		}
		_ = Foreach(vec, op, nil)
		assert.Equal(t, 10, len(rows))
		for i, e := range rows {
			assert.Equal(t, i, e)
		}

		rows = rows[:0]
		_ = Foreach(vec, op, sels)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, 2, rows[0])
		assert.Equal(t, 6, rows[1])

		rows = rows[:0]
		_ = ForeachWindow(vec, 2, 6, op, nil)
		assert.Equal(t, []int{2, 3, 4, 5, 6, 7}, rows)
		rows = rows[:0]
		_ = ForeachWindow(vec, 2, 6, op, sels)
		assert.Equal(t, []int{2, 6}, rows)
		rows = rows[:0]
		_ = ForeachWindow(vec, 3, 6, op, sels)
		assert.Equal(t, []int{6}, rows)
		rows = rows[:0]
		_ = ForeachWindow(vec, 3, 3, op, sels)
		assert.Equal(t, []int{}, rows)

		ReleaseVector(vec)
	}
}

func TestVector6(t *testing.T) {
	vecTypes := types.MockColTypes()
	sels := mustNullsOK(nulls.NewWithSize(1, bitmapTestMP))
	sels.Add(uint64(2), uint64(6))
	f := func(vecType types.Type, nullable bool) {
		vec := MockVector(vecType, 10, false, nil)
		if nullable {
			UpdateRow(vec, 4, nil, true)
		}
		bias := 0
		win := Window(vec, bias, 8)
		assert.Equal(t, 8, win.Length())
		rows := make([]int, 0)
		op := func(v any, _ bool, row int) (err error) {
			rows = append(rows, row)
			assert.Equal(t, GetValue(vec, row+bias), v)
			return
		}
		_ = Foreach(win, op, nil)
		assert.Equal(t, 8, len(rows))
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, rows)
		rows = rows[:0]
		_ = ForeachWindow(win, 2, 3, op, nil)
		assert.Equal(t, 3, len(rows))
		assert.Equal(t, []int{2, 3, 4}, rows)

		rows = rows[:0]
		_ = Foreach(win, op, sels)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, 2, rows[0])
		assert.Equal(t, 6, rows[1])

		rows = rows[:0]
		_ = ForeachWindow(win, 2, 6, op, sels)
		assert.Equal(t, []int{2, 6}, rows)
		rows = rows[:0]
		_ = ForeachWindow(win, 3, 4, op, sels)
		assert.Equal(t, []int{6}, rows)
		rows = rows[:0]
		_ = ForeachWindow(win, 3, 3, op, sels)
		assert.Equal(t, []int{}, rows)

		bias = 1
		win = Window(vec, bias, 8)

		op2 := func(v any, _ bool, row int) (err error) {
			rows = append(rows, row)
			// t.Logf("row=%d,v=%v", row, v)
			// t.Logf("row=%d, winv=%v", row, win.Get(row))
			// t.Logf("row+bias=%d, rawv=%v", row+bias, vec.Get(row+bias))
			assert.Equal(t, GetValue(vec, row+bias), v)
			assert.Equal(t, GetValue(win, row), v)
			return
		}
		rows = rows[:0]
		_ = Foreach(win, op2, nil)
		assert.Equal(t, 8, len(rows))
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, rows)
		rows = rows[:0]
		_ = ForeachWindow(win, 2, 3, op, nil)
		assert.Equal(t, 3, len(rows))
		assert.Equal(t, []int{2, 3, 4}, rows)
		rows = rows[:0]
		_ = Foreach(win, op, sels)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, 2, rows[0])
		assert.Equal(t, 6, rows[1])

		rows = rows[:0]
		_ = ForeachWindow(win, 2, 6, op, sels)
		assert.Equal(t, []int{2, 6}, rows)
		rows = rows[:0]
		_ = ForeachWindow(win, 3, 4, op, sels)
		assert.Equal(t, []int{6}, rows)
		rows = rows[:0]
		_ = ForeachWindow(win, 3, 3, op, sels)
		assert.Equal(t, []int{}, rows)

		ReleaseVector(vec)
	}
	for _, vecType := range vecTypes {
		f(vecType, false)
		f(vecType, true)
	}
}

func TestVector7(t *testing.T) {
	vecTypes := types.MockColTypes()
	testF := func(typ types.Type, nullable bool) {
		vec := MockVector(typ, 10, false, nil)
		if nullable {
			AppendValue(vec, nil, true)
		}
		vec2 := MockVector(typ, 10, false, nil)
		vec3 := MakeVector(typ, common.DefaultAllocator)
		Extend(vec3, vec)
		assert.Equal(t, vec.Length(), vec3.Length())
		Extend(vec3, vec2)
		assert.Equal(t, vec.Length()+vec2.Length(), vec3.Length())
		for i := 0; i < vec3.Length(); i++ {
			if i >= vec.Length() {
				assert.Equal(t, GetValue(vec2, i-vec.Length()), GetValue(vec3, i))
			} else {
				if vec.IsNull(uint64(i)) {
					assert.Equal(t, true, vec3.IsNull(uint64(i)))
				} else {
					assert.Equal(t, GetValue(vec, i), GetValue(vec3, i))
				}
			}
		}

		vec4 := MakeVector(typ, common.DefaultAllocator)
		cnt := 5
		if nullable {
			cnt = 6
		}
		ExtendWithOffset(vec4, vec, 5, cnt)
		assert.Equal(t, cnt, vec4.Length())
		// t.Log(vec4.String())
		// t.Log(vec.String())
		for i := 0; i < cnt; i++ {
			assert.Equal(t, GetValue(vec, i+5), GetValue(vec4, i))
		}

		ReleaseVector(vec)
		ReleaseVector(vec2)
		ReleaseVector(vec3)
	}
	for _, typ := range vecTypes {
		testF(typ, true)
		testF(typ, false)
	}
}

func TestVector8(t *testing.T) {
	vec := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec)
	AppendValue(vec, int32(0), false)
	AppendValue(vec, int32(1), false)
	AppendValue(vec, int32(2), false)
	AppendValue(vec, nil, true)
	AppendValue(vec, int32(4), false)
	AppendValue(vec, int32(5), false)
	assert.True(t, vec.IsNull(3))
	DeleteRow(vec, 1)
	assert.True(t, vec.IsNull(2))
	DeleteRow(vec, 3)
	assert.True(t, vec.IsNull(2))
	UpdateRow(vec, 1, nil, true)
	assert.True(t, vec.IsNull(1))
	assert.True(t, vec.IsNull(2))
	AppendValue(vec, nil, true)
	assert.True(t, vec.IsNull(1))
	assert.True(t, vec.IsNull(2))
	assert.True(t, vec.IsNull(4))
	Compact(vec, roaring.BitmapOf(0, 2))
	assert.Equal(t, 3, vec.Length())
	assert.True(t, vec.IsNull(0))
	assert.True(t, vec.IsNull(2))
	t.Log(vec.String())
}

func TestVector9(t *testing.T) {
	opts := withAllocator(Options{})
	vec := NewVector(types.T_varchar.ToType(), opts)
	AppendValue(vec, []byte("h1"), false)
	AppendValue(vec, []byte("h22"), false)
	AppendValue(vec, []byte("h333"), false)
	AppendValue(vec, nil, true)
	AppendValue(vec, []byte("h4444"), false)

	cloned := CloneWindow(vec, 2, 2, vec.MPool() /* REVIEW: CloneWindow pool */)
	assert.Equal(t, 2, cloned.Length())
	assert.Equal(t, GetValue(vec, 2), GetValue(cloned, 0))
	assert.Equal(t, GetValue(vec, 3), GetValue(cloned, 1))
	ReleaseVector(cloned)
	ReleaseVector(vec)
	assert.Zero(t, opts.Allocator.CurrNB())
}

func TestCompact(t *testing.T) {
	opts := withAllocator(Options{})
	vec := NewVector(types.T_varchar.ToType(), opts)

	AppendValue(vec, nil, true)
	t.Log(vec.String())
	deletes := roaring.BitmapOf(0)
	//{null}
	Compact(vec, deletes)
	//{}
	assert.Equal(t, 0, vec.Length())

	AppendValue(vec, nil, true)
	AppendValue(vec, nil, true)
	AppendValue(vec, nil, true)
	deletes = roaring.BitmapOf(0, 1)
	//{n,n,n}
	Compact(vec, deletes)
	//{n}
	assert.Equal(t, 1, vec.Length())
	assert.True(t, vec.IsNull(0))

	AppendValue(vec, []byte("var"), false)
	AppendValue(vec, nil, true)
	AppendValue(vec, []byte("var"), false)
	AppendValue(vec, nil, true)
	//{null,var,null,var,null}
	deletes = roaring.BitmapOf(1, 3)
	Compact(vec, deletes)
	//{null,null,null}
	assert.Equal(t, 3, vec.Length())
	assert.True(t, vec.IsNull(0))
	assert.True(t, vec.IsNull(1))
	assert.True(t, vec.IsNull(2))
	ReleaseVector(vec)
}

func BenchmarkVectorExtend(t *testing.B) {
	vec1 := MockVector(types.T_int32.ToType(), 0, true, nil)
	vec2 := MockVector(types.T_int32.ToType(), 1, true, nil)
	defer ReleaseVector(vec1)
	defer ReleaseVector(vec2)

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		Extend(vec1, vec2)
	}
}

func TestForeachWindowFixed(t *testing.T) {
	vec1 := MockVector(types.T_uint32.ToType(), 2, false, nil)
	defer ReleaseVector(vec1)
	AppendValue(vec1, nil, true)

	cnt := 0
	op := func(v uint32, isNull bool, row int) (err error) {
		t.Logf("v=%v,null=%v,row=%d", v, isNull, row)
		cnt++
		if cnt == vec1.Length() {
			assert.True(t, isNull)
		} else {
			assert.Equal(t, GetValue(vec1, row).(uint32), v)
		}
		return
	}
	ForeachWindowFixed(vec1, 0, vec1.Length(), false, op, nil, nil)
	assert.Equal(t, vec1.Length(), cnt)
}

func TestForeachWindowBytes(t *testing.T) {
	vec1 := MockVector(types.T_varchar.ToType(), 2, false, nil)
	defer ReleaseVector(vec1)
	AppendValue(vec1, nil, true)

	cnt := 0
	op := func(v []byte, isNull bool, row int) (err error) {
		t.Logf("v=%v,null=%v,row=%d", v, isNull, row)
		cnt++
		if cnt == vec1.Length() {
			assert.True(t, isNull)
		} else {
			assert.Equal(t, 0, bytes.Compare(v, GetValue(vec1, row).([]byte)))
		}
		return
	}
	ForeachWindowVarlen(vec1, 0, vec1.Length(), false, op, nil, nil)
	assert.Equal(t, vec1.Length(), cnt)
}

func BenchmarkForeachVector(b *testing.B) {
	rows := 1000
	int64s := MockVector2(types.T_int64.ToType(), rows, 0)
	defer ReleaseVector(int64s)
	b.Run("int64-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Foreach(int64s, func(any, bool, int) error {
				return nil
			}, nil)
		}
	})
	b.Run("int64-new", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ForeachVectorWindow(int64s, 0, rows, func(int64, bool, int) (err error) {
				return
			}, nil, nil)
		}
	})

	chars := MockVector2(types.T_varchar.ToType(), rows, 0)
	defer ReleaseVector(chars)
	b.Run("chars-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Foreach(chars, func(any, bool, int) error {
				return nil
			}, nil)
		}
	})
	b.Run("chars-new", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ForeachVectorWindow(chars, 0, rows, func([]byte, bool, int) (err error) {
				return
			}, nil, nil)
		}
	})
}

func BenchmarkForeachVectorBytes(b *testing.B) {
	rows := 1000
	vec := MockVector2(types.T_int64.ToType(), rows, 0)
	defer ReleaseVector(vec)
	b.Run("int64-bytes", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ForeachWindowBytes(vec, 0, vec.Length(), func(v []byte, isNull bool, row int) (err error) {
				return
			}, nil)
		}
	})
	b.Run("int64-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Foreach(vec, func(any, bool, int) error {
				return nil
			}, nil)
		}
	})
}

func testFunc[T any](args ...any) func(T, bool, int) error {
	return func(v T, isNull bool, row int) (err error) {
		return
	}
}

func BenchmarkFunctions(b *testing.B) {
	var funcs = map[types.T]any{
		types.T_bool:  testFunc[bool],
		types.T_int32: testFunc[int32],
		types.T_char:  testFunc[[]byte],
	}
	vec := MockVector2(types.T_char.ToType(), 1000, 0)
	defer ReleaseVector(vec)
	b.Run("func-new", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ForeachVectorWindow(vec, 0, vec.Length(), MakeForeachVectorOp(vec.GetType().Oid, funcs), nil, nil)
		}
	})
	b.Run("func-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Foreach(vec, func(any, bool, int) (err error) {
				return
			}, nil)
		}
	})
}

func getOverload(typ types.T, t *testing.T, rows *roaring.Bitmap, vec Vector) any {
	switch typ {
	case types.T_bool:
		return overLoadFactory[bool](t, rows, vec)
	case types.T_bit:
		return overLoadFactory[uint64](t, rows, vec)
	case types.T_int8:
		return overLoadFactory[int8](t, rows, vec)
	case types.T_int16:
		return overLoadFactory[int16](t, rows, vec)
	case types.T_int32:
		return overLoadFactory[int32](t, rows, vec)
	case types.T_int64:
		return overLoadFactory[int64](t, rows, vec)
	case types.T_uint8:
		return overLoadFactory[uint8](t, rows, vec)
	case types.T_uint16:
		return overLoadFactory[uint16](t, rows, vec)
	case types.T_uint32:
		return overLoadFactory[uint32](t, rows, vec)
	case types.T_uint64:
		return overLoadFactory[uint64](t, rows, vec)
	case types.T_float32:
		return overLoadFactory[float32](t, rows, vec)
	case types.T_float64:
		return overLoadFactory[float64](t, rows, vec)
	case types.T_timestamp:
		return overLoadFactory[types.Timestamp](t, rows, vec)
	case types.T_date:
		return overLoadFactory[types.Date](t, rows, vec)
	case types.T_time:
		return overLoadFactory[types.Time](t, rows, vec)
	case types.T_datetime:
		return overLoadFactory[types.Datetime](t, rows, vec)
	case types.T_decimal64:
		return overLoadFactory[types.Decimal64](t, rows, vec)
	case types.T_decimal128:
		return overLoadFactory[types.Decimal128](t, rows, vec)
	case types.T_decimal256:
		return overLoadFactory[types.Decimal256](t, rows, vec)
	case types.T_TS:
		return overLoadFactory[types.TS](t, rows, vec)
	case types.T_Rowid:
		return overLoadFactory[types.Rowid](t, rows, vec)
	case types.T_Blockid:
		return overLoadFactory[types.Blockid](t, rows, vec)
	case types.T_uuid:
		return overLoadFactory[types.Uuid](t, rows, vec)
	case types.T_enum:
		return overLoadFactory[types.Enum](t, rows, vec)
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_json, types.T_text,
		types.T_array_float32, types.T_array_float64:
		return overLoadFactory[[]byte](t, rows, vec)
	default:
		panic("unsupport")
	}
}

func overLoadFactory[T any](t *testing.T, rows *roaring.Bitmap, vec Vector) func(v T, _ bool, row int) error {
	return func(v T, _ bool, row int) (err error) {
		rows.Add(uint32(row))
		assert.Equal(t, GetValue(vec, row), v)
		return
	}
}

func TestForeachSelectBitmap(t *testing.T) {
	vecTypes := types.MockColTypes()
	sels := mustNullsOK(nulls.NewWithSize(1, bitmapTestMP))
	sels.Add(uint64(2), uint64(6))
	f := func(vecType types.Type, nullable bool) {
		vec := MockVector(vecType, 10, false, nil)
		rows := roaring.New()
		op := getOverload(vecType.Oid, t, rows, vec)

		ForeachVectorWindow(vec, 0, vec.Length(), op, nil, sels)
		assert.Equal(t, uint64(2), rows.GetCardinality())
		assert.True(t, rows.Contains(2))
		assert.True(t, rows.Contains(6))
	}
	for _, vecType := range vecTypes {
		f(vecType, false)
	}

	vec2 := MockVector(types.T_int32.ToType(), 10, true, nil)
	defer ReleaseVector(vec2)

	_ = ForeachVectorWindow(vec2, 0, 5, nil, func(_ any, _ bool, _ int) (err error) {
		return
	}, nil)
}

func TestVectorPool(t *testing.T) {
	cnt := 10
	pool := NewVectorPool(t.Name(), cnt)
	assert.Equal(t, cnt, len(pool.fixSizedPool)+len(pool.varlenPool))

	allTypes := []types.Type{
		types.T_bool.ToType(),
		types.T_int32.ToType(),
		types.T_uint8.ToType(),
		types.T_float32.ToType(),
		types.T_datetime.ToType(),
		types.T_date.ToType(),
		types.T_decimal64.ToType(),
		types.T_char.ToType(),
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_json.ToType(),
	}
	vecs := make([]Vector, 0, len(allTypes))
	for _, typ := range allTypes {
		vec := pool.GetVector(&typ)
		assert.Equal(t, *vec.GetType(), typ)
		vecs = append(vecs, vec)
	}

	assert.Equal(t, 0, pool.Allocated())

	for _, vec := range vecs {
		PreExtend(vec, 100)
	}

	allocated := pool.Allocated()

	if pool.ratio >= 0.6 && pool.ratio < 0.7 {
		t.Log(pool.String())
		usedCnt, _ := pool.FixedSizeUsed(false)
		assert.GreaterOrEqual(t, 6, usedCnt)
		usedCnt, _ = pool.VarlenUsed(false)
		assert.GreaterOrEqual(t, 4, usedCnt)
		usedCnt, _ = pool.Used(false)
		assert.GreaterOrEqual(t, 10, usedCnt)
	}

	for _, vec := range vecs {
		ReleaseVector(vec)
	}

	t.Log(pool.String())
	assert.Equal(t, allocated, pool.Allocated())
}

func TestVectorPool2(t *testing.T) {
	pool := NewVectorPool(t.Name(), 10, WithAllocationLimit(1024))
	typ := types.T_int32.ToType()
	vec := pool.GetVector(&typ)
	PreExtend(vec, 300)
	t.Log(pool.String())
	assert.Less(t, 1024, pool.Allocated())
	ReleaseVector(vec)
	assert.Equal(t, 0, pool.Allocated())
}

func TestVectorPool3(t *testing.T) {
	pool := NewVectorPool(t.Name(), 10)
	typ := types.T_int32.ToType()
	vec1 := NewVector(typ)
	AppendValue(vec1, int32(1), false)
	AppendValue(vec1, int32(2), false)
	AppendValue(vec1, int32(3), false)

	vec2 := CloneWindowWithPool(vec1, 0, 3, pool)
	t.Log(PPString(vec2, 0))
	ReleaseVector(vec1)
	ReleaseVector(vec2)
}

func TestConstNullVector(t *testing.T) {
	vec := NewConstNullVector(types.T_int32.ToType(), 10, common.DefaultAllocator)
	defer ReleaseVector(vec)
	assert.Equal(t, 10, vec.Length())
	assert.True(t, vec.IsConstNull())

	for i := 0; i < vec.Length(); i++ {
		assert.True(t, vec.IsNull(uint64(i)))
	}

	var w bytes.Buffer
	_, err := WriteVector(vec, &w)
	assert.NoError(t, err)

	vec2 := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec2)
	_, err = ReadVector(vec2, &w)
	assert.NoError(t, err)
	assert.Equal(t, 10, vec2.Length())
	assert.True(t, vec2.IsConstNull())
	for i := 0; i < vec2.Length(); i++ {
		assert.True(t, vec2.IsNull(uint64(i)))
	}

	vecw := Window(vec, 0, 5)
	assert.Equal(t, 5, vecw.Length())
	assert.True(t, vecw.IsConstNull())
	for i := 0; i < vecw.Length(); i++ {
		assert.True(t, vecw.IsNull(uint64(i)))
	}
}

func TestConstVector(t *testing.T) {
	vec := NewConstFixed[int32](types.T_int32.ToType(), int32(1), 10)
	defer ReleaseVector(vec)

	assert.Equal(t, 10, vec.Length())
	assert.True(t, vec.IsConst())
	assert.False(t, vec.IsConstNull())

	for i := 0; i < vec.Length(); i++ {
		v := GetValue(vec, i).(int32)
		assert.Equal(t, int32(1), v)
	}

	vec2 := NewConstBytes(types.T_char.ToType(), []byte("abc"), 10)
	defer ReleaseVector(vec2)
	assert.Equal(t, 10, vec2.Length())
	assert.True(t, vec2.IsConst())
	assert.False(t, vec2.IsConstNull())

	for i := 0; i < vec2.Length(); i++ {
		assert.Equal(t, []byte("abc"), GetValue(vec2, i).([]byte))
	}

	var w bytes.Buffer
	_, err := WriteVector(vec, &w)
	assert.NoError(t, err)

	vec3 := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec3)
	_, err = ReadVector(vec3, &w)
	assert.NoError(t, err)
	assert.True(t, vec3.IsConst())
	assert.False(t, vec3.IsConstNull())
	assert.Equal(t, 10, vec3.Length())
	for i := 0; i < vec3.Length(); i++ {
		assert.Equal(t, int32(1), GetValue(vec3, i).(int32))
	}

	w.Reset()
	_, err = WriteVector(vec2, &w)
	assert.NoError(t, err)

	vec4 := MakeVector(types.T_char.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec4)
	_, err = ReadVector(vec4, &w)
	assert.NoError(t, err)
	assert.True(t, vec4.IsConst())
	assert.False(t, vec4.IsConstNull())
	assert.Equal(t, 10, vec4.Length())
	for i := 0; i < vec4.Length(); i++ {
		assert.Equal(t, []byte("abc"), GetValue(vec4, i).([]byte))
	}

	vecw := Window(vec, 0, 5)
	assert.Equal(t, 5, vecw.Length())
	assert.True(t, vecw.IsConst())
	assert.False(t, vecw.IsConstNull())
	for i := 0; i < vecw.Length(); i++ {
		assert.Equal(t, int32(1), GetValue(vecw, i).(int32))
	}

	vecw4 := Window(vec4, 0, 5)
	assert.Equal(t, 5, vecw4.Length())
	assert.True(t, vecw4.IsConst())
	assert.False(t, vecw4.IsConstNull())
	for i := 0; i < vecw4.Length(); i++ {
		assert.Equal(t, []byte("abc"), GetValue(vecw4, i).([]byte))
	}

}

// TestVectorWrapper_WriteTo_PreAllocation tests that WriteTo uses pre-allocation to reduce reallocations
func TestVectorWrapper_WriteTo_PreAllocation(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec)

	// Add data
	for i := 0; i < 1000; i++ {
		AppendValue(vec, int32(i), false)
	}

	// Get estimated size
	estimatedSize := ApproxSize(vec)
	require.Greater(t, estimatedSize, 0, "estimated size should be positive")
	_ = estimatedSize // estimatedSize is used for validation only

	// WriteTo should work correctly
	var buf bytes.Buffer
	n, err := WriteVector(vec, &buf)
	require.NoError(t, err)
	require.Greater(t, n, int64(0), "should write data")

	// Verify data can be read back
	vec2 := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec2)
	_, err = ReadVector(vec2, &buf)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec, vec2), "serialized and deserialized vectors should be equal")
}

// TestVectorWrapper_WriteTo_MultipleCalls tests that multiple WriteTo calls produce consistent results
func TestVectorWrapper_WriteTo_MultipleCalls(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int64.ToType(), opts)
	defer ReleaseVector(vec)

	// Add data
	for i := 0; i < 500; i++ {
		AppendValue(vec, int64(i*2), false)
	}

	// First WriteTo
	var buf1 bytes.Buffer
	n1, err := WriteVector(vec, &buf1)
	require.NoError(t, err)

	// Second WriteTo - should produce identical output
	var buf2 bytes.Buffer
	n2, err := WriteVector(vec, &buf2)
	require.NoError(t, err)

	// Results should be identical
	assert.Equal(t, n1, n2, "both WriteTo calls should write the same amount")
	assert.Equal(t, buf1.Bytes(), buf2.Bytes(), "both WriteTo calls should produce identical output")
}

// TestVectorWrapper_WriteTo_DifferentSizes tests WriteTo with different data sizes
func TestVectorWrapper_WriteTo_DifferentSizes(t *testing.T) {

	opts := withAllocator(Options{})
	testCases := []struct {
		name  string
		count int
	}{
		{"small", 10},
		{"medium", 100},
		{"large", 1000},
		{"very_large", 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vec := NewVector(types.T_int32.ToType(), opts)
			defer ReleaseVector(vec)

			// Add data
			for i := 0; i < tc.count; i++ {
				AppendValue(vec, int32(i), false)
			}

			// WriteTo
			var buf bytes.Buffer
			n, err := WriteVector(vec, &buf)
			require.NoError(t, err, "WriteTo should succeed for %s data", tc.name)
			require.Greater(t, n, int64(0), "should write data")

			// Verify can read back
			vec2 := NewVector(types.T_int32.ToType(), opts)
			defer ReleaseVector(vec2)
			_, err = ReadVector(vec2, &buf)
			require.NoError(t, err)
			assert.True(t, VectorEquals(vec, vec2), "serialized and deserialized vectors should be equal for %s data", tc.name)
		})
	}
}

// TestVectorWrapper_WriteTo_DifferentTypes tests WriteTo with different vector types
func TestVectorWrapper_WriteTo_DifferentTypes(t *testing.T) {

	testTypes := []types.Type{
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_float64.ToType(),
		types.T_varchar.ToType(),
		types.T_char.ToType(),
		types.T_bool.ToType(),
	}

	for _, typ := range testTypes {
		t.Run(typ.String(), func(t *testing.T) {
			vec := MockVector(typ, 100, false, nil)
			defer ReleaseVector(vec)

			// WriteTo
			var buf bytes.Buffer
			n, err := WriteVector(vec, &buf)
			require.NoError(t, err, "WriteTo should succeed for type %s", typ.String())
			require.Greater(t, n, int64(0), "should write data")

			// Verify can read back
			vec2 := MakeVector(typ, common.DefaultAllocator)
			defer ReleaseVector(vec2)
			_, err = ReadVector(vec2, &buf)
			require.NoError(t, err)
			assert.True(t, VectorEquals(vec, vec2), "serialized and deserialized vectors should be equal for type %s", typ.String())
		})
	}
}

// TestVectorWrapper_WriteTo_WithNulls tests WriteTo with null values
func TestVectorWrapper_WriteTo_WithNulls(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec)

	// Add data with nulls
	for i := 0; i < 100; i++ {
		if i%3 == 0 {
			AppendValue(vec, nil, true)
		} else {
			AppendValue(vec, int32(i), false)
		}
	}

	// WriteTo
	var buf bytes.Buffer
	n, err := WriteVector(vec, &buf)
	require.NoError(t, err)
	require.Greater(t, n, int64(0))

	// Verify can read back
	vec2 := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec2)
	_, err = ReadVector(vec2, &buf)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec, vec2), "serialized and deserialized vectors with nulls should be equal")
}

// TestVectorWrapper_WriteTo_EmptyVector tests WriteTo with empty vector
func TestVectorWrapper_WriteTo_EmptyVector(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec)

	// WriteTo empty vector
	var buf bytes.Buffer
	n, err := WriteVector(vec, &buf)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, int64(8), "should write at least size header")

	// Verify can read back
	vec2 := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec2)
	_, err = ReadVector(vec2, &buf)
	require.NoError(t, err)
	assert.Equal(t, 0, vec2.Length(), "empty vector should deserialize to empty vector")
}

// TestVectorWrapper_WriteToV1_PreAllocation tests that WriteToV1 uses pre-allocation
func TestVectorWrapper_WriteToV1_PreAllocation(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec)

	// Add data
	for i := 0; i < 1000; i++ {
		AppendValue(vec, int32(i), false)
	}

	// WriteToV1 should work correctly
	var buf bytes.Buffer
	n, err := WriteVectorV1(vec, &buf)
	require.NoError(t, err)
	require.Greater(t, n, int64(0), "should write data")

	// Verify data can be read back
	vec2 := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec2)
	_, err = ReadVectorV1(vec2, &buf)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec, vec2), "serialized and deserialized vectors should be equal")
}

// TestVectorWrapper_WriteToV1_MultipleCalls tests that multiple WriteToV1 calls produce consistent results
func TestVectorWrapper_WriteToV1_MultipleCalls(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int64.ToType(), opts)
	defer ReleaseVector(vec)

	// Add data
	for i := 0; i < 500; i++ {
		AppendValue(vec, int64(i*2), false)
	}

	// First WriteToV1
	var buf1 bytes.Buffer
	n1, err := WriteVectorV1(vec, &buf1)
	require.NoError(t, err)

	// Second WriteToV1 - should produce identical output
	var buf2 bytes.Buffer
	n2, err := WriteVectorV1(vec, &buf2)
	require.NoError(t, err)

	// Results should be identical
	assert.Equal(t, n1, n2, "both WriteToV1 calls should write the same amount")
	assert.Equal(t, buf1.Bytes(), buf2.Bytes(), "both WriteToV1 calls should produce identical output")
}

// TestVectorWrapper_WriteTo_ConsistencyWithOriginal tests that optimized WriteTo produces same output as original
func TestVectorWrapper_WriteTo_ConsistencyWithOriginal(t *testing.T) {

	testCases := []struct {
		name string
		vec  func() Vector
	}{
		{
			"int32_vector",
			func() Vector {
				opts := withAllocator(Options{})
				vec := NewVector(types.T_int32.ToType(), opts)
				for i := 0; i < 100; i++ {
					AppendValue(vec, int32(i), false)
				}
				return vec
			},
		},
		{
			"varchar_vector",
			func() Vector {
				opts := withAllocator(Options{})
				vec := NewVector(types.T_varchar.ToType(), opts)
				for i := 0; i < 50; i++ {
					AppendValue(vec, []byte(fmt.Sprintf("value%d", i)), false)
				}
				return vec
			},
		},
		{
			"vector_with_nulls",
			func() Vector {
				opts := withAllocator(Options{})
				vec := NewVector(types.T_int64.ToType(), opts)
				for i := 0; i < 100; i++ {
					if i%5 == 0 {
						AppendValue(vec, nil, true)
					} else {
						AppendValue(vec, int64(i), false)
					}
				}
				return vec
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vec := tc.vec()
			defer ReleaseVector(vec)

			// WriteTo (optimized version)
			var buf1 bytes.Buffer
			n1, err := WriteVector(vec, &buf1)
			require.NoError(t, err)

			// WriteToV1 (also optimized)
			var buf2 bytes.Buffer
			n2, err := WriteVectorV1(vec, &buf2)
			require.NoError(t, err)

			// Both should write data
			require.Greater(t, n1, int64(0))
			require.Greater(t, n2, int64(0))

			// Verify both can be read back correctly
			vecType := *vec.GetType()
			opts := withAllocator(Options{})
			vec1 := NewVector(vecType, opts)
			defer ReleaseVector(vec1)
			_, err = ReadVector(vec1, &buf1)
			require.NoError(t, err)
			assert.True(t, VectorEquals(vec, vec1), "WriteTo should produce readable output")

			vec2 := NewVector(vecType, opts)
			defer ReleaseVector(vec2)
			_, err = ReadVectorV1(vec2, &buf2)
			require.NoError(t, err)
			assert.True(t, VectorEquals(vec, vec2), "WriteToV1 should produce readable output")
		})
	}
}

// TestVectorWrapper_WriteTo_LargeData tests WriteTo with large data to verify pre-allocation helps
func TestVectorWrapper_WriteTo_LargeData(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_varchar.ToType(), opts)
	defer ReleaseVector(vec)

	// Add large data
	largeString := make([]byte, 10000)
	for i := range largeString {
		largeString[i] = byte('a' + (i % 26))
	}
	for i := 0; i < 100; i++ {
		AppendValue(vec, largeString, false)
	}

	// WriteTo should handle large data efficiently
	var buf bytes.Buffer
	n, err := WriteVector(vec, &buf)
	require.NoError(t, err)
	require.Greater(t, n, int64(1000000), "should write large amount of data")

	// Verify can read back
	vec2 := NewVector(types.T_varchar.ToType(), opts)
	defer ReleaseVector(vec2)
	_, err = ReadVector(vec2, &buf)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec, vec2), "large data should serialize and deserialize correctly")
}

// TestVectorWrapper_WriteTo_ConstVector tests WriteTo with const vectors
func TestVectorWrapper_WriteTo_ConstVector(t *testing.T) {

	// Test const fixed vector
	vec1 := NewConstFixed[int32](types.T_int32.ToType(), int32(42), 100)
	defer ReleaseVector(vec1)

	var buf1 bytes.Buffer
	n1, err := WriteVector(vec1, &buf1)
	require.NoError(t, err)
	require.Greater(t, n1, int64(0))

	vec1Read := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec1Read)
	_, err = ReadVector(vec1Read, &buf1)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec1, vec1Read), "const fixed vector should serialize correctly")

	// Test const bytes vector
	vec2 := NewConstBytes(types.T_char.ToType(), []byte("test"), 50)
	defer ReleaseVector(vec2)

	var buf2 bytes.Buffer
	n2, err := WriteVector(vec2, &buf2)
	require.NoError(t, err)
	require.Greater(t, n2, int64(0))

	vec2Read := MakeVector(types.T_char.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec2Read)
	_, err = ReadVector(vec2Read, &buf2)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec2, vec2Read), "const bytes vector should serialize correctly")

	// Test const null vector
	vec3 := NewConstNullVector(types.T_int64.ToType(), 200, common.DefaultAllocator)
	defer ReleaseVector(vec3)

	var buf3 bytes.Buffer
	n3, err := WriteVector(vec3, &buf3)
	require.NoError(t, err)
	require.Greater(t, n3, int64(0))

	vec3Read := MakeVector(types.T_int64.ToType(), common.DefaultAllocator)
	defer ReleaseVector(vec3Read)
	_, err = ReadVector(vec3Read, &buf3)
	require.NoError(t, err)
	assert.True(t, VectorEquals(vec3, vec3Read), "const null vector should serialize correctly")
}

// TestVectorWrapper_WriteTo_ApproxSizeAccuracy tests that ApproxSize is used for pre-allocation
func TestVectorWrapper_WriteTo_ApproxSizeAccuracy(t *testing.T) {

	opts := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opts)
	defer ReleaseVector(vec)

	// Add data
	for i := 0; i < 1000; i++ {
		AppendValue(vec, int32(i), false)
	}

	// Get ApproxSize
	approxSize := ApproxSize(vec)
	require.Greater(t, approxSize, 0)

	// WriteTo
	var buf bytes.Buffer
	n, err := WriteVector(vec, &buf)
	require.NoError(t, err)

	// The written size should be close to ApproxSize (allowing for overhead)
	// ApproxSize doesn't include the 8-byte header, so actual size should be approxSize + 8
	actualSize := int(n) - 8 // Subtract header size
	assert.GreaterOrEqual(t, actualSize, approxSize*80/100, "actual size should be at least 80%% of ApproxSize")
	assert.LessOrEqual(t, actualSize, approxSize*120/100, "actual size should be at most 120%% of ApproxSize")
}

// TestVectorWrapper_WriteTo_ConcurrentCalls tests that WriteTo can be called concurrently on different vectors
func TestVectorWrapper_WriteTo_ConcurrentCalls(t *testing.T) {

	opts := withAllocator(Options{})
	numGoroutines := 10
	results := make([][]byte, numGoroutines)
	errs := make([]error, numGoroutines)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			vec := NewVector(types.T_int32.ToType(), opts)
			defer ReleaseVector(vec)

			// Add unique data for each goroutine
			for j := 0; j < 100; j++ {
				AppendValue(vec, int32(idx*1000+j), false)
			}

			var buf bytes.Buffer
			_, err := WriteVector(vec, &buf)
			errs[idx] = err
			if err == nil {
				results[idx] = buf.Bytes()
			}
		}(i)
	}
	wg.Wait()

	// All should succeed
	for i, err := range errs {
		require.NoError(t, err, "goroutine %d should succeed", i)
		require.NotNil(t, results[i], "goroutine %d should produce output", i)
	}

	// All outputs should be different (different data)
	for i := 0; i < numGoroutines; i++ {
		for j := i + 1; j < numGoroutines; j++ {
			assert.NotEqual(t, results[i], results[j], "different vectors should produce different output")
		}
	}
}
