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
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTnConst(t *testing.T) {
	m := mpool.MustNewZero()
	v := movec.NewConstNull(types.T_int32.ToType(), 20, m)
	v.IsConstNull()
	tnv := ToTNVector(v, m)
	require.True(t, tnv.IsNull(2))
}

func withAllocator(opt Options) Options {
	opt.Allocator = mpool.MustNewZero()
	return opt
}

func TestVectorShallowForeach(t *testing.T) {
	defer testutils.AfterTest(t)()
	for _, typ := range []types.Type{types.T_int32.ToType(), types.T_char.ToType()} {
		vec := MakeVector(typ, common.DefaultAllocator)
		for i := 0; i < 10; i++ {
			if i%2 == 0 {
				vec.Append(nil, true)
			} else {
				switch typ.Oid {
				case types.T_int32:
					vec.Append(int32(i), false)
				case types.T_char:
					vec.Append([]byte("test null"), false)
				}
			}
		}

		vec.Foreach(func(v any, isNull bool, row int) error {
			if row%2 == 0 {
				assert.True(t, isNull)
			}
			return nil
		}, nil)
	}
}

func TestVector1(t *testing.T) {
	defer testutils.AfterTest(t)()
	opt := withAllocator(Options{})
	vec := NewVector(types.T_int32.ToType(), opt)
	vec.Append(int32(12), false)
	vec.Append(int32(32), false)
	vec.AppendMany([]any{int32(1), int32(100)}, []bool{false, false})
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, int32(12), vec.Get(0).(int32))
	assert.Equal(t, int32(32), vec.Get(1).(int32))
	assert.Equal(t, int32(1), vec.Get(2).(int32))
	assert.Equal(t, int32(100), vec.Get(3).(int32))
	vec2 := NewVector(types.T_int32.ToType())
	vec2.Extend(vec)
	assert.Equal(t, 4, vec2.Length())
	assert.Equal(t, int32(12), vec2.Get(0).(int32))
	assert.Equal(t, int32(32), vec2.Get(1).(int32))
	assert.Equal(t, int32(1), vec2.Get(2).(int32))
	assert.Equal(t, int32(100), vec2.Get(3).(int32))
	vec.Close()
	vec2.Close()
	// XXX MPOOL
	// alloc := vec.GetMPool()
	// assert.Equal(t, 0, alloc.CurrNB())
}

func TestVector2(t *testing.T) {
	defer testutils.AfterTest(t)()
	opt := withAllocator(Options{})
	vec := NewVector(types.T_int64.ToType(), opt)
	t.Log(vec.String())
	now := time.Now()
	for i := 10; i > 0; i-- {
		vec.Append(int64(i), false)
	}
	t.Log(time.Since(now))
	assert.Equal(t, 10, vec.Length())
	assert.False(t, vec.HasNull())
	vec.Append(nil, true)
	assert.Equal(t, 11, vec.Length())
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))

	vec.Update(2, nil, true)
	assert.Equal(t, 11, vec.Length())
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))
	assert.True(t, vec.IsNull(2))

	vec.Update(2, int64(22), false)
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))
	assert.False(t, vec.IsNull(2))
	assert.Equal(t, any(int64(22)), vec.Get(2))

	vec.Update(10, int64(100), false)
	assert.False(t, vec.HasNull())
	assert.False(t, vec.IsNull(10))
	assert.False(t, vec.IsNull(2))
	assert.Equal(t, any(int64(22)), vec.Get(2))
	assert.Equal(t, any(int64(100)), vec.Get(10))

	t.Log(vec.String())

	vec.Close()
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
	defer testutils.AfterTest(t)()
	opts := withAllocator(Options{})
	vec1 := NewVector(types.T_int32.ToType(), opts)
	for i := 0; i < 100; i++ {
		vec1.Append(int32(i), false)
	}

	w := new(bytes.Buffer)
	_, err := vec1.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())

	vec2 := NewVector(types.T_int32.ToType(), opts)
	_, err = vec2.ReadFrom(r)
	assert.NoError(t, err)

	assert.True(t, vec1.Equals(vec2))

	// t.Log(vec1.String())
	// t.Log(vec2.String())
	vec1.Close()
	vec2.Close()
	assert.Zero(t, opts.Allocator.CurrNB())
}

func TestVector5(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	sels := nulls.NewWithSize(1)
	sels.Add(uint64(2), uint64(6))
	for _, vecType := range vecTypes {
		vec := MockVector(vecType, 10, false, nil)
		rows := make([]int, 0)
		op := func(v any, _ bool, row int) (err error) {
			rows = append(rows, row)
			assert.Equal(t, vec.Get(row), v)
			return
		}
		_ = vec.Foreach(op, nil)
		assert.Equal(t, 10, len(rows))
		for i, e := range rows {
			assert.Equal(t, i, e)
		}

		rows = rows[:0]
		_ = vec.Foreach(op, sels)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, 2, rows[0])
		assert.Equal(t, 6, rows[1])

		rows = rows[:0]
		_ = vec.ForeachWindow(2, 6, op, nil)
		assert.Equal(t, []int{2, 3, 4, 5, 6, 7}, rows)
		rows = rows[:0]
		_ = vec.ForeachWindow(2, 6, op, sels)
		assert.Equal(t, []int{2, 6}, rows)
		rows = rows[:0]
		_ = vec.ForeachWindow(3, 6, op, sels)
		assert.Equal(t, []int{6}, rows)
		rows = rows[:0]
		_ = vec.ForeachWindow(3, 3, op, sels)
		assert.Equal(t, []int{}, rows)

		vec.Close()
	}
}

func TestVector6(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	sels := nulls.NewWithSize(1)
	sels.Add(uint64(2), uint64(6))
	f := func(vecType types.Type, nullable bool) {
		vec := MockVector(vecType, 10, false, nil)
		if nullable {
			vec.Update(4, nil, true)
		}
		bias := 0
		win := vec.Window(bias, 8)
		assert.Equal(t, 8, win.Length())
		rows := make([]int, 0)
		op := func(v any, _ bool, row int) (err error) {
			rows = append(rows, row)
			assert.Equal(t, vec.Get(row+bias), v)
			return
		}
		_ = win.Foreach(op, nil)
		assert.Equal(t, 8, len(rows))
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, rows)
		rows = rows[:0]
		_ = win.ForeachWindow(2, 3, op, nil)
		assert.Equal(t, 3, len(rows))
		assert.Equal(t, []int{2, 3, 4}, rows)

		rows = rows[:0]
		_ = win.Foreach(op, sels)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, 2, rows[0])
		assert.Equal(t, 6, rows[1])

		rows = rows[:0]
		_ = win.ForeachWindow(2, 6, op, sels)
		assert.Equal(t, []int{2, 6}, rows)
		rows = rows[:0]
		_ = win.ForeachWindow(3, 4, op, sels)
		assert.Equal(t, []int{6}, rows)
		rows = rows[:0]
		_ = win.ForeachWindow(3, 3, op, sels)
		assert.Equal(t, []int{}, rows)

		bias = 1
		win = vec.Window(bias, 8)

		op2 := func(v any, _ bool, row int) (err error) {
			rows = append(rows, row)
			// t.Logf("row=%d,v=%v", row, v)
			// t.Logf("row=%d, winv=%v", row, win.Get(row))
			// t.Logf("row+bias=%d, rawv=%v", row+bias, vec.Get(row+bias))
			assert.Equal(t, vec.Get(row+bias), v)
			assert.Equal(t, win.Get(row), v)
			return
		}
		rows = rows[:0]
		_ = win.Foreach(op2, nil)
		assert.Equal(t, 8, len(rows))
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, rows)
		rows = rows[:0]
		_ = win.ForeachWindow(2, 3, op, nil)
		assert.Equal(t, 3, len(rows))
		assert.Equal(t, []int{2, 3, 4}, rows)
		rows = rows[:0]
		_ = win.Foreach(op, sels)
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, 2, rows[0])
		assert.Equal(t, 6, rows[1])

		rows = rows[:0]
		_ = win.ForeachWindow(2, 6, op, sels)
		assert.Equal(t, []int{2, 6}, rows)
		rows = rows[:0]
		_ = win.ForeachWindow(3, 4, op, sels)
		assert.Equal(t, []int{6}, rows)
		rows = rows[:0]
		_ = win.ForeachWindow(3, 3, op, sels)
		assert.Equal(t, []int{}, rows)

		vec.Close()
	}
	for _, vecType := range vecTypes {
		f(vecType, false)
		f(vecType, true)
	}
}

func TestVector7(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	testF := func(typ types.Type, nullable bool) {
		vec := MockVector(typ, 10, false, nil)
		if nullable {
			vec.Append(nil, true)
		}
		vec2 := MockVector(typ, 10, false, nil)
		vec3 := MakeVector(typ, common.DefaultAllocator)
		vec3.Extend(vec)
		assert.Equal(t, vec.Length(), vec3.Length())
		vec3.Extend(vec2)
		assert.Equal(t, vec.Length()+vec2.Length(), vec3.Length())
		for i := 0; i < vec3.Length(); i++ {
			if i >= vec.Length() {
				assert.Equal(t, vec2.Get(i-vec.Length()), vec3.Get(i))
			} else {
				if vec.IsNull(i) {
					assert.Equal(t, true, vec3.IsNull(i))
				} else {
					assert.Equal(t, vec.Get(i), vec3.Get(i))
				}
			}
		}

		vec4 := MakeVector(typ, common.DefaultAllocator)
		cnt := 5
		if nullable {
			cnt = 6
		}
		vec4.ExtendWithOffset(vec, 5, cnt)
		assert.Equal(t, cnt, vec4.Length())
		// t.Log(vec4.String())
		// t.Log(vec.String())
		for i := 0; i < cnt; i++ {
			assert.Equal(t, vec.Get(i+5), vec4.Get(i))
		}

		vec.Close()
		vec2.Close()
		vec3.Close()
	}
	for _, typ := range vecTypes {
		testF(typ, true)
		testF(typ, false)
	}
}

func TestVector8(t *testing.T) {
	defer testutils.AfterTest(t)()
	vec := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer vec.Close()
	vec.Append(int32(0), false)
	vec.Append(int32(1), false)
	vec.Append(int32(2), false)
	vec.Append(nil, true)
	vec.Append(int32(4), false)
	vec.Append(int32(5), false)
	assert.True(t, vec.IsNull(3))
	vec.Delete(1)
	assert.True(t, vec.IsNull(2))
	vec.Delete(3)
	assert.True(t, vec.IsNull(2))
	vec.Update(1, nil, true)
	assert.True(t, vec.IsNull(1))
	assert.True(t, vec.IsNull(2))
	vec.Append(nil, true)
	assert.True(t, vec.IsNull(1))
	assert.True(t, vec.IsNull(2))
	assert.True(t, vec.IsNull(4))
	vec.Compact(roaring.BitmapOf(0, 2))
	assert.Equal(t, 3, vec.Length())
	assert.True(t, vec.IsNull(0))
	assert.True(t, vec.IsNull(2))
	t.Log(vec.String())
}

func TestVector9(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := withAllocator(Options{})
	vec := NewVector(types.T_varchar.ToType(), opts)
	vec.Append([]byte("h1"), false)
	vec.Append([]byte("h22"), false)
	vec.Append([]byte("h333"), false)
	vec.Append(nil, true)
	vec.Append([]byte("h4444"), false)

	cloned := vec.CloneWindow(2, 2)
	assert.Equal(t, 2, cloned.Length())
	assert.Equal(t, vec.Get(2), cloned.Get(0))
	assert.Equal(t, vec.Get(3), cloned.Get(1))
	cloned.Close()
	vec.Close()
	assert.Zero(t, opts.Allocator.CurrNB())
}

func TestCompact(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := withAllocator(Options{})
	vec := NewVector(types.T_varchar.ToType(), opts)

	vec.Append(nil, true)
	t.Log(vec.String())
	deletes := roaring.BitmapOf(0)
	//{null}
	vec.Compact(deletes)
	//{}
	assert.Equal(t, 0, vec.Length())

	vec.Append(nil, true)
	vec.Append(nil, true)
	vec.Append(nil, true)
	deletes = roaring.BitmapOf(0, 1)
	//{n,n,n}
	vec.Compact(deletes)
	//{n}
	assert.Equal(t, 1, vec.Length())
	assert.True(t, vec.IsNull(0))

	vec.Append([]byte("var"), false)
	vec.Append(nil, true)
	vec.Append([]byte("var"), false)
	vec.Append(nil, true)
	//{null,var,null,var,null}
	deletes = roaring.BitmapOf(1, 3)
	vec.Compact(deletes)
	//{null,null,null}
	assert.Equal(t, 3, vec.Length())
	assert.True(t, vec.IsNull(0))
	assert.True(t, vec.IsNull(1))
	assert.True(t, vec.IsNull(2))
	vec.Close()
}

func BenchmarkVectorExtend(t *testing.B) {
	vec1 := MockVector(types.T_int32.ToType(), 0, true, nil)
	vec2 := MockVector(types.T_int32.ToType(), 1, true, nil)
	defer vec1.Close()
	defer vec2.Close()

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		vec1.Extend(vec2)
	}
}

func TestForeachWindowFixed(t *testing.T) {
	vec1 := MockVector(types.T_uint32.ToType(), 2, false, nil)
	defer vec1.Close()
	vec1.Append(nil, true)

	cnt := 0
	op := func(v uint32, isNull bool, row int) (err error) {
		t.Logf("v=%v,null=%v,row=%d", v, isNull, row)
		cnt++
		if cnt == vec1.Length() {
			assert.True(t, isNull)
		} else {
			assert.Equal(t, vec1.Get(row).(uint32), v)
		}
		return
	}
	ForeachWindowFixed(vec1.GetDownstreamVector(), 0, vec1.Length(), false, op, nil, nil)
	assert.Equal(t, vec1.Length(), cnt)
}

func TestForeachWindowBytes(t *testing.T) {
	vec1 := MockVector(types.T_varchar.ToType(), 2, false, nil)
	defer vec1.Close()
	vec1.Append(nil, true)

	cnt := 0
	op := func(v []byte, isNull bool, row int) (err error) {
		t.Logf("v=%v,null=%v,row=%d", v, isNull, row)
		cnt++
		if cnt == vec1.Length() {
			assert.True(t, isNull)
		} else {
			assert.Equal(t, 0, bytes.Compare(v, vec1.Get(row).([]byte)))
		}
		return
	}
	ForeachWindowVarlen(vec1.GetDownstreamVector(), 0, vec1.Length(), false, op, nil, nil)
	assert.Equal(t, vec1.Length(), cnt)
}

func BenchmarkForeachVector(b *testing.B) {
	rows := 1000
	int64s := MockVector2(types.T_int64.ToType(), rows, 0)
	defer int64s.Close()
	b.Run("int64-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			int64s.Foreach(func(any, bool, int) error {
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
	defer chars.Close()
	b.Run("chars-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			chars.Foreach(func(any, bool, int) error {
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
	defer vec.Close()
	b.Run("int64-bytes", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ForeachWindowBytes(vec.GetDownstreamVector(), 0, vec.Length(), func(v []byte, isNull bool, row int) (err error) {
				return
			}, nil)
		}
	})
	b.Run("int64-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec.Foreach(func(any, bool, int) error {
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
	defer vec.Close()
	b.Run("func-new", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ForeachVectorWindow(vec, 0, vec.Length(), MakeForeachVectorOp(vec.GetType().Oid, funcs), nil, nil)
		}
	})
	b.Run("func-old", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec.Foreach(func(any, bool, int) (err error) {
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
		assert.Equal(t, vec.Get(row), v)
		return
	}
}

func TestForeachSelectBitmap(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	sels := nulls.NewWithSize(1)
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
	defer vec2.Close()

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
	vecs := make([]*vectorWrapper, 0, len(allTypes))
	for _, typ := range allTypes {
		vec := pool.GetVector(&typ)
		assert.Equal(t, *vec.GetType(), typ)
		vecs = append(vecs, vec)
	}

	assert.Equal(t, 0, pool.Allocated())

	for _, vec := range vecs {
		vec.PreExtend(100)
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
		vec.Close()
	}

	t.Log(pool.String())
	assert.Equal(t, allocated, pool.Allocated())
}

func TestVectorPool2(t *testing.T) {
	pool := NewVectorPool(t.Name(), 10, WithAllocationLimit(1024))
	typ := types.T_int32.ToType()
	vec := pool.GetVector(&typ)
	vec.PreExtend(300)
	t.Log(pool.String())
	assert.Less(t, 1024, pool.Allocated())
	vec.Close()
	assert.Equal(t, 0, pool.Allocated())
}

func TestVectorPool3(t *testing.T) {
	pool := NewVectorPool(t.Name(), 10)
	typ := types.T_int32.ToType()
	vec1 := NewVector(typ)
	vec1.Append(int32(1), false)
	vec1.Append(int32(2), false)
	vec1.Append(int32(3), false)

	vec2 := vec1.CloneWindowWithPool(0, 3, pool)
	t.Log(vec2.PPString(0))
	vec1.Close()
	vec2.Close()
}

func TestConstNullVector(t *testing.T) {
	vec := NewConstNullVector(types.T_int32.ToType(), 10, common.DefaultAllocator)
	defer vec.Close()
	assert.Equal(t, 10, vec.Length())
	assert.True(t, vec.IsConstNull())

	for i := 0; i < vec.Length(); i++ {
		assert.True(t, vec.IsNull(i))
	}

	var w bytes.Buffer
	_, err := vec.WriteTo(&w)
	assert.NoError(t, err)

	vec2 := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer vec2.Close()
	_, err = vec2.ReadFrom(&w)
	assert.NoError(t, err)
	assert.Equal(t, 10, vec2.Length())
	assert.True(t, vec2.IsConstNull())
	for i := 0; i < vec2.Length(); i++ {
		assert.True(t, vec2.IsNull(i))
	}

	vecw := vec.Window(0, 5)
	assert.Equal(t, 5, vecw.Length())
	assert.True(t, vecw.IsConstNull())
	for i := 0; i < vecw.Length(); i++ {
		assert.True(t, vecw.IsNull(i))
	}
}

func TestConstVector(t *testing.T) {
	vec := NewConstFixed[int32](types.T_int32.ToType(), int32(1), 10)
	defer vec.Close()

	assert.Equal(t, 10, vec.Length())
	assert.True(t, vec.IsConst())
	assert.False(t, vec.IsConstNull())

	for i := 0; i < vec.Length(); i++ {
		v := vec.Get(i).(int32)
		assert.Equal(t, int32(1), v)
	}

	vec2 := NewConstBytes(types.T_char.ToType(), []byte("abc"), 10)
	defer vec2.Close()
	assert.Equal(t, 10, vec2.Length())
	assert.True(t, vec2.IsConst())
	assert.False(t, vec2.IsConstNull())

	for i := 0; i < vec2.Length(); i++ {
		assert.Equal(t, []byte("abc"), vec2.Get(i).([]byte))
	}

	var w bytes.Buffer
	_, err := vec.WriteTo(&w)
	assert.NoError(t, err)

	vec3 := MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer vec3.Close()
	_, err = vec3.ReadFrom(&w)
	assert.NoError(t, err)
	assert.True(t, vec3.IsConst())
	assert.False(t, vec3.IsConstNull())
	assert.Equal(t, 10, vec3.Length())
	for i := 0; i < vec3.Length(); i++ {
		assert.Equal(t, int32(1), vec3.Get(i).(int32))
	}

	w.Reset()
	_, err = vec2.WriteTo(&w)
	assert.NoError(t, err)

	vec4 := MakeVector(types.T_char.ToType(), common.DefaultAllocator)
	defer vec4.Close()
	_, err = vec4.ReadFrom(&w)
	assert.NoError(t, err)
	assert.True(t, vec4.IsConst())
	assert.False(t, vec4.IsConstNull())
	assert.Equal(t, 10, vec4.Length())
	for i := 0; i < vec4.Length(); i++ {
		assert.Equal(t, []byte("abc"), vec4.Get(i).([]byte))
	}

	vecw := vec.Window(0, 5)
	assert.Equal(t, 5, vecw.Length())
	assert.True(t, vecw.IsConst())
	assert.False(t, vecw.IsConstNull())
	for i := 0; i < vecw.Length(); i++ {
		assert.Equal(t, int32(1), vecw.Get(i).(int32))
	}

	vecw4 := vec4.Window(0, 5)
	assert.Equal(t, 5, vecw4.Length())
	assert.True(t, vecw4.IsConst())
	assert.False(t, vecw4.IsConstNull())
	for i := 0; i < vecw4.Length(); i++ {
		assert.Equal(t, []byte("abc"), vecw4.Get(i).([]byte))
	}

}
