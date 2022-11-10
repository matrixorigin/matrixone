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
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func withAllocator(opt *Options) *Options {
	if opt == nil {
		opt = new(Options)
	}
	opt.Allocator = mpool.MustNewZero()
	return opt
}

// func checkFullyEqualVector(t *testing.T, v1, v2 Vector) {
// 	checkEqualVector(t, v1, v2)
// 	assert.Equal(t, v1.Capacity(), v2.Capacity())
// 	assert.Equal(t, v1.Allocated(), v2.Allocated())
// }

// func checkEqualVector(t *testing.T, v1, v2 Vector) {
// 	assert.Equal(t, v1.GetType(), v2.GetType())
// 	assert.Equal(t, v1.Length(), v2.Length())
// 	assert.Equal(t, v1.HasNull(), v2.HasNull())
// 	assert.Equal(t, v1.Nullable(), v2.Nullable())
// 	for i := 0; i < v1.Length(); i++ {
// 		assert.Equal(t, v1.Get(i), v2.Get(i))
// 	}
// }

func TestVector1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opt := withAllocator(nil)
	vec := MakeVector(types.T_int32.ToType(), false, opt)
	vec.Append(int32(12))
	vec.Append(int32(32))
	assert.False(t, vec.Nullable())
	vec.AppendMany(int32(1), int32(100))
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, int32(12), vec.Get(0).(int32))
	assert.Equal(t, int32(32), vec.Get(1).(int32))
	assert.Equal(t, int32(1), vec.Get(2).(int32))
	assert.Equal(t, int32(100), vec.Get(3).(int32))
	vec2 := NewVector[int32](types.T_int32.ToType(), false)
	vec2.Extend(vec)
	assert.Equal(t, 4, vec2.Length())
	assert.Equal(t, int32(12), vec2.Get(0).(int32))
	assert.Equal(t, int32(32), vec2.Get(1).(int32))
	assert.Equal(t, int32(1), vec2.Get(2).(int32))
	assert.Equal(t, int32(100), vec2.Get(3).(int32))
	vec.Close()
	vec2.Close()
	// XXX MPOOL
	// alloc := vec.GetAllocator()
	// assert.Equal(t, 0, alloc.CurrNB())
}

func TestVector2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opt := withAllocator(nil)
	vec := MakeVector(types.T_int64.ToType(), true, opt)
	t.Log(vec.String())
	assert.True(t, vec.Nullable())
	now := time.Now()
	for i := 10; i > 0; i-- {
		vec.Append(int64(i))
	}
	t.Log(time.Since(now))
	assert.Equal(t, 10, vec.Length())
	assert.False(t, vec.HasNull())
	vec.Append(types.Null{})
	assert.Equal(t, 11, vec.Length())
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))

	vec.Update(2, types.Null{})
	assert.Equal(t, 11, vec.Length())
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))
	assert.True(t, vec.IsNull(2))

	vec.Update(2, int64(22))
	assert.True(t, vec.HasNull())
	assert.True(t, vec.IsNull(10))
	assert.False(t, vec.IsNull(2))
	assert.Equal(t, any(int64(22)), vec.Get(2))

	vec.Update(10, int64(100))
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
	defer leaktest.AfterTest(t)()
	opts := withAllocator(nil)
	vec1 := MakeVector(types.T_int32.ToType(), false, opts)
	for i := 0; i < 100; i++ {
		vec1.Append(int32(i))
	}

	w := new(bytes.Buffer)
	_, err := vec1.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())

	vec2 := MakeVector(types.T_int32.ToType(), false, opts)
	_, err = vec2.ReadFrom(r)
	assert.NoError(t, err)

	assert.True(t, vec1.Equals(vec2))

	// t.Log(vec1.String())
	// t.Log(vec2.String())
	vec1.Close()
	vec2.Close()
	assert.Zero(t, opts.Allocator.CurrNB())
}

func TestVector4(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := MockVector(vecType, 1000, false, true, nil)
		assert.Equal(t, 1000, vec.Length())
		vec.Append(types.Null{})
		w := new(bytes.Buffer)
		_, err := vec.WriteTo(w)
		assert.NoError(t, err)
		srcBuf := w.Bytes()
		srcSize := len(srcBuf)
		destBuf := make([]byte, lz4.CompressBlockBound(srcSize))
		destBuf, err = compress.Compress(srcBuf, destBuf, compress.Lz4)
		assert.NoError(t, err)
		f := MockCompressedFile(destBuf, srcSize, compress.Lz4)
		vec2 := MakeVector(vecType, true)
		err = vec2.ReadFromFile(f, nil)
		assert.NoError(t, err)
		assert.True(t, vec.Equals(vec2))
		vec.Close()
		vec2.Close()
	}
	buffer := new(bytes.Buffer)
	for _, vecType := range vecTypes {
		vec := MockVector(vecType, 1000, false, true, nil)
		assert.Equal(t, 1000, vec.Length())
		vec.Append(types.Null{})
		w := new(bytes.Buffer)
		_, err := vec.WriteTo(w)
		assert.NoError(t, err)
		srcBuf := w.Bytes()
		srcSize := len(srcBuf)
		destBuf := make([]byte, lz4.CompressBlockBound(srcSize))
		destBuf, err = compress.Compress(srcBuf, destBuf, compress.Lz4)
		assert.NoError(t, err)
		f := MockCompressedFile(destBuf, srcSize, compress.Lz4)
		vec2 := MakeVector(vecType, true)
		err = vec2.ReadFromFile(f, buffer)
		assert.NoError(t, err)
		assert.True(t, vec.Equals(vec2))
		vec.Close()
		vec2.Close()
	}
}

func TestVector5(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	sels := roaring.BitmapOf(2, 6)
	for _, vecType := range vecTypes {
		vec := MockVector(vecType, 10, false, true, nil)
		rows := make([]int, 0)
		op := func(v any, row int) (err error) {
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
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	sels := roaring.BitmapOf(2, 6)
	f := func(vecType types.Type, nullable bool) {
		vec := MockVector(vecType, 10, false, nullable, nil)
		if nullable {
			vec.Update(4, types.Null{})
		}
		bias := 0
		win := vec.Window(bias, 8)
		assert.Equal(t, 8, win.Length())
		assert.Equal(t, 8, win.Capacity())
		rows := make([]int, 0)
		op := func(v any, row int) (err error) {
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

		op2 := func(v any, row int) (err error) {
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
	defer leaktest.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	testF := func(typ types.Type, nullable bool) {
		vec := MockVector(typ, 10, false, nullable, nil)
		if nullable {
			vec.Append(types.Null{})
		}
		vec2 := MockVector(typ, 10, false, nullable, nil)
		vec3 := MakeVector(typ, nullable)
		vec3.Extend(vec)
		assert.Equal(t, vec.Length(), vec3.Length())
		vec3.Extend(vec2)
		assert.Equal(t, vec.Length()+vec2.Length(), vec3.Length())
		for i := 0; i < vec3.Length(); i++ {
			if i >= vec.Length() {
				assert.Equal(t, vec2.Get(i-vec.Length()), vec3.Get(i))
			} else {
				assert.Equal(t, vec.Get(i), vec3.Get(i))
			}
		}

		vec4 := MakeVector(typ, nullable)
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
	defer leaktest.AfterTest(t)()
	vec := MakeVector(types.T_int32.ToType(), true)
	defer vec.Close()
	vec.Append(int32(0))
	vec.Append(int32(1))
	vec.Append(int32(2))
	vec.Append(types.Null{})
	vec.Append(int32(4))
	vec.Append(int32(5))
	assert.True(t, types.IsNull(vec.Get(3)))
	vec.Delete(1)
	assert.True(t, types.IsNull(vec.Get(2)))
	vec.Delete(3)
	assert.True(t, types.IsNull(vec.Get(2)))
	vec.Update(1, types.Null{})
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	vec.Append(types.Null{})
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	assert.True(t, types.IsNull(vec.Get(4)))
	vec.Compact(roaring.BitmapOf(0, 2))
	assert.Equal(t, 3, vec.Length())
	assert.True(t, types.IsNull(vec.Get(0)))
	assert.True(t, types.IsNull(vec.Get(2)))
	t.Log(vec.String())
}

func TestVector9(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opts := withAllocator(nil)
	vec := MakeVector(types.T_varchar.ToType(), true, opts)
	vec.Append([]byte("h1"))
	vec.Append([]byte("h22"))
	vec.Append([]byte("h333"))
	vec.Append(types.Null{})
	vec.Append([]byte("h4444"))

	cloned := vec.CloneWindow(2, 2)
	assert.Equal(t, 2, cloned.Length())
	assert.Equal(t, vec.Get(2), cloned.Get(0))
	assert.Equal(t, vec.Get(3), cloned.Get(1))
	cloned.Close()
	vec.Close()
	assert.Zero(t, opts.Allocator.CurrNB())
}

func TestCloneWithBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opts := withAllocator(nil)
	vec := MakeVector(types.T_varchar.ToType(), true, opts)
	vec.Append([]byte("h1"))
	vec.Append([]byte("h22"))
	vec.Append([]byte("h333"))
	vec.Append(types.Null{})
	vec.Append([]byte("h4444"))

	buffer := new(bytes.Buffer)
	cloned := CloneWithBuffer(vec, buffer)
	assert.True(t, vec.Equals(cloned))
	assert.Zero(t, cloned.Allocated())

	bs := vec.Bytes()
	buf := buffer.Bytes()
	res := bytes.Compare(bs.Storage, buf[:len(bs.Storage)])
	assert.Zero(t, res)
	res = bytes.Compare(bs.HeaderBuf(), buf[len(bs.Storage):len(bs.HeaderBuf())+len(bs.Storage)])
	assert.Zero(t, res)
}

func TestCompact(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opts := withAllocator(nil)
	vec := MakeVector(types.T_varchar.ToType(), true, opts)

	vec.Append(types.Null{})
	deletes := roaring.BitmapOf(0)
	//{null}
	vec.Compact(deletes)
	//{}
	assert.Equal(t, 0, vec.Length())

	vec.Append(types.Null{})
	vec.Append(types.Null{})
	vec.Append(types.Null{})
	deletes = roaring.BitmapOf(0, 1)
	//{n,n,n}
	vec.Compact(deletes)
	//{n}
	assert.Equal(t, 1, vec.Length())
	assert.True(t, types.IsNull(vec.Get(0)))

	vec.Append([]byte("var"))
	vec.Append(types.Null{})
	vec.Append([]byte("var"))
	vec.Append(types.Null{})
	//{null,var,null,var,null}
	deletes = roaring.BitmapOf(1, 3)
	vec.Compact(deletes)
	//{null,null,null}
	assert.Equal(t, 3, vec.Length())
	assert.True(t, types.IsNull(vec.Get(0)))
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	vec.Close()
}
