// Copyright 2021 Matrix Origin
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

package vector

import (
	"bytes"
	"matrixone/pkg/container/types"
	v "matrixone/pkg/container/vector"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdVectorOld(t *testing.T) {
	vecType := types.Type{Oid: types.T_int32, Size: 4, Width: 4}
	capacity := uint64(4)
	vec := NewStdVector(vecType, capacity)
	assert.False(t, vec.IsReadonly())
	assert.Equal(t, 0, vec.Length())
	err := vec.Append(4, []int32{int32(0), int32(1), int32(2), int32(3)})
	assert.Nil(t, err)
	assert.Equal(t, 4, vec.Length())
	assert.True(t, vec.IsReadonly())
	err = vec.Append(1, []int32{int32(4)})
	assert.NotNil(t, err)
	assert.Equal(t, 4, vec.Length())
	assert.False(t, vec.HasNull())

	ref, err := vec.SliceReference(1, 3)
	assert.Equal(t, 2, ref.Length())
	assert.Equal(t, 2, ref.Capacity())
	//assert.Equal(t, int32(1), ref.GetValue(0))
	//assert.Equal(t, int32(2), ref.GetValue(1))
	assert.False(t, ref.HasNull())
	assert.True(t, ref.(IVector).IsReadonly())

	vvec := v.New(vecType)
	vvec.Append([]int32{0, 1, 2, 3, 4})
	vvec.Nsp.Add(2, 3)

	vec2 := NewStdVector(vecType, capacity)
	n, err := vec2.AppendVector(vvec, 0)
	assert.Nil(t, err)
	assert.Equal(t, capacity, uint64(n))
	assert.True(t, vec2.HasNull())
	assert.True(t, vec2.IsReadonly())
	res, err := vec2.IsNull(2)
	assert.Nil(t, err)
	assert.True(t, res)
	res, err = vec2.IsNull(3)
	assert.Nil(t, err)
	assert.True(t, res)

	newCap := uint64(1024 * 1024)
	var wg sync.WaitGroup
	var lens []int
	var vals []int32
	var ro []bool
	var searchWg sync.WaitGroup
	var mtx sync.Mutex
	vec01 := NewStdVector(vecType, newCap)
	loopCnt := 1000
	for i := 0; i < loopCnt; i++ {
		wg.Add(1)
		go func() {
			vec01.AppendVector(vvec, 0)
			wg.Done()
		}()
		searchWg.Add(1)
		go func() {
			view := vec01.GetLatestView()
			if view.Length() > 0 {
				v, err := view.GetValue(view.Length() - 1)
				assert.Nil(t, err)
				assert.Equal(t, int32(4), v)
				mtx.Lock()
				ro = append(ro, view.IsReadonly())
				vals = append(vals, v.(int32))
				lens = append(lens, view.Length())
				mtx.Unlock()
			}
			searchWg.Done()
		}()
	}

	wg.Wait()
	searchWg.Wait()
	assert.Equal(t, vvec.Length()*loopCnt, vec01.Length())
	assert.False(t, vec01.IsReadonly())
	// t.Log(lens)
	// t.Log(vals)
	// t.Log(ro)
	assert.Equal(t, 2000, vec01.NullCnt())
}

func TestStrVectorOld(t *testing.T) {
	size := uint64(4)
	vec := NewStrVector(types.Type{Oid: types.T(types.T_varchar), Size: 24}, size)
	assert.Equal(t, int(size), vec.Capacity())
	assert.Equal(t, 0, vec.Length())

	assert.False(t, vec.IsReadonly())
	str0 := "str0"
	str1 := "str1"
	str2 := "str2"
	str3 := "str3"
	strs := [][]byte{[]byte(str0), []byte(str1)}
	err := vec.Append(len(strs), strs)
	assert.Nil(t, err)
	assert.Equal(t, len(strs), vec.Length())
	assert.False(t, vec.IsReadonly())
	s := 0
	for _, str := range strs {
		s += len(str)
	}
	assert.Equal(t, uint64(len(strs)*2*4+s), vec.GetMemorySize())
	prevLen := len(strs)
	strs = [][]byte{[]byte(str2), []byte(str3)}
	err = vec.Append(len(strs), strs)
	assert.Nil(t, err)
	assert.Equal(t, prevLen+len(strs), vec.Length())
	assert.Equal(t, vec.Capacity(), vec.Length())
	assert.True(t, vec.IsReadonly())
	for _, str := range strs {
		s += len(str)
	}
	assert.Equal(t, uint64((len(strs)+prevLen)*2*4+s), vec.GetMemorySize())
	val, err := vec.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, []byte(str0), val)
	val, err = vec.GetValue(1)
	assert.Nil(t, err)
	assert.Equal(t, []byte(str1), val)
	val, err = vec.GetValue(2)
	assert.Nil(t, err)
	assert.Equal(t, []byte(str2), val)
	val, err = vec.GetValue(3)
	assert.Nil(t, err)
	assert.Equal(t, []byte(str3), val)

	marshalled, err := vec.Marshal()
	assert.Nil(t, err)

	mirror := NewEmptyStrVector()
	err = mirror.Unmarshal(marshalled)
	assert.Nil(t, err)

	assert.Equal(t, uint64((len(strs)+prevLen)*2*4+s), mirror.GetMemorySize())
	val, err = mirror.GetValue(0)
	assert.Equal(t, []byte(str0), val)
	//assert.Equal(t, []byte(str1), mirror.GetValue(1))
	//assert.Equal(t, []byte(str2), mirror.GetValue(2))
	val, err = mirror.GetValue(3)
	assert.Equal(t, []byte(str3), val)
	assert.Equal(t, 4, mirror.Length())
	assert.True(t, mirror.IsReadonly())

	view := mirror.GetLatestView()
	assert.Equal(t, uint64((len(strs)+prevLen)*2*4+s), view.(buf.IMemoryNode).GetMemorySize())
	val, err = view.GetValue(0)
	assert.Equal(t, []byte(str0), val)
	//assert.Equal(t, []byte(str1), view.GetValue(1))
	//assert.Equal(t, []byte(str2), view.GetValue(2))
	val, err = view.GetValue(3)
	assert.Equal(t, []byte(str3), val)
	assert.Equal(t, 4, view.Length())
	assert.True(t, mirror.IsReadonly())

	ref, err := vec.SliceReference(1, 3)
	assert.Nil(t, err)
	assert.Equal(t, 2, ref.Length())
	val, err = ref.GetValue(0)
	assert.Equal(t, []byte(str1), val)
	//assert.Equal(t, []byte(str2), ref.GetValue(1))

	fname := "/tmp/xxstrvec"
	f, err := os.Create(fname)
	assert.Nil(t, err)
	_, err = vec.WriteTo(f)
	assert.Nil(t, err)
	f.Close()

	f, err = os.OpenFile(fname, os.O_RDONLY, 0666)
	assert.Nil(t, err)
	builtVec := NewEmptyStrVector()
	_, err = builtVec.ReadFrom(f)
	assert.Nil(t, err)
	val, err = builtVec.GetValue(0)
	assert.Equal(t, []byte(str0), val)
	//assert.Equal(t, []byte(str1), builtVec.GetValue(1))
	//assert.Equal(t, []byte(str2), builtVec.GetValue(2))
	val, err = builtVec.GetValue(3)
	assert.Equal(t, []byte(str3), val)
	assert.Equal(t, 4, builtVec.Length())
	assert.True(t, builtVec.IsReadonly())
	f.Close()
}

func TestCopy(t *testing.T) {
	t0 := types.Type{Oid: types.T(types.T_varchar), Size: 24}
	t1 := types.Type{Oid: types.T_int32, Size: 4, Width: 4}
	rows := uint64(100)
	vec0 := MockVector(t0, rows)
	vec1 := MockVector(t1, rows)
	defer vec0.Close()
	defer vec1.Close()

	for i := 0; i < 4; i++ {
		v0_0, err := vec0.CopyToVectorWithBuffer(bytes.NewBuffer(make([]byte, 0)), bytes.NewBuffer(make([]byte, 0)))
		assert.Nil(t, err)
		assert.NotNil(t, v0_0)
		assert.Equal(t, int(rows), v0_0.Length())
		v0_1, err := vec0.CopyToVector()
		assert.Nil(t, err)
		assert.Equal(t, int(rows), v0_1.Length())
		for row := 0; row < int(rows); row++ {
			expected, err := vec0.GetValue(row)
			assert.Nil(t, err)
			value0_0 := v0_0.Col.(*types.Bytes).Get(int64(row))
			value0_1 := v0_1.Col.(*types.Bytes).Get(int64(row))
			assert.Equal(t, expected, value0_0)
			assert.Equal(t, expected, value0_1)
			// t.Log(string(value0_0))
			// t.Log(string(value0_1))
		}
	}

	for i := 0; i < 4; i++ {
		v1_0, err := vec1.CopyToVectorWithBuffer(bytes.NewBuffer(make([]byte, 0)), bytes.NewBuffer(make([]byte, 0)))
		assert.Nil(t, err)
		assert.NotNil(t, v1_0)
		assert.Equal(t, int(rows), v1_0.Length())
		v1_1, err := vec1.CopyToVector()
		assert.Nil(t, err)
		assert.Equal(t, int(rows), v1_1.Length())
		for row := 0; row < int(rows); row++ {
			expected, err := vec1.GetValue(row)
			assert.Nil(t, err)
			value1_0 := v1_0.Col.([]int32)[row]
			value1_1 := v1_1.Col.([]int32)[row]
			assert.Equal(t, expected, value1_0)
			assert.Equal(t, expected, value1_1)
		}
	}
}

func TestWrapper(t *testing.T) {
	t0 := types.Type{Oid: types.T(types.T_varchar), Size: 24}
	t1 := types.Type{Oid: types.T_int32, Size: 4, Width: 4}
	rows := uint64(100)
	vec0 := MockVector(t0, rows)
	vec1 := MockVector(t1, rows)
	defer vec0.Close()
	defer vec1.Close()
	v0, err := vec0.CopyToVector()
	assert.Nil(t, err)
	v1, err := vec1.CopyToVector()
	assert.Nil(t, err)
	w0 := NewVectorWrapper(v0)
	w1 := NewVectorWrapper(v1)
	assert.Equal(t, int(rows), w0.Length())
	assert.Equal(t, int(rows), w1.Length())

	fname := "/tmp/vectorwrapper"
	f, err := os.Create(fname)
	assert.Nil(t, err)
	n, err := w0.WriteTo(f)
	assert.Nil(t, err)
	f.Close()

	f, err = os.OpenFile(fname, os.O_RDONLY, 0666)
	assert.Nil(t, err)
	rw0 := NewEmptyWrapper(t0)
	// rw0.AllocSize = uint64(n)
	rw0.File = common.NewMemFile(int64(n))
	_, err = rw0.ReadFrom(f)
	assert.Nil(t, err)

	assert.Equal(t, int(rows), rw0.Length())
	f.Close()

	f, err = os.OpenFile(fname, os.O_RDONLY, 0666)
	assert.Nil(t, err)
	assert.Nil(t, err)
	ww0 := NewEmptyWrapper(t0)
	// ww0.AllocSize = uint64(n)
	ww0.File = common.NewMemFile(int64(n))
	nr, err := ww0.ReadWithBuffer(f, bytes.NewBuffer(make([]byte, 0)), bytes.NewBuffer(make([]byte, 0)))
	assert.Equal(t, n, nr)

	assert.Equal(t, int(rows), ww0.Length())
	f.Close()
}

func TestStrVector2(t *testing.T) {
	size := uint64(100)
	vec := NewStrVector(types.Type{Oid: types.T(types.T_varchar), Size: 24}, size)
	assert.Equal(t, int(size), vec.Capacity())
	assert.Equal(t, 0, vec.Length())

	str0 := "str0"
	strs := [][]byte{[]byte(str0)}
	err := vec.Append(len(strs), strs)
	assert.Nil(t, err)
	assert.Equal(t, len(strs), vec.Length())
	assert.False(t, vec.IsReadonly())

	buf, err := vec.Marshal()
	assert.Nil(t, err)

	vec2 := NewStrVector(types.Type{Oid: types.T(types.T_varchar), Size: 24}, size)
	err = vec2.Unmarshal(buf)
	assert.Nil(t, err)
}
