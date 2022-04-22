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
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	v "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mock"

	"github.com/stretchr/testify/assert"
)

func TestStrVector(t *testing.T) {
	colTypes := mock.MockColTypes(14)
	capacity := uint64(10000)
	vecs := make([]*StrVector, 0)
	for i, colType := range colTypes {
		if i < 12 {
			continue
		}
		vec := NewStrVector(colType, capacity)
		vecs = append(vecs, vec)
	}
	assert.Equal(t, container.StrVec, NewVector(colTypes[12], capacity).GetType())
	vec0 := vecs[0]
	assert.False(t, vec0.IsReadonly())
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(num int) {
			assert.Nil(t, vec0.Append(1, [][]byte{[]byte(strconv.Itoa(num))}))
			wg.Add(1)
			go func(prev int) {
				view := vec0.GetLatestView()
				assert.True(t, view.Length() >= prev)
				if prev > 0 {
					_, err := vec0.GetValue(prev - 1)
					assert.Nil(t, err)
				}
				wg.Done()
			}(vec0.Length())
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 10000, vec0.Length())
	assert.True(t, vec0.IsReadonly())
	assert.False(t, vec0.HasNull())

	ref, err := vec0.SliceReference(0, 4)
	assert.Nil(t, err)
	assert.Equal(t, 4, ref.Length())
	//assert.Equal(t, 4, ref.Capacity())
	assert.True(t, ref.(IVector).IsReadonly())
	assert.False(t, ref.HasNull())

	vvec := v.New(colTypes[13])
	assert.Nil(t, v.Append(vvec, make([][]byte, 10010)))
	nulls.Add(vvec.Nsp, 2, 3)
	vec1 := vecs[1]
	_, err = vec1.AppendVector(vvec, 0)
	assert.Nil(t, err)
	assert.True(t, vec1.HasNull())
	assert.True(t, vec1.IsReadonly())
	_, err = vec1.AppendVector(vvec, 10011)
	assert.NotNil(t, err)
	_, err = vec1.AppendVector(vvec, 0)
	assert.NotNil(t, err)
	view := vec1.GetLatestView()
	assert.True(t, view.HasNull())

	vvec = v.New(colTypes[13])
	assert.Nil(t, v.Append(vvec, make([][]byte, 9999)))
	nulls.Add(vvec.Nsp, 0)
	vecs[1] = NewStrVector(colTypes[13], capacity)
	vec2 := vecs[1]
	_, err = vec2.AppendVector(vvec, 0)
	assert.Nil(t, err)
	_, err = vec2.SliceReference(0, 1)
	assert.NotNil(t, err)
	col := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		dat := vvec.Col.(*types.Bytes).Get(int64(i))
		col = append(col, dat)
	}
	err = vec2.Append(4, col)
	assert.NotNil(t, err)
	_, err = vec2.GetValue(9999)
	assert.NotNil(t, err)
	_, err = vec2.CopyToVector()
	assert.NotNil(t, err)
	_, err = vec2.CopyToVectorWithBuffer(nil, bytes.NewBuffer(make([]byte, 0)))
	assert.NotNil(t, err)
	_, err = vec2.AppendVector(vvec, 0)
	assert.Nil(t, err)
	ref1, err := vec2.SliceReference(0, 1)
	assert.Nil(t, err)
	assert.True(t, ref1.HasNull())
	rov, err := vec2.CopyToVector()
	assert.Nil(t, err)
	assert.Equal(t, 10000, v.Length(rov))
	assert.True(t, nulls.Any(rov.Nsp))
	rov2, err := vec2.CopyToVectorWithBuffer(nil, bytes.NewBuffer(make([]byte, 0)))
	assert.Nil(t, err)
	assert.Equal(t, 10000, v.Length(rov2))
	assert.True(t, nulls.Any(rov2.Nsp))

	newCap := uint64(1024 * 1024)
	vvec = v.New(colTypes[13])
	err = v.Append(vvec, [][]byte{[]byte(strconv.Itoa(0)), []byte(strconv.Itoa(1)), []byte(strconv.Itoa(2)), []byte(strconv.Itoa(3)), []byte(strconv.Itoa(4))})
	assert.Nil(t, err)
	nulls.Add(vvec.Nsp, 2, 3)
	var lens []int
	var vals [][]byte
	var ro []bool
	var searchWg sync.WaitGroup
	var mtx sync.Mutex
	vec01 := NewStrVector(colTypes[13], newCap)
	loopCnt := 1000
	for i := 0; i < loopCnt; i++ {
		wg.Add(1)
		go func() {
			_, err := vec01.AppendVector(vvec, 0)
			assert.Nil(t, err)
			wg.Done()
		}()
		searchWg.Add(1)
		go func() {
			view := vec01.GetLatestView()
			if view.Length() > 0 {
				v, err := view.GetValue(view.Length() - 1)
				assert.Nil(t, err)
				assert.Equal(t, []byte(strconv.Itoa(4)), v)
				mtx.Lock()
				ro = append(ro, view.IsReadonly())
				vals = append(vals, v.([]byte))
				lens = append(lens, view.Length())
				mtx.Unlock()
			}
			searchWg.Done()
		}()
	}

	wg.Wait()
	searchWg.Wait()
	assert.Equal(t, v.Length(vvec)*loopCnt, vec01.Length())
	assert.False(t, vec01.IsReadonly())
	node := StrVectorConstructor(common.NewMemFile(4*100), false, func(node base.IMemoryNode) {
		// do nothing
	})
	nvec := node.(*StrVector)
	nvec.PlacementNew(colTypes[12])
	nvec.PlacementNew(colTypes[13])
	assert.Equal(t, 50, nvec.Capacity())
	assert.Equal(t, uint64(400), nvec.GetMemoryCapacity())
	nvec.UseCompress = true
	assert.Equal(t, uint64(400), nvec.GetMemoryCapacity())
	assert.Equal(t, uint64(0), nvec.GetMemorySize())
	nvec.FreeMemory()
	assert.Nil(t, nvec.Close())
	assert.Equal(t, uint64(400), nvec.GetMemoryCapacity())

	vecs = make([]*StrVector, 0)
	for i, colType := range colTypes {
		if i < 12 {
			continue
		}
		vec := NewStrVector(colType, capacity)
		vecs = append(vecs, vec)
		assert.Equal(t, 0, vec.NullCnt())
		_, err = vec.IsNull(10000)
		assert.NotNil(t, err)
		isn, err := vec.IsNull(0)
		assert.False(t, isn)
		rov, err := MockVector(colType, 1000).CopyToVector()
		assert.Nil(t, err)
		nulls.Add(rov.Nsp, 0, 1)
		_, err = vec.AppendVector(rov, 0)
		assert.Nil(t, err)
		isn, err = vec.IsNull(1)
		assert.True(t, isn)
		_, err = vec.GetValue(999)
		assert.Nil(t, err)
		_, err = vec.GetValue(1000)
		assert.NotNil(t, err)
		col := make([][]byte, 1000)
		for i := 0; i < 1000; i++ {
			dat := rov.Col.(*types.Bytes).Get(int64(i))
			col = append(col, dat)
		}
		err = vec.Append(1000, col)
		assert.Nil(t, err)
		assert.Equal(t, 2000, vec.Length())
		rov_, err := MockVector(colType, 8000).CopyToVector()
		assert.Nil(t, err)
		col = make([][]byte, 8000)
		for i := 0; i < 8000; i++ {
			dat := rov_.Col.(*types.Bytes).Get(int64(i))
			col = append(col, dat)
		}
		err = vec.Append(8000, col)
		assert.Nil(t, err)
		err = vec.Append(1, rov_.Col)
		assert.NotNil(t, err)
		err = vec.SetValue(0, []byte("xxx"))
		assert.NotNil(t, err)
		isn, err = vec.IsNull(1)
		assert.True(t, isn)
		vec.ResetReadonly()
		assert.False(t, vec.IsReadonly())
		isn, err = vec.IsNull(1)
		assert.True(t, isn)
		assert.Equal(t, 10000, vec.Length())
		assert.Equal(t, 2, vec.NullCnt())
	}

	buf, err := vecs[0].Marshal()
	assert.Nil(t, err)
	tmpv := NewEmptyStrVector()
	assert.Nil(t, tmpv.Unmarshal(buf))
	assert.Equal(t, 10000, tmpv.Length())
	assert.True(t, tmpv.HasNull())

	nvec.VMask = &nulls.Nulls{}
	nvec.Data = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 0),
		Lengths: make([]uint32, 0),
	}
	buf, err = nvec.Marshal()
	assert.Nil(t, err)
	tmpv = NewEmptyStrVector()
	assert.Nil(t, tmpv.Unmarshal(buf))
	assert.True(t, tmpv.Type.Eq(colTypes[13]))

	size := uint64(100)
	vecm := NewStrVector(types.Type{Oid: types.T(types.T_varchar), Size: 24}, size)
	assert.Equal(t, int(size), vecm.Capacity())
	assert.Equal(t, 0, vecm.Length())

	str0 := "str0"
	strs := [][]byte{[]byte(str0)}
	err = vecm.Append(len(strs), strs)
	assert.Nil(t, err)
	assert.Equal(t, len(strs), vecm.Length())
	assert.False(t, vecm.IsReadonly())

	buf, err = vecm.Marshal()
	assert.Nil(t, err)

	vecum := NewStrVector(types.Type{Oid: types.T(types.T_varchar), Size: 24}, size)
	err = vecum.Unmarshal(buf)
	assert.Nil(t, err)

	f, err := os.Create("/tmp/teststrvec")
	_, err = vecs[1].WriteTo(f)
	assert.Nil(t, err)
	assert.Nil(t, f.Close())
	f, err = os.Open("/tmp/teststrvec")
	tmpv = NewEmptyStrVector()
	_, err = tmpv.ReadFrom(f)
	assert.Nil(t, f.Close())
	assert.Nil(t, err)
	assert.Equal(t, 10000, tmpv.Length())
	assert.True(t, tmpv.HasNull())
}
