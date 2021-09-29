package vector

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/nulls"
	v "matrixone/pkg/container/vector"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"sync"
	"testing"
)

func TestStdVector(t *testing.T) {
	schema := metadata.MockSchemaAll(12)
	capacity := uint64(10000)
	vecs := make([]*StdVector, 0)
	for _, colDef := range schema.ColDefs {
		vec := NewStdVector(colDef.Type, capacity)
		vecs = append(vecs, vec)
	}
	assert.Equal(t, dbi.StdVec, NewVector(schema.ColDefs[0].Type, capacity).GetType())
	vec0 := vecs[0]
	assert.False(t, vec0.IsReadonly())
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(num int) {
			assert.Nil(t, vec0.Append(1, []int8{int8(num)}))
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
	assert.Equal(t, 4, ref.Capacity())
	assert.True(t, ref.(IVector).IsReadonly())
	assert.False(t, ref.HasNull())

	vvec := v.New(schema.ColDefs[1].Type)
	assert.Nil(t, vvec.Append(make([]int16, 10010)))
	vvec.Nsp.Add(2, 3)
	vec1 := vecs[1]
	_, err = vec1.AppendVector(vvec, 0)
	assert.Nil(t, err)
	assert.True(t, vec1.HasNull())
	assert.True(t, vec1.IsReadonly())
	_, err = vec1.AppendVector(vvec, 10011)
	assert.NotNil(t, err)
	_, err = vec1.AppendVector(vvec, 0)
	assert.NotNil(t, err)
	err = vec1.SetValue(0, int16(1))
	assert.NotNil(t, err)
	view := vec1.GetLatestView()
	assert.True(t, view.HasNull())

	vvec = v.New(schema.ColDefs[2].Type)
	assert.Nil(t, vvec.Append(make([]int32, 9999)))
	vvec.Nsp.Add(0)
	vec2 := vecs[2]
	_, err = vec2.AppendVector(vvec, 0)
	assert.Nil(t, err)
	_, err = vec2.SliceReference(0, 1)
	assert.NotNil(t, err)
	err = vec2.Append(4, vvec.Col)
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
	assert.Equal(t, 10000, rov.Length())
	assert.True(t, rov.Nsp.Any())
	rov2, err := vec2.CopyToVectorWithBuffer(nil, bytes.NewBuffer(make([]byte, 0)))
	assert.Nil(t, err)
	assert.Equal(t, 10000, rov2.Length())
	assert.True(t, rov2.Nsp.Any())

	newCap := uint64(1024 * 1024)
	vvec = v.New(schema.ColDefs[2].Type)
	err = vvec.Append([]int32{0, 1, 2, 3, 4})
	assert.Nil(t, err)
	vvec.Nsp.Add(2, 3)
	var lens []int
	var vals []int32
	var ro []bool
	var searchWg sync.WaitGroup
	var mtx sync.Mutex
	vec01 := NewStdVector(schema.ColDefs[2].Type, newCap)
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
	node := StdVectorConstructor(common.NewMemFile(4*100), false, func(node buf.IMemoryNode) {
		// do nothing
	})
	nvec := node.(*StdVector)
	nvec.PlacementNew(schema.ColDefs[0].Type)
	nvec.PlacementNew(schema.ColDefs[2].Type)
	assert.Equal(t, 100, nvec.Capacity())
	assert.Equal(t, 400, nvec.dataBytes())
	assert.Equal(t, uint64(400), nvec.GetMemoryCapacity())
	nvec.UseCompress = true
	assert.Equal(t, uint64(400), nvec.GetMemoryCapacity())
	assert.Equal(t, uint64(0), nvec.GetMemorySize())
	nvec.FreeMemory()
	assert.Nil(t, nvec.Close())
	assert.Equal(t, uint64(400), nvec.GetMemoryCapacity())
	assert.Equal(t, 0, nvec.dataBytes())

	vecs = make([]*StdVector, 0)
	for _, colDef := range schema.ColDefs {
		vec := NewStdVector(colDef.Type, capacity)
		vecs = append(vecs, vec)
		rov, err := MockVector(colDef.Type, 1000).CopyToVector()
		assert.Nil(t, err)
		rov.Nsp.Add(0, 1)
		_, err = vec.AppendVector(rov, 0)
		assert.Nil(t, err)
		_, err = vec.GetValue(999)
		assert.Nil(t, err)
		_, err = vec.GetValue(1000)
		assert.NotNil(t, err)
		err = vec.Append(1000, rov.Col)
		assert.Nil(t, err)
		assert.Equal(t, 2000, vec.Length())
		rov_, err := MockVector(colDef.Type, 8000).CopyToVector()
		assert.Nil(t, err)
		err = vec.Append(8000, rov_.Col)
		assert.Nil(t, err)
		err = vec.Append(1, rov_.Col)
		assert.NotNil(t, err)
	}

	buf, err := vecs[0].Marshal()
	assert.Nil(t, err)
	tmpv := NewEmptyStdVector()
	assert.Nil(t, tmpv.Unmarshal(buf))
	assert.Equal(t, 10000, tmpv.Length())
	assert.True(t, tmpv.HasNull())

	nvec.VMask = &nulls.Nulls{}
	buf, err = nvec.Marshal()
	assert.Nil(t, err)
	tmpv = NewEmptyStdVector()
	assert.Nil(t, tmpv.Unmarshal(buf))
	assert.True(t, tmpv.Type.Eq(schema.ColDefs[2].Type))

	f, err := os.Create("/tmp/teststdvec")
	_, err = vecs[1].WriteTo(f)
	assert.Nil(t, err)
	assert.Nil(t, f.Close())
	f, err = os.Open("/tmp/teststdvec")
	tmpv = NewEmptyStdVector()
	_, err = tmpv.ReadFrom(f)
	assert.Nil(t, f.Close())
	assert.Nil(t, err)
	assert.Equal(t, 10000, tmpv.Length())
	assert.True(t, tmpv.HasNull())
}