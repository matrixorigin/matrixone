package vector

import (
	"matrixone/pkg/container/types"
	v "matrixone/pkg/container/vector"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdVector(t *testing.T) {
	vecType := types.Type{types.T_int32, 4, 4, 0}
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

	ref := vec.SliceReference(1, 3)
	assert.Equal(t, 2, ref.Length())
	assert.Equal(t, 2, ref.Capacity())
	assert.Equal(t, int32(1), ref.GetValue(0))
	assert.Equal(t, int32(2), ref.GetValue(1))
	assert.False(t, ref.HasNull())
	assert.True(t, ref.IsReadonly())

	vvec := v.New(vecType)
	vvec.Append([]int32{0, 1, 2, 3, 4})
	vvec.Nsp.Add(2, 3)

	vec2 := NewStdVector(vecType, capacity)
	n, err := vec2.AppendVector(vvec, 0)
	assert.Nil(t, err)
	assert.Equal(t, capacity, uint64(n))
	assert.True(t, vec2.HasNull())
	assert.True(t, vec2.IsReadonly())
	assert.True(t, vec2.IsNull(2))
	assert.True(t, vec2.IsNull(3))

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
				v := view.GetValue(view.Length() - 1)
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
