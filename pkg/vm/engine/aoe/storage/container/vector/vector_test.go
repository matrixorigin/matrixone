package vector

import (
	"matrixone/pkg/container/types"
	v "matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
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

func TestStrVector(t *testing.T) {
	size := uint64(4)
	vec := NewStrVector(types.Type{types.T(types.T_varchar), 24, 0, 0}, size)
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
	assert.Equal(t, uint64(len(strs)*2*4+s), vec.(buf.IMemoryNode).GetMemorySize())
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
	assert.Equal(t, uint64((len(strs)+prevLen)*2*4+s), vec.(buf.IMemoryNode).GetMemorySize())
	assert.Equal(t, []byte(str0), vec.GetValue(0))
	assert.Equal(t, []byte(str1), vec.GetValue(1))
	assert.Equal(t, []byte(str2), vec.GetValue(2))
	assert.Equal(t, []byte(str3), vec.GetValue(3))

	nodeVec := vec.(buf.IMemoryNode)
	marshalled, err := nodeVec.Marshall()
	assert.Nil(t, err)

	mirror := NewEmptyStrVector()
	err = mirror.(buf.IMemoryNode).Unmarshall(marshalled)
	assert.Nil(t, err)

	assert.Equal(t, uint64((len(strs)+prevLen)*2*4+s), mirror.(buf.IMemoryNode).GetMemorySize())
	assert.Equal(t, []byte(str0), mirror.GetValue(0))
	assert.Equal(t, []byte(str1), mirror.GetValue(1))
	assert.Equal(t, []byte(str2), mirror.GetValue(2))
	assert.Equal(t, []byte(str3), mirror.GetValue(3))
	assert.Equal(t, 4, mirror.Length())
	assert.True(t, mirror.IsReadonly())

	view := mirror.GetLatestView()
	assert.Equal(t, uint64((len(strs)+prevLen)*2*4+s), view.(buf.IMemoryNode).GetMemorySize())
	assert.Equal(t, []byte(str0), view.GetValue(0))
	assert.Equal(t, []byte(str1), view.GetValue(1))
	assert.Equal(t, []byte(str2), view.GetValue(2))
	assert.Equal(t, []byte(str3), view.GetValue(3))
	assert.Equal(t, 4, view.Length())
	assert.True(t, mirror.IsReadonly())

	ref := vec.SliceReference(1, 3)
	assert.Equal(t, 2, ref.Length())
	assert.Equal(t, []byte(str1), ref.GetValue(0))
	assert.Equal(t, []byte(str2), ref.GetValue(1))

	fname := "/tmp/xxstrvec"
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0666)
	assert.Nil(t, err)
	_, err = nodeVec.WriteTo(f)
	assert.Nil(t, err)
	f.Close()

	f, err = os.OpenFile(fname, os.O_RDONLY, 0666)
	assert.Nil(t, err)
	builtVec := NewEmptyStrVector().(IVectorNode)
	_, err = builtVec.ReadFrom(f)
	assert.Nil(t, err)
	assert.Equal(t, []byte(str0), builtVec.GetValue(0))
	assert.Equal(t, []byte(str1), builtVec.GetValue(1))
	assert.Equal(t, []byte(str2), builtVec.GetValue(2))
	assert.Equal(t, []byte(str3), builtVec.GetValue(3))
	assert.Equal(t, 4, builtVec.Length())
	assert.True(t, builtVec.IsReadonly())
	f.Close()
}

func TestWrapper(t *testing.T) {
	t0 := types.Type{types.T(types.T_varchar), 24, 0, 0}
	t1 := types.Type{types.T_int32, 4, 4, 0}
	rows := uint64(100)
	vec0 := MockVector(t0, rows)
	vec1 := MockVector(t1, rows)
	v0 := vec0.CopyToVector()
	v1 := vec1.CopyToVector()
	w0 := NewVectorWrapper(v0)
	w1 := NewVectorWrapper(v1)
	assert.Equal(t, int(rows), w0.Length())
	assert.Equal(t, int(rows), w1.Length())

	fname := "/tmp/vectorwrapper"
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0666)
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

	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(gm, mempool.New(1<<32, 8))
	f, err = os.OpenFile(fname, os.O_RDONLY, 0666)
	assert.Nil(t, err)
	assert.Nil(t, err)
	ww0 := NewEmptyWrapper(t0)
	// ww0.AllocSize = uint64(n)
	ww0.File = common.NewMemFile(int64(n))
	ref := uint64(1)
	nr, err := ww0.ReadWithProc(f, ref, proc)
	assert.Equal(t, n, nr)

	assert.Equal(t, int(rows), ww0.Length())
	refCnt := encoding.DecodeUint64(ww0.Vector.Data[0:mempool.CountSize])
	assert.Equal(t, ref, refCnt)
	assert.True(t, proc.Size() != 0)
	ww0.Vector.Free(proc)
	assert.True(t, proc.Size() == 0)
	f.Close()
}
