package containers

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAppend(t *testing.T) {

	opt := withAllocator(Options{})
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

	vec.Close()

}

func TestExtend(t *testing.T) {
	defer testutils.AfterTest(t)()
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

func TestVectorDelete(t *testing.T) {
	defer testutils.AfterTest(t)()
	vec := MakeVector(types.T_int32.ToType(), true)
	defer vec.Close()
	vec.Append(int32(0))
	vec.Append(int32(1))
	vec.Append(int32(2))
	vec.Append(types.Null{})
	vec.Append(int32(4))
	vec.Append(int32(5))
	assert.True(t, types.IsNull(vec.Get(3)))
	t.Log(vec.String())
	vec.Delete(1)
	assert.True(t, types.IsNull(vec.Get(2)))
	t.Log(vec.String())
	vec.Delete(3)
	assert.True(t, types.IsNull(vec.Get(2)))
	t.Log(vec.String())
	vec.Update(1, types.Null{})
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	t.Log(vec.String())
	vec.Append(types.Null{})
	t.Log(vec.String())
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	t.Log(vec.String())
	assert.True(t, types.IsNull(vec.Get(4)))
	vec.Compact(roaring.BitmapOf(0, 2))
	assert.Equal(t, 3, vec.Length())
	assert.True(t, types.IsNull(vec.Get(0)))
	assert.True(t, types.IsNull(vec.Get(2)))
	t.Log(vec.String())
}

func TestVectorForEachWindow(t *testing.T) {
	defer testutils.AfterTest(t)()
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

func TestVectorWindow(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes(17)
	sels := roaring.BitmapOf(2, 6)
	f := func(vecType types.Type, nullable bool) {
		vec := MockVector(vecType, 10, false, nullable, nil)
		t.Log(vec.String())
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

		t.Log(vec.String())
		t.Log(win.String())
		op2 := func(v any, row int) (err error) {
			rows = append(rows, row)
			t.Logf("row=%d,v=%v", row, v)
			t.Logf("row=%d, winv=%v", row, win.Get(row))
			t.Logf("row+bias=%d, rawv=%v", row+bias, vec.Get(row+bias))
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
		break
	}
}

func TestVector_WindowWithNulls(t *testing.T) {
	vec := cnVector.New(types.T_int8.ToType())
	vec.Append(int8(0), false, mpool.MustNewZero())
	vec.Append(int8(1), false, mpool.MustNewZero())
	vec.Append(int8(2), false, mpool.MustNewZero())
	vec.Append(int8(-1), true, mpool.MustNewZero())
	vec.Append(int8(6), false, mpool.MustNewZero())
	vec.Append(int8(-1), true, mpool.MustNewZero())
	vec.Append(int8(-1), true, mpool.MustNewZero())
	vec.Append(int8(6), false, mpool.MustNewZero())
	vec.Append(int8(7), false, mpool.MustNewZero())
	vec.Append(int8(8), false, mpool.MustNewZero())
	t.Log(vec.String())

	win := cnVector.New(types.T_int8.ToType())
	cnVector.Window(vec, 1, 7, win)

	t.Log(win.String())
}

func TestVectorCompact(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := withAllocator(Options{})
	vec := MakeVector(types.T_varchar.ToType(), true, opts)

	vec.Append(types.Null{})
	t.Log(vec.String())
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
	t.Log(vec.String())
	vec.Compact(deletes)
	t.Log(vec.String())
	//{n}
	assert.Equal(t, 1, vec.Length())
	assert.True(t, types.IsNull(vec.Get(0)))
	t.Log(vec.String())

	vec.Append([]byte("var"))
	vec.Append(types.Null{})
	vec.Append([]byte("var"))
	vec.Append(types.Null{})
	//{null,var,null,var,null}
	deletes = roaring.BitmapOf(1, 3)
	t.Log(vec.String())
	vec.Compact(deletes)
	t.Log(vec.String())
	//{null,null,null}
	assert.Equal(t, 3, vec.Length())
	assert.True(t, types.IsNull(vec.Get(0)))
	assert.True(t, types.IsNull(vec.Get(1)))
	assert.True(t, types.IsNull(vec.Get(2)))
	vec.Close()
}

func TestVectorCloneWithBuffer(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := withAllocator(Options{})
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

func Test(t *testing.T) {
	a := []int{1, 2, 3, 4}
	fmt.Println(a)
	fmt.Println(cap(a))

	a = append(a[:2], a[3:]...)
	a = a[0:len(a):len(a)]
	fmt.Println(a)
	fmt.Println(cap(a))
}
