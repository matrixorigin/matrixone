package containers

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
