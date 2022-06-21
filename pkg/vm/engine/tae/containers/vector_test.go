package containers

import (
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

func withAllocator(opt *Options) *Options {
	if opt == nil {
		opt = new(Options)
	}
	opt.Allocator = stl.NewSimpleAllocator()
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
	opt := withAllocator(nil)
	var vec Vector
	vec = MakeVector(types.Type_INT32.ToType(), false, opt)
	vec.Append(int32(12))
	vec.Append(int32(32))
	assert.False(t, vec.Nullable())
	vec.AppendMany(int32(1), int32(100))
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, int32(12), vec.Get(0).(int32))
	assert.Equal(t, int32(32), vec.Get(1).(int32))
	assert.Equal(t, int32(1), vec.Get(2).(int32))
	assert.Equal(t, int32(100), vec.Get(3).(int32))
	vec2 := NewVector[int32](types.Type_INT32.ToType(), false)
	vec2.Extend(vec)
	assert.Equal(t, 4, vec2.Length())
	assert.Equal(t, int32(12), vec2.Get(0).(int32))
	assert.Equal(t, int32(32), vec2.Get(1).(int32))
	assert.Equal(t, int32(1), vec2.Get(2).(int32))
	assert.Equal(t, int32(100), vec2.Get(3).(int32))
	alloc := vec.GetAllocator()
	vec.Close()
	vec2.Close()
	assert.Equal(t, 0, alloc.Usage())
}

func TestVector2(t *testing.T) {
	opt := withAllocator(nil)
	var vec Vector
	vec = MakeVector(types.Type_INT64.ToType(), true, opt)
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
	assert.Zero(t, opt.Allocator.Usage())

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
	opts := withAllocator(nil)
	vec1 := MakeVector(types.Type_INT32.ToType(), false, opts)
	for i := 0; i < 100; i++ {
		vec1.Append(int32(i))
	}

	w := new(bytes.Buffer)
	_, err := vec1.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())

	vec2 := MakeVector(types.Type_INT32.ToType(), false, opts)
	_, err = vec2.ReadFrom(r)
	assert.NoError(t, err)

	assert.True(t, vec1.Equals(vec2))

	// t.Log(vec1.String())
	// t.Log(vec2.String())
	vec1.Close()
	vec2.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func TestVector4(t *testing.T) {
	vecTypes := types.MockColTypes(17)
	for _, vecType := range vecTypes {
		vec := MockVector(vecType, 10, true, true, nil)
		assert.Equal(t, 10, vec.Length())
		t.Log(vec.String())
	}
	vec := MakeVector(types.Type_INT32.ToType(), false)
	vec.Append(int32(1))
	vec.Append(int32(2))
	t.Log(vec.String())
}
