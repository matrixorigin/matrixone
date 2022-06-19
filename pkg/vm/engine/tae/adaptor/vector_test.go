package adaptor

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/stretchr/testify/assert"
)

func TestVector1(t *testing.T) {
	var vec Vector
	vec = NewVector[int32](types.Type_INT32.ToType())
	vec.Append(int32(12))
	vec.Append(int32(32))
	assert.False(t, vec.Nullable())
	vec.AppendMany(int32(1), int32(100))
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, int32(12), vec.Get(0).(int32))
	assert.Equal(t, int32(32), vec.Get(1).(int32))
	assert.Equal(t, int32(1), vec.Get(2).(int32))
	assert.Equal(t, int32(100), vec.Get(3).(int32))
	vec2 := NewVector[int32](types.Type_INT32.ToType())
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
