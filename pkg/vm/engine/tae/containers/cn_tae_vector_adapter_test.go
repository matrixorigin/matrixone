package containers

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
