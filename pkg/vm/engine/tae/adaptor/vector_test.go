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
	t.Log(vec.String())
	vec2 := NewVector[int32](types.Type_INT32.ToType())
	vec2.Extend(vec)
	t.Log(vec2.String())
}
