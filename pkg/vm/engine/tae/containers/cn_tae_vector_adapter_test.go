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
	assert.Equal(t, 2, vec.Length())

	vec.Close()
}
