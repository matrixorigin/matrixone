package container

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/stretchr/testify/assert"
)

func TestSpan1(t *testing.T) {
	arr := []int32{4, 3, 2, 1}
	var span stl.Vector[int32]
	span = NewStdSpan(arr)
	assert.Equal(t, 4, span.Length())
	assert.Equal(t, int32(4), span.Get(0))
	assert.Equal(t, int32(3), span.Get(1))
	assert.Equal(t, int32(2), span.Get(2))
	assert.Equal(t, int32(1), span.Get(3))
}
