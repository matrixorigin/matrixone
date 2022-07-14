package adaptors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitSet1(t *testing.T) {
	bs := NewBitSet(10)
	assert.Equal(t, 10, bs.Size())
	assert.True(t, bs.IsEmpty())

	bs.Add(2)
	assert.False(t, bs.IsEmpty())
	assert.Equal(t, 1, bs.GetCardinality())

	bs.Remove(2)
	assert.True(t, bs.IsEmpty())
	assert.Equal(t, 0, bs.GetCardinality())

	bs.AddRange(3, 8)
	assert.False(t, bs.IsEmpty())
	assert.Equal(t, 5, bs.GetCardinality())

	bs.AddRange(4, 7)
	assert.False(t, bs.IsEmpty())
	assert.Equal(t, 5, bs.GetCardinality())

	bs.RemoveRange(4, 7)
	assert.False(t, bs.IsEmpty())
	assert.Equal(t, 2, bs.GetCardinality())

	bs.Clear()
	assert.True(t, bs.IsEmpty())
	assert.Equal(t, 0, bs.GetCardinality())
}
