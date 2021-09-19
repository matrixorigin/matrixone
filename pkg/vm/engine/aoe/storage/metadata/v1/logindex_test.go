package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogIndex(t *testing.T) {
	idx := LogIndex{
		ID:       1,
		Start:    0,
		Count:    0,
		Capacity: 4,
	}
	assert.False(t, idx.IsApplied())
	idx.Count = 4
	assert.True(t, idx.IsApplied())
	m, err := idx.Marshal()
	assert.Nil(t, err)
	var idx1 LogIndex
	assert.Nil(t, idx1.UnMarshall(make([]byte, 0)))
	assert.Nil(t, idx1.UnMarshall(m))
	assert.Equal(t, idx.String(), "(1,0,4,4)")
}
