package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFile(t *testing.T) {
	mf := NewMemFile(0)
	stat := mf.Stat()
	assert.Equal(t, stat.Size(), int64(0))
	assert.Equal(t, stat.Name(), "")
	assert.Equal(t, stat.CompressAlgo(), 0)
	assert.Equal(t, stat.OriginSize(), int64(0))
	assert.Equal(t, mf.GetFileType(), MemFile)
	mf.Ref()
	mf.Unref()
	_, err := mf.Read(make([]byte, 0))
	assert.Nil(t, err)
}
