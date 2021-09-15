package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRefs(t *testing.T) {
	helper := RefHelper{
		Refs:     int64(0),
		OnZeroCB: func() {

		},
	}
	assert.Equal(t, helper.RefCount(), int64(0))
	helper.Ref()
	assert.Equal(t, helper.RefCount(), int64(1))
	helper.Ref()
	helper.Unref()
	assert.Equal(t, helper.RefCount(), int64(1))
	helper.Unref()
	assert.Equal(t, helper.RefCount(), int64(0))
	assert.Panics(t, helper.Unref)
}
