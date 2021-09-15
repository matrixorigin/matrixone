package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type O struct {
	BaseMvcc
}

func TestMVCC(t *testing.T) {
	mvcc0 := &O{}
	mvcc1 := &O{}
	mvcc2 := &O{}
	mvcc0.Unpin = func(i interface{}) {}
	mvcc1.Unpin = func(i interface{}) {}
	mvcc2.Unpin = func(i interface{}) {}
	mvcc0.Pin = func(i interface{}) {}
	mvcc1.Pin = func(i interface{}) {}
	mvcc2.Pin = func(i interface{}) {}
	mvcc0.GetObject = func() interface{} { return *mvcc0 }
	mvcc1.GetObject = func() interface{} { return *mvcc1 }
	mvcc2.GetObject = func() interface{} { return *mvcc2 }
	assert.NotNil(t, mvcc0.Object())
	mvcc0.SetNextVersion(mvcc1)
	mvcc1.SetNextVersion(mvcc2)
	mvcc1.SetPrevVersion(mvcc0)
	mvcc2.SetPrevVersion(mvcc1)
	assert.Nil(t, mvcc0.GetPrevVersion())
	assert.Nil(t, mvcc2.GetNextVersion())
	assert.NotNil(t, mvcc0.GetNextVersion())
	assert.NotNil(t, mvcc2.GetPrevVersion())
	mvcc2.OnVersionStale()
	mvcc1.OnVersionStale()
}
