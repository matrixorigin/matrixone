package memtable

import (
	"matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testNodeHandle struct {
	nodeHandle
	t *testing.T
}

func (h *testNodeHandle) load() {
	h.t.Logf("Load %s", h.id.BlockString())
}
func (h *testNodeHandle) unload() {
	h.t.Logf("Unload %s", h.id.BlockString())
}
func (h *testNodeHandle) destory() {
	h.t.Logf("Destroy %s", h.id.BlockString())
}

func newTestNodeHandle(mgr *nodeManager, id common.ID, size uint64, t *testing.T) *testNodeHandle {
	n := &testNodeHandle{
		nodeHandle: *newNodeHandle(mgr, id, size),
		t:          t,
	}
	n.loadFunc = n.load
	n.unloadFunc = n.unload
	n.destoryFunc = n.destory
	return n
}

func TestHandle(t *testing.T) {
	maxsize := uint64(100)
	limiter := &memtableLimiter{
		maxactivesize: maxsize,
	}
	evicter := manager.NewSimpleEvictHolder()
	mgr := newNodeManager(limiter, evicter)
	baseId := common.ID{}
	id1 := baseId.NextBlock()
	id2 := baseId.NextBlock()
	id3 := baseId.NextBlock()
	id4 := baseId.NextBlock()
	sz1, sz2, sz3, sz4 := uint64(30), uint64(30), uint64(50), uint64(80)
	n1 := newTestNodeHandle(mgr, id1, sz1, t)
	n2 := newTestNodeHandle(mgr, id2, sz2, t)
	n3 := newTestNodeHandle(mgr, id3, sz3, t)
	n4 := newTestNodeHandle(mgr, id4, sz4, t)
	mgr.RegisterNode(n1)
	mgr.RegisterNode(n2)
	mgr.RegisterNode(n3)
	mgr.RegisterNode(n4)
	assert.Equal(t, 4, mgr.Count())

	ok := mgr.Pin(n1)
	assert.True(t, ok)
	ok = mgr.Pin(n2)
	assert.True(t, ok)
	assert.Equal(t, sz1+sz2, limiter.ActiveSize())
	ok = mgr.Pin(n3)
	assert.False(t, ok)
	mgr.Unpin(n1)
	ok = mgr.Pin(n3)
	assert.True(t, ok)
	assert.Equal(t, sz2+sz3, limiter.ActiveSize())

	mgr.Unpin(n2)
	mgr.Unpin(n3)
	ok = mgr.Pin(n4)
	assert.True(t, ok)
	assert.Equal(t, sz4, limiter.ActiveSize())

	t.Log(mgr.String())

	n1.Close()
	n2.Close()
	n3.Close()
	mgr.Unpin(n4)
	n4.Close()
	assert.Equal(t, uint64(0), limiter.ActiveSize())
	t.Log(mgr.String())
}
