package memtable

import (
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testNodeHandle struct {
	node
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
		node: *newNode(mgr, id, size),
		t:    t,
	}
	n.loadFunc = n.load
	n.unloadFunc = n.unload
	n.destoryFunc = n.destory
	return n
}

func TestHandle(t *testing.T) {
	maxsize := uint64(100)
	evicter := bm.NewSimpleEvictHolder()
	mgr := newNodeManager(maxsize, evicter)
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

	h1 := mgr.Pin(n1)
	assert.NotNil(t, h1)
	h2 := mgr.Pin(n2)
	assert.NotNil(t, h2)
	assert.Equal(t, sz1+sz2, mgr.Total())
	h3 := mgr.Pin(n3)
	assert.Nil(t, h3)
	h1.Close()
	h3 = mgr.Pin(n3)
	assert.NotNil(t, h3)
	assert.Equal(t, sz2+sz3, mgr.Total())

	h2.Close()
	h3.Close()
	h4 := mgr.Pin(n4)
	assert.NotNil(t, h4)
	assert.Equal(t, sz4, mgr.Total())

	t.Log(mgr.String())

	n1.Close()
	n2.Close()
	n3.Close()
	h4.Close()
	n4.Close()
	assert.Equal(t, uint64(0), mgr.Total())
	t.Log(mgr.String())
}
