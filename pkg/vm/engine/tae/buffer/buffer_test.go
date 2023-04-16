// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

type testNodeHandle struct {
	Node
	t *testing.T
}

func (h *testNodeHandle) load() {
	id := h.key.(common.ID)
	h.t.Logf("Load %s", id.BlockString())
}
func (h *testNodeHandle) unload() {
	id := h.key.(common.ID)
	h.t.Logf("Unload %s", id.BlockString())
}
func (h *testNodeHandle) destroy() {
	id := h.key.(common.ID)
	h.t.Logf("Destroy %s", id.BlockString())
}

func newTestNodeHandle(mgr *nodeManager, id common.ID, size uint64, t *testing.T) *testNodeHandle {
	n := &testNodeHandle{
		t: t,
	}
	n.Node = *NewNode(n, mgr, id, size)
	n.LoadFunc = n.load
	n.UnloadFunc = n.unload
	n.DestroyFunc = n.destroy
	return n
}

func TestHandle(t *testing.T) {
	defer testutils.AfterTest(t)()
	maxsize := uint64(100)
	evicter := NewSimpleEvictHolder()
	mgr := NewNodeManager(maxsize, evicter)
	id1 := common.ID{}
	seg := objectio.NewSegmentid()
	id1.BlockID = objectio.NewBlockid(&seg, 0, 1)
	id2 := id1
	id2.BlockID = objectio.NewBlockid(&seg, 0, 2)
	id3 := id1
	id3.BlockID = objectio.NewBlockid(&seg, 0, 3)
	id4 := id1
	id4.BlockID = objectio.NewBlockid(&seg, 0, 4)
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
	// t.Log(mgr.String())
	h4.Close()
	h3 = mgr.Pin(n3)
	assert.NotNil(t, h3)
	h3.Close()
	h1 = mgr.Pin(n1)
	err := h1.GetNode().Expand(uint64(100), nil)
	assert.NotNil(t, err)
	err = h1.GetNode().Expand(uint64(60), nil)
	assert.Nil(t, err)
	h2 = mgr.Pin(n2)
	assert.Nil(t, h2)
	// t.Log(mgr.String())
	n1.Close()
	n2.Close()
	n3.Close()
	n4.Close()
	assert.Equal(t, uint64(0), mgr.Total())
	// t.Log(mgr.String())
}
