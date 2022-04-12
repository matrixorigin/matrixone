package catalog

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
)

type testNode struct {
	val int
}

func newTestNode(val int) *testNode {
	return &testNode{val: val}
}

func (n *testNode) Compare(o common.NodePayload) int {
	on := o.(*testNode)
	if n.val > on.val {
		return 1
	} else if n.val < on.val {
		return -1
	}
	return 0
}

func TestDLNode(t *testing.T) {
	link := new(common.Link)
	now := time.Now()
	var node *common.DLNode
	// for i := 10; i >= 0; i-- {
	nodeCnt := 10
	for i := 0; i < nodeCnt; i++ {
		n := link.Insert(newTestNode(i))
		if i == 5 {
			node = n
		}
	}
	t.Log(time.Since(now))
	cnt := 0
	link.Loop(func(node *common.DLNode) bool {
		cnt++
		return true
	}, true)
	assert.Equal(t, nodeCnt, cnt)
	assert.Equal(t, 5, node.GetPayload().(*testNode).val)

	link.Delete(node)
	cnt = 0
	link.Loop(func(node *common.DLNode) bool {
		t.Logf("%d", node.GetPayload().(*testNode).val)
		cnt++
		return true
	}, true)
	assert.Equal(t, nodeCnt-1, cnt)
}
