// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
