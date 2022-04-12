package updates

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

type testUpdateNode struct {
	updates  *BlockUpdates
	startTs  uint64
	commitTs uint64
	next     *testUpdateNode
	prev     *testUpdateNode
	ntype    int8
}

func newTestUpdateNode(ntype int8, meta *catalog.BlockEntry, start uint64, deletes *roaring.Bitmap) *testUpdateNode {
	return &testUpdateNode{
		ntype:    ntype,
		startTs:  start,
		commitTs: ^uint64(0),
		updates:  NewBlockUpdates(nil, meta, nil, deletes),
	}
}

func (update *testUpdateNode) getID() *common.ID { return update.updates.id }

func (update *testUpdateNode) repr() string {
	commitState := "C"
	if !update.hasCommitted() {
		commitState = "UC"
	}
	ntype := "Txn"
	if update.ntype == 1 {
		ntype = "Merge"
	}
	nextStr := "nil"
	if update.next != nil {
		nextStr = fmt.Sprintf("%s", update.next.getID().BlockString())
	}
	s := fmt.Sprintf("[%s:%s:%s](%d-%d)->%s", ntype, commitState, update.getID().BlockString(), update.startTs, update.commitTs, nextStr)
	return s
}

func (update *testUpdateNode) hasCommitted() bool {
	return update.commitTs != ^uint64(0)
}

func (update *testUpdateNode) isMergedNode() bool {
	return update.ntype == 1
}

func (update *testUpdateNode) less(o *testUpdateNode) bool {
	if update.hasCommitted() && !o.hasCommitted() {
		return true
	}
	if !update.hasCommitted() && o.hasCommitted() {
		return false
	}
	if update.hasCommitted() && o.hasCommitted() {
		if update.commitTs < o.commitTs {
			return true
		} else if update.commitTs > o.commitTs {
			return false
		}
		if o.isMergedNode() {
			return true
		}
		return false
	}
	return update.startTs < o.startTs
}

func (update *testUpdateNode) commit(ts uint64) {
	if update.hasCommitted() {
		panic("not expected")
	}
	if ts <= update.startTs || ts == ^uint64(0) {
		panic("not expected")
	}
	update.commitTs = ts
}

func sortNodes(node *testUpdateNode) *testUpdateNode {
	curr := node
	head := curr
	prev := node.prev
	next := node.next
	for (curr != nil && next != nil) && curr.less(next) {
		if head == curr {
			head = next
		}
		if prev != nil {
			prev.next = next
		}
		next.prev = prev

		prev = next
		next = next.next

		prev.next = curr
		curr.prev = prev

		curr.next = next
		if next != nil {
			next.prev = curr
		}
	}
	return head
}

func insertLink(node, head *testUpdateNode) *testUpdateNode {
	if head == nil {
		head = node
		return head
	}
	node.next = head
	head.prev = node
	head = sortNodes(node)
	return head
}

func loopLink(t *testing.T, head *testUpdateNode, fn func(node *testUpdateNode) bool) {
	curr := head
	for curr != nil {
		goNext := fn(curr)
		if !goNext {
			break
		}
		curr = curr.next
	}
}

func findHead(n *testUpdateNode) *testUpdateNode {
	head := n
	for head.prev != nil {
		head = head.prev
	}
	return head
}

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func makeMerge(l *common.Link) *BlockUpdates {
	var merge *BlockUpdates
	l.Loop(func(node *common.DLNode) bool {
		update := node.GetPayload().(*BlockUpdates)
		if update.commitTs == txnif.UncommitTS {
			return true
		}
		if merge == nil {
			merge = NewMergeBlockUpdates(update.commitTs, update.meta, nil, nil)
		}
		merge.MergeLocked(update)
		if update.nodeType == NT_Merge {
			return false
		}
		return true
	}, false)
	return merge
}

func TestUpdatesMerge(t *testing.T) {
	var head *testUpdateNode
	cnt1 := 11
	cnt2 := 10
	nodes := make([]*testUpdateNode, 0)
	schema := catalog.MockSchema(1)
	c := catalog.MockCatalog(initTestPath(t), "mock", nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	for i := 0; i < cnt1+cnt2; i++ {
		start := uint64(i) * 2
		node := newTestUpdateNode(0, blk, uint64(i)*2, nil)
		node.updates.DeleteLocked(uint32(i)*10, uint32(i+1)*10-1)
		err := node.updates.UpdateLocked(uint32(i+1)*10000, 0, (i+1)*10000)
		if i < cnt1 {
			node.commit(start + 1)
		} else {
			nodes = append(nodes, node)
		}
		assert.Nil(t, err)
		head = insertLink(node, head)
	}

	makeMerge := func() *testUpdateNode {
		var merge *testUpdateNode
		loopLink(t, head, func(n *testUpdateNode) bool {
			if n.isMergedNode() {
				return false
			}
			if !n.hasCommitted() {
				return true
			}
			if merge == nil {
				merge = newTestUpdateNode(1, n.updates.meta, n.startTs, nil)
				merge.commit(n.commitTs)
			}
			merge.updates.MergeLocked(n.updates)
			return true
		})
		return merge
	}

	merge := makeMerge()
	insertLink(merge, head)
	t.Log(merge.updates.localDeletes.String())
	assert.Equal(t, cnt1*10, int(merge.updates.localDeletes.GetCardinality()))
	assert.Equal(t, cnt1, int(merge.updates.cols[0].txnMask.GetCardinality()))

	commitTs := head.startTs + uint64(100)
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		if n.hasCommitted() {
			continue
		}
		n.commit(commitTs)
		commitTs++
		sortNodes(n)
	}

	head = findHead(head)

	cnt := 0
	loopLink(t, head, func(n *testUpdateNode) bool {
		if n.hasCommitted() && !n.isMergedNode() {
			cnt++
		}
		return true
	})
	assert.Equal(t, cnt1+cnt2, cnt)

	merge = makeMerge()
	head = findHead(head)
	insertLink(merge, head)
	head = findHead(head)
	loopLink(t, head, func(n *testUpdateNode) bool {
		t.Log(n.repr())
		return true
	})
	assert.Equal(t, cnt2*10, int(merge.updates.localDeletes.GetCardinality()))
	assert.Equal(t, cnt2, int(merge.updates.cols[0].txnMask.GetCardinality()))
}

func TestUpdates(t *testing.T) {
	committed := 10
	nodes := make([]*testUpdateNode, 0)
	schema := catalog.MockSchema(1)
	c := catalog.MockCatalog(initTestPath(t), "mock", nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	var head *testUpdateNode
	for i := 0; i < committed; i++ {
		node := newTestUpdateNode(0, blk, uint64(committed-i)*10, nil)
		head = insertLink(node, head)
		nodes = append(nodes, node)
	}

	head = findHead(head)
	loopLink(t, head, func(node *testUpdateNode) bool {
		t.Log(node.repr())
		return true
	})
	now := time.Now()
	commitTs := (committed + 1) * 10
	mergeIdx := len(nodes) / 2
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i].commit(uint64(commitTs + committed - i))
		sortNodes(nodes[i])
	}

	mergeNode := newTestUpdateNode(1, nodes[mergeIdx].updates.meta, nodes[mergeIdx].startTs, nil)
	mergeNode.commit(nodes[mergeIdx].commitTs)

	head = findHead(nodes[0])

	insertLink(mergeNode, head)

	loopLink(t, head, func(node *testUpdateNode) bool {
		t.Log(node.repr())
		return true
	})
	t.Log(time.Since(now))
	assert.Equal(t, mergeNode.next, nodes[mergeIdx])

	var w bytes.Buffer
	err := mergeNode.updates.WriteTo(&w)
	assert.Nil(t, err)

	m1 := mergeNode.updates
	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	m2 := NewEmptyBlockUpdates()
	err = m2.ReadFrom(r)
	assert.Equal(t, *m1.id, *m2.id)
	// assert.True(t, m1.localDeletes.Equals(m2.localDeletes))
	assert.Equal(t, len(m1.cols), len(m2.cols))

	cmd, _, _ := mergeNode.updates.MakeCommand(1, false)
	w.Reset()
	err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf = w.Bytes()
	r = bytes.NewBuffer(buf)

	cmd2, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	assert.NotNil(t, cmd2)
}

func TestUpdates2(t *testing.T) {
	schema := catalog.MockSchema(1)
	c := catalog.MockCatalog(initTestPath(t), "mock", nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	uncommitted := new(common.Link)

	cnt1 := 11
	cnt2 := 10
	chain := NewUpdateChain(nil, blk)

	for i := 0; i < cnt1+cnt2; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), uint64(i)*2, nil)
		node := chain.AddNode(txn)
		// updates := node.GetUpdates()
		node.DeleteLocked(uint32(i)*10, uint32(i+1)*10-1)
		err := node.UpdateLocked(uint32(i+1)*10000, 0, (i+1)*10000)
		if i < cnt1 {
			txn.CommitTS = txn.StartTS + 1
			err := node.PrepareCommit()
			assert.Nil(t, err)
			// node.commitTs = txn.StartTS + 1
			// node.txn = nil
		} else {
			uncommitted.Insert(node)
		}
		assert.Nil(t, err)
	}
	uncommittedCnt := 0
	uncommitted.Loop(func(node *common.DLNode) bool {
		uncommittedCnt++
		return true
	}, true)
	assert.Equal(t, cnt2, uncommittedCnt)

	totalCnt := 0
	chain.LoopChainLocked(func(node *BlockUpdateNode) bool {
		totalCnt++
		return true
	}, true)
	assert.Equal(t, cnt2+cnt1, totalCnt)

	m := chain.AddMergeNode()
	t.Log(m.String())
	t.Log(m.localDeletes.String())

	assert.Equal(t, cnt1*10, int(m.localDeletes.GetCardinality()))
	assert.Equal(t, cnt1, int(m.cols[0].txnMask.GetCardinality()))

	totalCnt = 0
	chain.LoopChainLocked(func(node *BlockUpdateNode) bool {
		totalCnt++
		if totalCnt == uncommittedCnt+1 {
			assert.True(t, node.IsMerge())
		}
		t.Logf("Loop1: %s", node.String())
		return true
	}, false)
	assert.Equal(t, cnt2+cnt1+1, totalCnt)
	// t.Log(link.GetHead().GetPayload().(*BlockUpdates).String())

	commitTs := chain.FirstNode().GetStartTS() + uint64(100)
	for {
		node := chain.FirstNode()
		if node.GetCommitTSLocked() != txnif.UncommitTS {
			break
		}
		node.txn.MockSetCommitTSLocked(commitTs)
		err := node.PrepareCommit()
		assert.Nil(t, err)
		commitTs++
	}

	prev := txnif.UncommitTS
	chain.LoopChainLocked(func(node *BlockUpdateNode) bool {
		assert.True(t, node.GetCommitTSLocked() <= prev)
		prev = node.GetCommitTSLocked()
		t.Logf("Loop2: %s", node.String())
		return true
	}, false)

	m = chain.AddMergeNode()

	chain.LoopChainLocked(func(node *BlockUpdateNode) bool {
		t.Log(node.String())
		return true
	}, false)

	t.Log(m.localDeletes.String())
}

func TestUpdates3(t *testing.T) {
	schema := catalog.MockSchema(1)
	c := catalog.MockCatalog(initTestPath(t), "mock", nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blkCnt := 200
	chains := make([]*BlockUpdateChain, 0, blkCnt)
	now := time.Now()
	for i := 0; i < blkCnt; i++ {
		blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)
		blk.CreateAt = common.NextGlobalSeqNum()
		chain := NewUpdateChain(nil, blk)
		chains = append(chains, chain)
	}
	for _, chain := range chains {
		nodeCnt := rand.Intn(4) + 40
		for j := 0; j < nodeCnt; j++ {
			txn := new(txnbase.Txn)
			txn.TxnCtx = new(txnbase.TxnCtx)
			txn.StartTS = common.NextGlobalSeqNum()
			node := chain.AddNode(txn)
			node.DeleteLocked(uint32(j)*1, uint32(j+1)*(uint32(nodeCnt))-1)
			node.UpdateLocked(uint32(j)+1000, 0, int32((j+1)*1000))
			node.commitTs = common.NextGlobalSeqNum()
		}
	}
	t.Log(time.Since(now))

	now = time.Now()
	for _, chain := range chains {
		chain.AddMergeNode()
	}
	t.Log(time.Since(now))

	now = time.Now()
	for _, chain := range chains {
		m := chain.AddMergeNode()
		t.Log(m.localDeletes.String())
	}
	t.Log(time.Since(now))
	rowCnt := uint64(10000)
	bats := make([]*gbat.Batch, len(chains))
	for i := range bats {
		bat := mock.MockBatch(schema.Types(), rowCnt)
		bats[i] = bat
	}

	doApply := func(bat *gbat.Batch, chain *BlockUpdateChain) func() {
		return func() {
			update := chain.FirstNode()
			update.ApplyChanges(bat, update.localDeletes)
			// t.Logf("delete cnt: %d", update.localDeletes.GetCardinality())
			// t.Logf("update cnt: %d", update.cols[0].txnMask.GetCardinality())
			// t.Logf("Length post apply change: %d", gvec.Length(bat.Vecs[0]))
			assert.Equal(t, rowCnt, update.localDeletes.GetCardinality()+uint64(gvec.Length(bat.Vecs[0])))
		}
	}

	pool, _ := ants.NewPool(12)

	now = time.Now()
	for i, bat := range bats {
		chain := chains[i]
		pool.Submit(doApply(bat, chain))
	}
	t.Log(time.Since(now))
}
