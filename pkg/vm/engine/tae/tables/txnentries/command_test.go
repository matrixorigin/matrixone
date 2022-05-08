package txnentries

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestCompactBlockCmd(t *testing.T) {
	from := &common.ID{TableID: 1, SegmentID: 2, BlockID: 3}
	to := &common.ID{TableID: 1, SegmentID: 3, BlockID: 1}
	cmd := newCompactBlockCmd(from, to)

	var w bytes.Buffer
	err := cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	checkCompactBlockCmdIsEqual(t, cmd, cmd2.(*compactBlockCmd))
}

func checkCompactBlockCmdIsEqual(t *testing.T, cmd1, cmd2 *compactBlockCmd) {
	checkIDIsEqual(t, cmd1.from, cmd2.from)
	checkIDIsEqual(t, cmd1.to, cmd2.to)
}

func checkIDIsEqual(t *testing.T, id1, id2 *common.ID) {
	assert.Equal(t, id1.TableID, id2.TableID)
	assert.Equal(t, id1.SegmentID, id2.SegmentID)
	assert.Equal(t, id1.BlockID, id2.BlockID)
}

func TestMergeBlocksCmd(t *testing.T) {
	from := []*common.ID{{TableID: 1, SegmentID: 2, BlockID: 3}, {TableID: 1, SegmentID: 2, BlockID: 4}}
	to := []*common.ID{{TableID: 1, SegmentID: 3, BlockID: 1}}
	cmd := newMergeBlocksCmd(from, to)

	var w bytes.Buffer
	err := cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	checkMergeBlocksCmdIsEqual(t, cmd, cmd2.(*mergeBlocksCmd))
}

func checkMergeBlocksCmdIsEqual(t *testing.T, cmd1, cmd2 *mergeBlocksCmd) {
	assert.Equal(t, len(cmd1.from), len(cmd2.from))
	for i, from1 := range cmd1.from {
		checkIDIsEqual(t, from1, cmd2.from[i])
	}
	assert.Equal(t, len(cmd1.to), len(cmd2.to))
	for i, to1 := range cmd1.to {
		checkIDIsEqual(t, to1, cmd2.to[i])
	}
}
