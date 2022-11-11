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

package txnentries

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestCompactBlockCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	from := &common.ID{TableID: 1, SegmentID: 2, BlockID: 3}
	to := &common.ID{TableID: 1, SegmentID: 3, BlockID: 1}
	cmd := newCompactBlockCmd(from, to, nil, 0)

	var w bytes.Buffer
	_, err := cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, _, err := txnbase.BuildCommandFrom(r)
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	droppedSegs := []*common.ID{{TableID: 1, SegmentID: 2}, {TableID: 1, SegmentID: 2}}
	createdSegs := []*common.ID{{TableID: 1, SegmentID: 3}}
	droppedBlks := []*common.ID{{TableID: 1, SegmentID: 2, BlockID: 3}, {TableID: 1, SegmentID: 2, BlockID: 4}}
	createdBlks := []*common.ID{{TableID: 1, SegmentID: 3, BlockID: 1}}
	mapping := []uint32{3445, 4253, 425, 45, 123, 34, 42, 42, 2, 5, 0}
	fromAddr := []uint32{40000, 40000, 40000, 42}
	toAddr := []uint32{40000, 40000, 242}
	cmd := newMergeBlocksCmd(
		0,
		droppedSegs,
		createdSegs,
		droppedBlks,
		createdBlks,
		mapping,
		fromAddr,
		toAddr,
		nil,
		0)

	var w bytes.Buffer
	_, err := cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	checkMergeBlocksCmdIsEqual(t, cmd, cmd2.(*mergeBlocksCmd))
}

func checkMergeBlocksCmdIsEqual(t *testing.T, cmd1, cmd2 *mergeBlocksCmd) {
	assert.Equal(t, len(cmd1.createdSegs), len(cmd2.createdSegs))
	for i, seg1 := range cmd1.createdSegs {
		checkIDIsEqual(t, seg1, cmd2.createdSegs[i])
	}
	assert.Equal(t, len(cmd1.createdBlks), len(cmd2.createdBlks))
	for i, blk1 := range cmd1.createdBlks {
		checkIDIsEqual(t, blk1, cmd2.createdBlks[i])
	}
	assert.Equal(t, len(cmd1.droppedSegs), len(cmd2.droppedSegs))
	for i, seg1 := range cmd1.droppedSegs {
		checkIDIsEqual(t, seg1, cmd2.droppedSegs[i])
	}
	assert.Equal(t, len(cmd1.droppedBlks), len(cmd2.droppedBlks))
	for i, blk1 := range cmd1.droppedBlks {
		checkIDIsEqual(t, blk1, cmd2.droppedBlks[i])
	}
	assert.Equal(t, len(cmd1.mapping), len(cmd2.mapping))
	assert.Equal(t, len(cmd1.fromAddr), len(cmd2.fromAddr))
	assert.Equal(t, len(cmd1.toAddr), len(cmd2.toAddr))
}
