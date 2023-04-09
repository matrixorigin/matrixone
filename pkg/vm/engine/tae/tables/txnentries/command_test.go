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
	sid1 := common.NewSegmentid()
	sid2 := common.NewSegmentid()
	from := &common.ID{TableID: 1, SegmentID: sid1, BlockID: common.NewBlockid(&sid1, 1, 0)}
	to := &common.ID{TableID: 1, SegmentID: sid2, BlockID: common.NewBlockid(&sid2, 3, 0)}
	cmd := newCompactBlockCmd(from, to, nil, 0)

	var w bytes.Buffer
	_, err := cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	_, _, err = txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
}

func TestMergeBlocksCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	droppedSid := common.NewSegmentid()
	createdSid := common.NewSegmentid()
	droppedSegs := []*common.ID{{TableID: 1, SegmentID: droppedSid}, {TableID: 1, SegmentID: droppedSid}}
	createdSegs := []*common.ID{{TableID: 1, SegmentID: createdSid}}
	droppedBlks := []*common.ID{
		{TableID: 1, SegmentID: droppedSid, BlockID: common.NewBlockid(&droppedSid, 3, 0)},
		{TableID: 1, SegmentID: droppedSid, BlockID: common.NewBlockid(&droppedSid, 4, 0)}}
	createdBlks := []*common.ID{{TableID: 1, SegmentID: createdSid, BlockID: common.NewBlockid(&createdSid, 1, 0)}}
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

	_, _, err = txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
}
