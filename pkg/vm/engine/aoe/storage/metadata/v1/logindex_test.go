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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogIndex(t *testing.T) {
	idx := LogIndex{
		ID:       MockLogBatchId(1),
		Start:    0,
		Count:    0,
		Capacity: 4,
	}
	assert.False(t, idx.IsApplied())
	idx.Count = 4
	assert.True(t, idx.IsApplied())
	m, err := idx.Marshal()
	assert.Nil(t, err)
	var idx1 LogIndex
	assert.Nil(t, idx1.UnMarshall(make([]byte, 0)))
	assert.Nil(t, idx1.UnMarshall(m))
	assert.Equal(t, idx.String(), "((1,0,1),0,4,4)")

	size := uint32(4)
	batchId := MockLogBatchId(uint64(2))
	batchId.Size = size

	for offset := uint32(0); offset < size-1; offset++ {
		batchId.Offset = offset
		assert.False(t, batchId.IsEnd())
	}
	batchId.Offset = size - 1
	assert.True(t, batchId.IsEnd())
}

func TestBlockAppliedIndex(t *testing.T) {
	blk := Block{}
	id, ok := blk.GetAppliedIndex()
	assert.False(t, ok)

	idx := LogIndex{
		ID:       MockLogBatchId(1),
		Start:    0,
		Count:    2,
		Capacity: 2,
	}
	err := blk.SetIndex(idx)
	assert.Nil(t, err)
	id, ok = blk.GetAppliedIndex()
	assert.True(t, ok)
	assert.Equal(t, idx.ID.Id, id)

	idx.ID.Id = uint64(2)
	err = blk.SetIndex(idx)
	assert.Nil(t, err)
	id, ok = blk.GetAppliedIndex()
	assert.True(t, ok)
	assert.Equal(t, idx.ID.Id, id)

	applied := id
	idx.ID.Id = uint64(3)
	idx.ID.Size = 2
	err = blk.SetIndex(idx)
	assert.Nil(t, err)
	id, ok = blk.GetAppliedIndex()
	assert.True(t, ok)
	assert.Equal(t, applied, id)

	idx.ID.Id = uint64(3)
	idx.ID.Offset = 1
	err = blk.SetIndex(idx)
	assert.Nil(t, err)
	id, ok = blk.GetAppliedIndex()
	assert.True(t, ok)
	assert.Equal(t, idx.ID.Id, id)
	assert.Equal(t, blk.GetReplayIndex().ID, idx.ID)
}
