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

package model

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
)

func TestTransferPage(t *testing.T) {
	src := common.ID{
		BlockID: 1,
	}
	dest := common.ID{
		BlockID: 2,
	}
	prefix := EncodeBlockKeyPrefix(0, dest.BlockID)
	offsets1 := NewRowIDVector()
	for i := 0; i < 10; i++ {
		rowID := EncodePhyAddrKeyWithPrefix(prefix, uint32(i))
		offsets1.Append(rowID)
	}
	offsets2 := offsets1.CloneWindow(0, offsets1.Length())

	memo1 := NewTransferPage(time.Now(), &src, offsets1)
	assert.Zero(t, memo1.RefCount())

	pinned := memo1.Pin()
	assert.Equal(t, int64(1), memo1.RefCount())
	pinned.Close()
	assert.Zero(t, memo1.RefCount())

	ttl := time.Millisecond * 10
	now := time.Now()
	memo2 := NewTransferPage(now, &src, offsets2)
	defer memo2.Close()
	assert.Zero(t, memo2.RefCount())

	assert.False(t, memo2.TTL(now.Add(ttl-time.Duration(1)), ttl))
	assert.True(t, memo2.TTL(now.Add(ttl+time.Duration(1)), ttl))

	rowId1 := memo2.TransferOne(1)
	_, blockId, offset := DecodePhyAddrKey(rowId1)
	assert.Equal(t, dest.BlockID, blockId)
	assert.Equal(t, uint32(1), offset)

	{
		srcOffs := []uint32{1, 3, 5, 7, 9}
		rowidVec := memo2.TransferMany(srcOffs...)
		assert.Equal(t, 5, rowidVec.Length())
		rowidVec.Foreach(func(v any, row int) (err error) {
			_, blockId, offset := DecodePhyAddrKey(v.(types.Rowid))
			assert.Equal(t, dest.BlockID, blockId)
			assert.Equal(t, srcOffs[row], offset)
			return
		}, nil)
	}
}

func TestTransferTable(t *testing.T) {
	ttl := time.Minute
	table := NewTransferTable[*TransferPage](ttl)
	defer table.Close()

	id1 := common.ID{BlockID: 1}
	id2 := common.ID{BlockID: 2}

	prefix := EncodeBlockKeyPrefix(id2.SegmentID, id2.BlockID)
	rowIDS := NewRowIDVector()
	for i := 0; i < 10; i++ {
		rowID := EncodePhyAddrKeyWithPrefix(prefix, uint32(i))
		rowIDS.Append(rowID)
	}

	now := time.Now()
	page1 := NewTransferPage(now, &id1, rowIDS)
	assert.False(t, table.AddPage(page1))
	assert.True(t, table.AddPage(page1))
	assert.Equal(t, int64(1), page1.RefCount())

	_, err := table.Pin(id2)
	assert.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB))
	pinned, err := table.Pin(id1)
	assert.NoError(t, err)

	assert.Equal(t, int64(2), pinned.Item().RefCount())

	table.RunTTL(now.Add(ttl - time.Duration(1)))
	assert.Equal(t, 1, table.Len())
	table.RunTTL(now.Add(ttl + time.Duration(1)))
	assert.Equal(t, 0, table.Len())

	assert.Equal(t, int64(1), pinned.Item().RefCount())
	pinned.Close()
	assert.Equal(t, int64(0), pinned.Item().RefCount())
}
