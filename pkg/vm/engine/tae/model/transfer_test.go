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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
)

func TestTransferPage(t *testing.T) {
	sid := objectio.NewSegmentid()
	src := common.ID{
		BlockID: *objectio.NewBlockid(sid, 1, 0),
	}
	dest := common.ID{
		BlockID: *objectio.NewBlockid(sid, 2, 0),
	}

	memo1 := NewTransferHashPage(&src, time.Now(), false)
	assert.Zero(t, memo1.RefCount())

	for i := 0; i < 10; i++ {
		memo1.Train(uint32(i), *objectio.NewRowid(&dest.BlockID, uint32(i)))
	}

	pinned := memo1.Pin()
	assert.Equal(t, int64(1), memo1.RefCount())
	pinned.Close()
	assert.Zero(t, memo1.RefCount())

	ttl := time.Millisecond * 10
	now := time.Now()
	memo2 := NewTransferHashPage(&src, now, false)
	defer memo2.Close()
	assert.Zero(t, memo2.RefCount())

	for i := 0; i < 10; i++ {
		memo2.Train(uint32(i), *objectio.NewRowid(&dest.BlockID, uint32(i)))
	}

	assert.False(t, memo2.TTL(now.Add(ttl-time.Duration(1)), ttl))
	assert.True(t, memo2.TTL(now.Add(ttl+time.Duration(1)), ttl))

	for i := 0; i < 10; i++ {
		rowID, ok := memo2.Transfer(uint32(i))
		assert.True(t, ok)
		blockId, offset := rowID.Decode()
		assert.Equal(t, dest.BlockID, blockId)
		assert.Equal(t, uint32(i), offset)
	}
}

func TestTransferTable(t *testing.T) {
	ttl := time.Minute
	table := NewTransferTable[*TransferHashPage](ttl)
	defer table.Close()
	sid := objectio.NewSegmentid()

	id1 := common.ID{BlockID: *objectio.NewBlockid(sid, 1, 0)}
	id2 := common.ID{BlockID: *objectio.NewBlockid(sid, 2, 0)}

	now := time.Now()
	page1 := NewTransferHashPage(&id1, now, false)
	for i := 0; i < 10; i++ {
		rowID := *objectio.NewRowid(&id2.BlockID, uint32(i))
		page1.Train(uint32(i), rowID)
	}

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
