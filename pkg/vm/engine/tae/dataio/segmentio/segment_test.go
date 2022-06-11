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

package segmentio

import (
	"bytes"
	"testing"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/compress"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "SEGMENTIO"
)

func TestSegment1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	id := common.NextGlobalSeqNum()
	seg := SegmentFactory.Build(dir, id)
	fp := seg.Fingerprint()
	assert.Equal(t, id, fp.SegmentID)

	blkId1 := common.NextGlobalSeqNum()
	blk1, err := seg.OpenBlock(blkId1, 2, nil)
	assert.Nil(t, err)
	blkTs1 := common.NextGlobalSeqNum()
	err = blk1.WriteTS(blkTs1)
	assert.Nil(t, err)

	ts, _ := blk1.ReadTS()
	assert.Equal(t, blkTs1, ts)
	blk1.Close()
	t.Log(seg.String())
	seg.Unref()
}

func TestSegmentFile_Replay(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	id := common.NextGlobalSeqNum()
	seg := SegmentFactory.Build(dir, id)
	fp := seg.Fingerprint()
	colCnt := 4
	indexCnt := make(map[int]int)
	for col := 0; col < colCnt; col++ {
		indexCnt[col] = 2
	}
	assert.Equal(t, id, fp.SegmentID)
	ids := make([]uint64, 0)
	var w bytes.Buffer
	dataStr := "hello tae"
	w.WriteString(dataStr)
	deletes := roaring.New()
	deletes.Add(10)
	deletes.Add(20)
	deletesBuf, _ := deletes.ToBytes()
	for i := 0; i < 20; i++ {
		blkId1 := common.NextGlobalSeqNum()
		block, err := seg.OpenBlock(blkId1, colCnt, indexCnt)
		assert.Nil(t, err)
		blockTs := common.NextGlobalSeqNum()
		err = block.WriteTS(blockTs)
		assert.Nil(t, err)
		err = block.WriteRows(1)
		assert.Nil(t, err)
		readTs, _ := block.ReadTS()
		assert.Equal(t, blockTs, readTs)
		err = block.WriteIndexMeta(w.Bytes())
		assert.Nil(t, err)

		err = block.WriteDeletes(deletesBuf)
		assert.Nil(t, err)
		assert.Equal(t, int64(len(deletesBuf)), block.GetDeletesFileStat().OriginSize())
		ids = append(ids, blkId1)

		colBlk0, err := block.OpenColumn(0)
		assert.Nil(t, err)
		assert.NotNil(t, colBlk0)
		err = colBlk0.WriteTS(blockTs)
		assert.Nil(t, err)
		err = colBlk0.WriteData(w.Bytes())
		assert.Nil(t, err)
		idx, err := colBlk0.OpenIndexFile(0)
		assert.Nil(t, err)
		_, err = idx.Write(w.Bytes())
		assert.Nil(t, err)
		colBlk0.WriteUpdates(w.Bytes())
		assert.Nil(t, err)
		colBlk0.Close()
	}

	seg = SegmentFactory.Build(dir, id)
	cache := bytes.NewBuffer(make([]byte, 2*1024*1024))
	err := seg.Replay(colCnt, indexCnt, cache)
	assert.Nil(t, err)
	for i := 0; i < 20; i++ {
		block, err := seg.OpenBlock(ids[i], colCnt, indexCnt)
		assert.Nil(t, err)
		assert.Equal(t, int64(len(deletesBuf)), block.GetDeletesFileStat().OriginSize())
		colBlk0, err := block.OpenColumn(0)
		assert.Nil(t, err)
		assert.NotNil(t, colBlk0)
		dataFile, err := colBlk0.OpenDataFile()
		assert.Nil(t, err)
		size := dataFile.Stat().Size()
		dsize := dataFile.Stat().OriginSize()
		assert.Equal(t, int64(len(dataStr)), dataFile.Stat().OriginSize())
		buf := make([]byte, size)
		dbuf := make([]byte, dsize)
		_, err = dataFile.Read(buf)
		assert.Nil(t, err)
		dbuf, err = compress.Decompress(buf, dbuf, compress.Lz4)
		assert.Nil(t, err)
		assert.Equal(t, dataStr, string(dbuf))
		t.Log(string(dbuf))
		assert.Equal(t, 1, int(block.ReadRows()))

		dataFile.Unref()
		inx, err := colBlk0.OpenIndexFile(0)
		assert.Nil(t, err)
		_, err = inx.Read(dbuf)
		assert.Nil(t, err)
		assert.Equal(t, dataStr, string(dbuf))
		update, err := colBlk0.OpenUpdateFile()
		buf = make([]byte, size)
		dsize = update.Stat().OriginSize()
		assert.Equal(t, int64(len(dataStr)), update.Stat().OriginSize())
		dbuf = make([]byte, dsize)
		_, err = update.Read(buf)
		assert.Nil(t, err)
		dbuf, err = compress.Decompress(buf, dbuf, compress.Lz4)
		assert.Nil(t, err)
		assert.Equal(t, dataStr, string(dbuf))
		err = colBlk0.ReadUpdates(buf)
		assert.Nil(t, err)
		dbuf, err = compress.Decompress(buf, dbuf, compress.Lz4)
		assert.Nil(t, err)
		assert.Equal(t, dataStr, string(dbuf))
		colBlk0.Close()

		block.Unref()
	}
}
