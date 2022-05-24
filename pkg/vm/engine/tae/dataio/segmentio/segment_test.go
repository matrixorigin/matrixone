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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "SEGMENTIO"
)

func TestSegment1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "seg")
	id := common.NextGlobalSeqNum()
	seg := SegmentFileIOFactory(name, id)
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
	name := path.Join(dir, "seg")
	id := common.NextGlobalSeqNum()
	seg := SegmentFileIOFactory(name, id)
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
	for i := 0; i < 20; i++ {
		blkId1 := common.NextGlobalSeqNum()
		block, err := seg.OpenBlock(blkId1, colCnt, indexCnt)
		assert.Nil(t, err)
		ids = append(ids, blkId1)

		colBlk0, err := block.OpenColumn(0)
		assert.Nil(t, err)
		assert.NotNil(t, colBlk0)
		err = colBlk0.WriteData(w.Bytes())
		assert.Nil(t, err)
		colBlk0.Close()
	}

	seg = SegmentFileIOOpenFactory(name, id)
	cache := bytes.NewBuffer(make([]byte, 10240*4096))
	err := seg.Replay(ids, colCnt, indexCnt, cache)
	assert.Nil(t, err)
	for i := 0; i < 20; i++ {
		block, err := seg.OpenBlock(ids[i], colCnt, indexCnt)
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

		dataFile.Unref()
		colBlk0.Close()

		block.Unref()
	}
}
