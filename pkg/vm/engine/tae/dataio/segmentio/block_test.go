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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/compress"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestBlock1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	colCnt := 4
	indexCnt := make(map[int]int)
	for col := 0; col < colCnt; col++ {
		indexCnt[col] = 2
	}
	var block file.Block
	id := common.NextGlobalSeqNum()
	seg := SegmentFactory.Build(dir, id).(*segmentFile)
	block = newBlock(common.NextGlobalSeqNum(), seg, colCnt, indexCnt)
	//blockTs := common.NextGlobalSeqNum()
	blockTs := types.NextGlobalTsForTest()
	err := block.WriteTS(blockTs)
	assert.Nil(t, err)
	readTs, _ := block.ReadTS()
	assert.Equal(t, blockTs, readTs)

	deletes := roaring.New()
	deletes.Add(10)
	deletes.Add(20)
	deletesBuf, _ := deletes.ToBytes()
	err = block.WriteDeletes(deletesBuf)
	assert.Nil(t, err)

	_, err = block.OpenColumn(colCnt)
	assert.NotNil(t, err)

	colBlk0, err := block.OpenColumn(0)
	assert.Nil(t, err)
	assert.NotNil(t, colBlk0)
	var w bytes.Buffer
	dataStr := "hello tae"
	w.WriteString(dataStr)
	err = colBlk0.WriteTS(blockTs)
	assert.Nil(t, err)
	err = colBlk0.WriteData(w.Bytes())
	assert.Nil(t, err)

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
