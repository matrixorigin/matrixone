package objectio

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
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
	blockTs := common.NextGlobalSeqNum()
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
	assert.Equal(t, int64(len(dataStr)), dataFile.Stat().OriginSize())
	buf := make([]byte, size)
	_, err = dataFile.Read(buf)
	assert.Nil(t, err)
	assert.Equal(t, dataStr, string(buf))
	t.Log(string(buf))

	dataFile.Unref()
	colBlk0.Close()

	block.Unref()
}
