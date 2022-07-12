package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	ModuleName = "OBJECTIO"
)

func TestSgment1(t *testing.T) {
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
