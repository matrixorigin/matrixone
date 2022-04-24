package mockio

import (
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "MOCKIO"
)

func TestSegment1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := path.Join(dir, "seg")
	id := common.NextGlobalSeqNum()
	seg := SegmentFileMockFactory(name, id)
	fp := seg.Fingerprint()
	assert.Equal(t, id, fp.SegmentID)

	blkId1 := common.NextGlobalSeqNum()
	blk1, err := seg.OpenBlock(blkId1, 2, nil)
	assert.Nil(t, err)
	blkTs1 := common.NextGlobalSeqNum()
	blk1.WriteTS(blkTs1)

	ts, _ := blk1.ReadTS()
	assert.Equal(t, blkTs1, ts)
	blk1.Close()
	t.Log(seg.String())
	seg.Unref()
}
