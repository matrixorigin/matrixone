package objectio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestMetaDriver_Replay(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	id := common.NextGlobalSeqNum()
	SegmentFactory.(*segmentFactory).fs = NewObjectFS()
	seg := SegmentFactory.Build(dir, id).(*segmentFile)
	fs := seg.fs
	fs.(*ObjectFS).attr.algo = compress.None
	for i := 0; i < 2; i++ {
		file, err := fs.OpenFile(fmt.Sprintf("1/2/%d.blk", i), os.O_CREATE)
		assert.Nil(t, err)
		_, err = file.Write([]byte(fmt.Sprintf("this is tests %d", i)))
		assert.Nil(t, err)
	}
	for i := 2; i < 4; i++ {
		file, err := fs.OpenFile(fmt.Sprintf("2/3/%d.blk", i), os.O_CREATE)
		assert.Nil(t, err)
		_, err = file.Write([]byte(fmt.Sprintf("this is tests %d", i)))
		assert.Nil(t, err)
	}
	fs1 := NewObjectFS()
	f := fs1.(*ObjectFS)
	f.SetDir(dir)
	f.attr.algo = compress.None
	err := f.RebuildObject()
	assert.Nil(t, err)
	err = f.driver.Replay()
	assert.Nil(t, err)
	assert.Equal(t, len(fs.(*ObjectFS).nodes), len(f.nodes))
}
