package mockio

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

var SegmentFileMockFactory = func(name string, id uint64) file.Segment {
	return newSegmentFile(name, id)
}

type segmentFile struct {
	common.RefHelper
	id     *common.ID
	ts     uint64
	blocks map[uint64]*blockFile
	name   string
}

func newSegmentFile(name string, id uint64) *segmentFile {
	sf := &segmentFile{
		blocks: make(map[uint64]*blockFile),
		name:   name,
	}
	sf.id = &common.ID{
		SegmentID: id,
	}
	sf.Ref()
	sf.OnZeroCB = sf.close
	return sf
}

func (sf *segmentFile) Fingerprint() *common.ID { return sf.id }
func (sf *segmentFile) Close() error            { return nil }

func (sf *segmentFile) close() {
	sf.Destory()
}
func (sf *segmentFile) Destory() {
	for _, block := range sf.blocks {
		block.Unref()
	}
	logutil.Infof("Destoring Segment %d", sf.id.SegmentID)
}

func (sf *segmentFile) OpenBlock(id uint64, colCnt int, indexCnt map[int]int) (block file.Block, err error) {
	bf := sf.blocks[id]
	if bf == nil {
		bf = newBlock(id, sf, colCnt, indexCnt)
		sf.blocks[id] = bf
	}
	block = bf
	return
}

func (sf *segmentFile) WriteTS(ts uint64) error {
	sf.ts = ts
	return nil
}

func (sf *segmentFile) ReadTS() uint64 {
	return sf.ts
}
