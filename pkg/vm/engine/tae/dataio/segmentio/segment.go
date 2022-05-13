package segmentio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/layout/segment"
	"sync"
)

var SegmentFileIOFactory = func(name string, id uint64) file.Segment {
	return newSegmentFile(name, id)
}

type segmentFile struct {
	sync.RWMutex
	common.RefHelper
	id     *common.ID
	ts     uint64
	blocks map[uint64]*blockFile
	name   string
	seg    *segment.Segment
}

func (sf *segmentFile) removeData(data *dataFile) {
	if data.file != nil {
		for _, file := range data.file {
			sf.seg.ReleaseFile(file)
		}
	}
}

func (sf *segmentFile) RemoveBlock(id uint64) {
	sf.Lock()
	defer sf.Unlock()
	block := sf.blocks[id]
	for _, column := range block.columns {
		sf.removeData(column.data)
	}
	sf.removeData(block.deletes.dataFile)
	sf.removeData(block.indexMeta)
	block.deletes = nil
	block.indexMeta = nil
	delete(sf.blocks, id)
}

func newSegmentFile(name string, id uint64) *segmentFile {
	sf := &segmentFile{
		blocks: make(map[uint64]*blockFile),
		name:   name,
	}
	sf.seg = &segment.Segment{}
	sf.seg.Init(sf.name)
	sf.seg.Mount()
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
	sf.Lock()
	defer sf.Unlock()
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

func (sf *segmentFile) String() string {
	s := fmt.Sprintf("SegmentFile[%d][\"%s\"][TS=%d][BCnt=%d]", sf.id, sf.name, sf.ts, len(sf.blocks))
	return s
}

func (sf *segmentFile) GetSegmentFile() *segment.Segment {
	return sf.seg
}

func (sf *segmentFile) Sync() error {
	return sf.seg.Sync()
}
