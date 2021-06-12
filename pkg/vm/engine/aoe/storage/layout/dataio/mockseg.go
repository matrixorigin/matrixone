package dataio

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

type MockSegmentFile struct {
}

func (msf *MockSegmentFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	log.Infof("MockSegmentFile ReadPart %d %s size: %d cap: %d", colIdx, id.SegmentString(), len(buf), cap(buf))
}

func (msf *MockSegmentFile) Close() error {
	return nil
}

func (msf *MockSegmentFile) Destory() {
}

func (msf *MockSegmentFile) RefBlock(id common.ID) {
}

func (msf *MockSegmentFile) UnrefBlock(id common.ID) {
}

func (msf *MockSegmentFile) MakeColSegmentFile(colIdx int) IColSegmentFile {
	return &MockColSegmentFile{}
}
