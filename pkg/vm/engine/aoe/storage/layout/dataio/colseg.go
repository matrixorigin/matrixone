package dataio

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

type ColSegmentFile struct {
	SegmentFile ISegmentFile
	ColIdx      uint64
}

func (csf *ColSegmentFile) ReadPart(id common.ID, buf []byte) {
	csf.SegmentFile.ReadPart(csf.ColIdx, id, buf)
}

type MockColSegmentFile struct {
}

func (msf *MockColSegmentFile) ReadPart(id common.ID, buf []byte) {
	log.Infof("MockColSegmentFile ReadPart %s size: %d cap: %d", id.SegmentString(), len(buf), cap(buf))
}
