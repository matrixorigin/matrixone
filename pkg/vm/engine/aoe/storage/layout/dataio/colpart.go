package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	// log "github.com/sirupsen/logrus"
)

type ColPartFile struct {
	SegmentFile ISegmentFile
	ColIdx      uint64
	ID          *common.ID
}

func (cpf *ColPartFile) Read(buf []byte) (n int, err error) {
	cpf.SegmentFile.ReadPart(cpf.ColIdx, *cpf.ID, buf)
	return len(buf), nil
}

type MockColPartFile struct {
}

func (cpf *MockColPartFile) Read(buf []byte) (n int, err error) {
	return len(buf), nil
}
