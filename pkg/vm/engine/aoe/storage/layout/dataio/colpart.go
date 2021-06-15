package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	// log "github.com/sirupsen/logrus"
)

type ColPartFile struct {
	SegmentFile ISegmentFile
	ID          *common.ID
}

func (cpf *ColPartFile) Read(buf []byte) (n int, err error) {
	cpf.SegmentFile.ReadPart(uint64(cpf.ID.Idx), *cpf.ID, buf)
	return len(buf), nil
}

type MockColPartFile struct {
}

func (cpf *MockColPartFile) Read(buf []byte) (n int, err error) {
	return len(buf), nil
}
