package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// log "github.com/sirupsen/logrus"
)

type ColPartFile struct {
	SegmentFile base.ISegmentFile
	ID          *common.ID
	Info        base.FileInfo
}

func newPartFile(id *common.ID, host base.ISegmentFile, isMock bool) base.IVirtaulFile {
	vf := &ColPartFile{
		SegmentFile: host,
		ID:          id,
	}
	vf.Ref()
	if !isMock {
		vf.Info = &fileStat{
			name: id.ToPartFilePath(),
			size: host.PartSize(uint64(id.Idx), *id),
		}
	}
	return vf
}

func (cpf *ColPartFile) Read(buf []byte) (n int, err error) {
	cpf.SegmentFile.ReadPart(uint64(cpf.ID.Idx), *cpf.ID, buf)
	return len(buf), nil
}

func (cpf *ColPartFile) Stat() base.FileInfo {
	return cpf.Info
}

func (cpf *ColPartFile) Ref() {
	cpf.SegmentFile.RefBlock(cpf.ID.AsBlockID())
}

func (cpf *ColPartFile) Unref() {
	cpf.SegmentFile.UnrefBlock(cpf.ID.AsBlockID())
}

type MockColPartFile struct {
}

func (cpf *MockColPartFile) Read(buf []byte) (n int, err error) {
	return len(buf), nil
}
