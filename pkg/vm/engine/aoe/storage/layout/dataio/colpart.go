package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// log "github.com/sirupsen/logrus"
)

type ColPartFile struct {
	SegmentFile base.ISegmentFile
	ID          *common.ID
	Info        common.FileInfo
}

func newPartFile(id *common.ID, host base.ISegmentFile, isMock bool) common.IVFile {
	vf := &ColPartFile{
		SegmentFile: host,
		ID:          id,
	}
	vf.Ref()
	if !isMock {
		vf.Info = &colPartFileStat{
			id: id,
			fileStat: fileStat{
				size:  host.PartSize(uint64(id.Idx), *id, false),
				osize: host.PartSize(uint64(id.Idx), *id, true),
				algo:  uint8(host.DataCompressAlgo(*id)),
			},
		}
		// log.Infof("size, osize, aglo: %d, %d, %d", vf.Info.Size(), vf.Info.OriginSize(), vf.Info.CompressAlgo())
	} else {
		vf.Info = &colPartFileStat{
			id:       id,
			fileStat: fileStat{},
		}
	}
	return vf
}

func (cpf *ColPartFile) Read(buf []byte) (n int, err error) {
	cpf.SegmentFile.ReadPart(uint64(cpf.ID.Idx), *cpf.ID, buf)
	return len(buf), nil
}

func (cpf *ColPartFile) Stat() common.FileInfo {
	return cpf.Info
}

func (cpf *ColPartFile) GetFileType() common.FileType {
	return common.DiskFile
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
