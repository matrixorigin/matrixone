// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// log "github.com/sirupsen/logrus"
)

// ColPartFile is a Reader instance of columnData in block,
// which provides external read services
type ColPartFile struct {

	// SegmentFile is UnsortedSegmentFile or SortedSegmentFile
	SegmentFile base.ISegmentFile

	// ID is block id
	ID *common.ID

	// Info is block fileinfo
	Info common.FileInfo
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

				// column data size
				size: host.PartSize(uint64(id.Idx), *id, false),

				// column data osize
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

	// SortedSegmentFile read one of its own Point
	// UnsortedSegmentFile calls BlockFile to read a Point of .blk
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

func (cpf *ColPartFile) Name() string {
	return cpf.Stat().(*colPartFileStat).Name()
}
