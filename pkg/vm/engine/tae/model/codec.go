// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// EncodeBlockKeyPrefix [48 Bit (BlockID) + 48 Bit (SegmentID)]
func EncodeBlockKeyPrefix(segmentId, blockId uint64) []byte {
	buf := make([]byte, 12)
	tempBuf := types.EncodeUint64(&segmentId)
	copy(buf[0:], tempBuf[2:])
	tempBuf = types.EncodeUint64(&blockId)
	copy(buf[6:], tempBuf[2:])
	return buf
}

func EncodePhyAddrKeyWithPrefix(prefix []byte, offset uint32) types.Rowid {
	var rowid types.Rowid
	copy(rowid[:], prefix)
	copy(rowid[types.BlockidSize:], types.EncodeUint32(&offset))
	return rowid
}

func DecodePhyAddrKeyFromValue(v any) (segmentId types.Uuid, blockId types.Blockid, offset uint32) {
	rowid := v.(types.Rowid)
	return DecodePhyAddrKey(rowid)
}

func DecodePhyAddrKey(src types.Rowid) (segmentId types.Uuid, blockId types.Blockid, offset uint32) {
	segmentId = src.GetSegid()
	blockId = src.GetBlockid()
	offset = types.DecodeUint32(src[types.BlockidSize:])
	return
}
