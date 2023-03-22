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

package common

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func MustUuid1() types.Uuid {
	return types.Uuid(uuid.Must(uuid.NewUUID()))
}

func NewSegmentid() types.Uuid {
	return MustUuid1()
}

func NewObjectName(seg *types.Uuid, filen uint16) string {
	return fmt.Sprintf("%s-%d", seg.ToString(), filen)
}

func NewBlockid(segid *types.Uuid, fileOffset, blkOffset uint16) types.Blockid {
	var id types.Blockid
	size := types.UuidSize
	copy(id[:size], segid[:])
	binary.BigEndian.PutUint16(id[size:size+2], fileOffset)
	binary.BigEndian.PutUint16(id[size+2:size+4], blkOffset)
	return id
}

func NewRowid(blkid *types.Blockid, offset uint32) types.Rowid {
	var rowid types.Rowid
	size := types.BlockidSize
	copy(rowid[:size], blkid[:])
	binary.BigEndian.PutUint32(rowid[size:size+4], offset)
	return rowid
}

func IsEmptySegid(id *types.Uuid) bool {
	for _, x := range id[:] {
		if x != 0 {
			return false
		}
	}
	return true
}

func IsEmptyBlkid(id *types.Blockid) bool {
	for _, x := range id[:] {
		if x != 0 {
			return false
		}
	}
	return true
}

func MustSegmentidFromMetalocName(name string) (types.Uuid, uint16) {
	id, err := types.ParseUuid(name[:36])
	if err != nil {
		panic(fmt.Sprintf("step1:%q is not valid object name: %v", name, err))
	}

	filen, err := strconv.ParseUint(name[37:], 10, 16)
	if err != nil {
		panic(fmt.Sprintf("step2:%q is not valid object name: %v", name, err))
	}
	return id, uint16(filen)
}

type IdAllocator struct {
	id atomic.Uint64
}

func (id *IdAllocator) Get() uint64 {
	return id.id.Load()
}

func (id *IdAllocator) Alloc() uint64 {
	return id.id.Add(1)
}

func (id *IdAllocator) Set(val uint64) {
	id.id.Store(val)
}
