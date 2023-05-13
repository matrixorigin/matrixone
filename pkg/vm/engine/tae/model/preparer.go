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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func PreparePhyAddrData(typ types.Type, prefix []byte, startRow, length uint32) (col containers.Vector, err error) {
	col = containers.MakeVector(typ)
	col.PreExtend(int(length))
	vec := col.GetDownstreamVector()
	m := col.GetAllocator()
	for i := uint32(0); i < length; i++ {
		rowid := EncodePhyAddrKeyWithPrefix(prefix, startRow+i)
		if err = vector.AppendFixed[types.Rowid](vec, rowid, false, m); err != nil {
			break
		}
	}
	if err != nil {
		col.Close()
		col = nil
	}
	return
}

func PreparePhyAddrDataWithPool(typ types.Type, prefix []byte, startRow, length uint32, pool *mpool.MPool) (col containers.Vector, err error) {
	col = containers.MakeVector(typ, containers.Options{Allocator: pool})
	for i := uint32(0); i < length; i++ {
		rowid := EncodePhyAddrKeyWithPrefix(prefix, startRow+i)
		col.Append(rowid, false)
	}
	return
}

type PreparedCompactedBlockData struct {
	Columns       *containers.Batch
	SortKey       containers.Vector
	SchemaVersion uint32
	Seqnums       []uint16
}

func NewPreparedCompactedBlockData() *PreparedCompactedBlockData {
	return &PreparedCompactedBlockData{}
}

func (preparer *PreparedCompactedBlockData) Close() {
	if preparer.Columns != nil {
		preparer.Columns.Close()
		preparer.Columns = nil
	}
	if preparer.SortKey != nil {
		preparer.SortKey.Close()
		preparer.SortKey = nil
	}
}
