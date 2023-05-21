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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func PreparePhyAddrData(id *objectio.Blockid, startRow, length uint32) (col containers.Vector, err error) {
	col = containers.MakeVector(objectio.RowidType)
	vec := col.GetDownstreamVector()
	m := col.GetAllocator()
	if err = objectio.ConstructRowidColumnTo(
		vec, id, startRow, length, m,
	); err != nil {
		col.Close()
		col = nil
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
