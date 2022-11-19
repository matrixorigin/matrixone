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

package tables

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/evictable"
)

func LoadPersistedColumnData(
	mgr base.INodeManager,
	fs *objectio.ObjectFS,
	id *common.ID,
	def *catalog.ColDef,
	location string,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	return evictable.FetchColumnData(
		id,
		def,
		location,
		buffer,
		mgr,
		fs)
}

func ReadPersistedBlockRow(location string) int {
	meta, err := blockio.DecodeMetaLocToMeta(location)
	if err != nil {
		panic(err)
	}
	return int(meta.GetRows())
}

func LoadPersistedDeletes(
	mgr base.INodeManager,
	fs *objectio.ObjectFS,
	location string) (bat *containers.Batch, err error) {
	bat = containers.NewBatch()
	colNames := []string{catalog.PhyAddrColumnName, catalog.AttrCommitTs, catalog.AttrAborted}
	colTypes := []types.Type{types.T_Rowid.ToType(), types.T_TS.ToType(), types.T_bool.ToType()}
	for i := 0; i < 3; i++ {
		vec, err := evictable.FetchDeltaData(nil, mgr, fs, location, uint16(i), colTypes[i])
		if err != nil {
			return bat, err
		}
		bat.AddVector(colNames[i], vec)
	}
	return
}
