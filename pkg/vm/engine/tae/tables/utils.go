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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
)

func constructRowId(id *common.ID, rows uint32) (col containers.Vector, err error) {
	prefix := id.BlockID[:]
	return model.PreparePhyAddrData(
		types.T_Rowid.ToType(),
		prefix,
		0,
		rows,
	)
}

func LoadPersistedColumnData(
	mgr base.INodeManager,
	fs *objectio.ObjectFS,
	id *common.ID,
	def *catalog.ColDef,
	location string,
) (vec containers.Vector, err error) {
	_, _, meta, rows, err := blockio.DecodeLocation(location)
	if err != nil {
		return nil, err
	}
	if def.IsPhyAddr() {
		return constructRowId(id, rows)
	}
	reader, err := blockio.NewObjectReader(fs.Service, location)
	if err != nil {
		return
	}
	bat, err := reader.LoadColumns(context.Background(), []uint16{uint16(def.Idx)}, []uint32{meta.Id()}, nil)
	if err != nil {
		return
	}
	return containers.NewVectorWithSharedMemory(bat[0].Vecs[0]), nil
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
	_, _, meta, _, err := blockio.DecodeLocation(location)
	if err != nil {
		return nil, err
	}
	reader, err := blockio.NewObjectReader(fs.Service, location)
	if err != nil {
		return
	}
	movbat, err := reader.LoadColumns(context.Background(), []uint16{0, 1, 2}, []uint32{meta.Id()}, nil)
	if err != nil {
		return
	}
	bat = containers.NewBatch()
	colNames := []string{catalog.PhyAddrColumnName, catalog.AttrCommitTs, catalog.AttrAborted}
	for i := 0; i < 3; i++ {
		bat.AddVector(colNames[i], containers.NewVectorWithSharedMemory(movbat[0].Vecs[i]))
	}
	return
}
