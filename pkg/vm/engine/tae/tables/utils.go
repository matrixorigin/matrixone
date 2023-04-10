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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
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

func generateGetRowClosure(
	typ types.T,
	filter *handle.Filter,
	sortKey containers.Vector,
	rows *roaring.Bitmap) any {
	switch typ {
	case types.T_bool:
		return getPersistedRowByFilterClosureFactory[bool](filter, sortKey, rows)
	case types.T_int8:
		return getPersistedRowByFilterClosureFactory[int8](filter, sortKey, rows)
	case types.T_int16:
		return getPersistedRowByFilterClosureFactory[int16](filter, sortKey, rows)
	case types.T_int32:
		return getPersistedRowByFilterClosureFactory[int32](filter, sortKey, rows)
	case types.T_int64:
		return getPersistedRowByFilterClosureFactory[int64](filter, sortKey, rows)
	case types.T_uint8:
		return getPersistedRowByFilterClosureFactory[uint8](filter, sortKey, rows)
	case types.T_uint16:
		return getPersistedRowByFilterClosureFactory[uint16](filter, sortKey, rows)
	case types.T_uint32:
		return getPersistedRowByFilterClosureFactory[uint32](filter, sortKey, rows)
	case types.T_uint64:
		return getPersistedRowByFilterClosureFactory[uint64](filter, sortKey, rows)
	case types.T_float32:
		return getPersistedRowByFilterClosureFactory[float32](filter, sortKey, rows)
	case types.T_float64:
		return getPersistedRowByFilterClosureFactory[float64](filter, sortKey, rows)
	case types.T_timestamp:
		return getPersistedRowByFilterClosureFactory[types.Timestamp](filter, sortKey, rows)
	case types.T_date:
		return getPersistedRowByFilterClosureFactory[types.Date](filter, sortKey, rows)
	case types.T_time:
		return getPersistedRowByFilterClosureFactory[types.Time](filter, sortKey, rows)
	case types.T_datetime:
		return getPersistedRowByFilterClosureFactory[types.Datetime](filter, sortKey, rows)
	case types.T_decimal64:
		return getPersistedRowByFilterClosureFactory[types.Decimal64](filter, sortKey, rows)
	case types.T_decimal128:
		return getPersistedRowByFilterClosureFactory[types.Decimal128](filter, sortKey, rows)
	case types.T_decimal256:
		return getPersistedRowByFilterClosureFactory[types.Decimal256](filter, sortKey, rows)
	case types.T_TS:
		return getPersistedRowByFilterClosureFactory[types.TS](filter, sortKey, rows)
	case types.T_Rowid:
		return getPersistedRowByFilterClosureFactory[types.Rowid](filter, sortKey, rows)
	case types.T_Blockid:
		return getPersistedRowByFilterClosureFactory[types.Blockid](filter, sortKey, rows)
	case types.T_uuid:
		return getPersistedRowByFilterClosureFactory[types.Uuid](filter, sortKey, rows)
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_json, types.T_text:
		return getPersistedRowByFilterClosureFactory[[]byte](filter, sortKey, rows)
	default:
		panic("unsupport")
	}
}

func getPersistedRowByFilterClosureFactory[T any](
	filter *handle.Filter,
	sortKey containers.Vector,
	rows *roaring.Bitmap) func(v T, _ bool, offset int) error {
	return func(v T, _ bool, offset int) error {
		if compute.CompareGeneric(v, filter.Val, sortKey.GetType().Oid) == 0 {
			row := uint32(offset)
			rows.Add(row)
			return nil
		}
		return nil
	}
}
