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
	id.Idx = uint16(def.Idx)
	return evictable.FetchColumnData(
		buffer,
		mgr,
		id,
		fs,
		id.Idx,
		location,
		def)
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
