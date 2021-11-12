package adaptor

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
)

func MockTableInfo(colCnt int) *aoe.TableInfo {
	tblInfo := &aoe.TableInfo{
		Name:    "mocktbl",
		Columns: make([]aoe.ColumnInfo, 0),
		Indices: make([]aoe.IndexInfo, 0),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colInfo := aoe.ColumnInfo{
			Name: name,
		}
		if i == 1 {
			colInfo.Type = types.Type{Oid: types.T(types.T_varchar), Size: 24}
		} else {
			colInfo.Type = types.Type{Oid: types.T_int32, Size: 4, Width: 4}
		}
		indexInfo := aoe.IndexInfo{Type: uint64(metadata.ZoneMap), Columns: []uint64{uint64(i)}}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	}
	return tblInfo
}

func TableInfoToSchema(catalog *metadata.Catalog, info *aoe.TableInfo) *metadata.Schema {
	schema := &metadata.Schema{
		Name:      info.Name,
		ColDefs:   make([]*metadata.ColDef, 0),
		Indices:   make([]*metadata.IndexInfo, 0),
		NameIndex: make(map[string]int),
	}
	for idx, colInfo := range info.Columns {
		newInfo := &metadata.ColDef{
			Name: colInfo.Name,
			Idx:  idx,
			Type: colInfo.Type,
		}
		schema.NameIndex[newInfo.Name] = len(schema.ColDefs)
		schema.ColDefs = append(schema.ColDefs, newInfo)
	}
	for _, indexInfo := range info.Indices {
		newInfo := &metadata.IndexInfo{
			Id:      catalog.NextIndexId(),
			Type:    metadata.IndexT(indexInfo.Type),
			Columns: make([]uint16, 0),
		}
		for _, col := range indexInfo.Columns {
			newInfo.Columns = append(newInfo.Columns, uint16(col))
		}
		schema.Indices = append(schema.Indices, newInfo)
	}

	return schema
}

func GetLogIndexFromTableOpCtx(ctx *dbi.TableOpCtx) *metadata.LogIndex {
	return &metadata.LogIndex{
		ShardId: ctx.ShardId,
		Id:      metadata.SimpleBatchId(ctx.OpIndex),
	}
}

func GetLogIndexFromDropTableCtx(ctx *dbi.DropTableCtx) *metadata.LogIndex {
	return &metadata.LogIndex{
		ShardId: ctx.ShardId,
		Id:      metadata.SimpleBatchId(ctx.OpIndex),
	}
}

func GetLogIndexFromAppendCtx(ctx *dbi.AppendCtx) *metadata.LogIndex {
	return &metadata.LogIndex{
		ShardId: ctx.ShardId,
		Id: metadata.LogBatchId{
			Id:     ctx.OpIndex,
			Offset: uint32(ctx.OpOffset),
			Size:   uint32(ctx.OpSize),
		},
		Capacity: uint64(vector.Length(ctx.Data.Vecs[0])),
	}
}
