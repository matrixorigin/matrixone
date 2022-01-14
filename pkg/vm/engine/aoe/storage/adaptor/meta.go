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

package adaptor

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
)

func MockTableInfo(colCnt int) *aoe.TableInfo {
	tblInfo := &aoe.TableInfo{
		Name:    "mocktbl",
		Columns: make([]aoe.ColumnInfo, 0),
		Indices: make([]aoe.IndexInfo, 0),
	}
	prefix := "mock_"
	indexId := 0
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
		indexId++
		indexInfo := aoe.IndexInfo{
			Type:    aoe.IndexT(metadata.ZoneMap),
			Columns: []uint64{uint64(i)},
			Name:    fmt.Sprintf("idx-%d", indexId),
		}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	}
	return tblInfo
}
func MockIndexInfo() *aoe.IndexInfo {
	idxInfo := aoe.IndexInfo{
		Type:    aoe.IndexT(metadata.ZoneMap),
		Columns: []uint64{uint64(0)},
		Name:    fmt.Sprintf("idx-%d", 0),
	}
	return &idxInfo
}

func TableInfoToSchema(catalog *metadata.Catalog, info *aoe.TableInfo) (*db.TableSchema, *db.IndexSchema) {
	schema := metadata.NewEmptySchema(info.Name)
	for idx, colInfo := range info.Columns {
		newInfo := &metadata.ColDef{
			Name: colInfo.Name,
			Idx:  idx,
			Type: colInfo.Type,
		}
		if colInfo.PrimaryKey {
			schema.PrimaryKey = idx
			logutil.Debugf("Table to schema, schema.PrimaryKey is %d, its name is %v.", schema.PrimaryKey, colInfo.Name)
		}
		schema.NameIndex[newInfo.Name] = len(schema.ColDefs)
		schema.ColDefs = append(schema.ColDefs, newInfo)
	}
	indice := metadata.NewIndexSchema()
	cols := make([]int, 0)
	var err error
	for _, indexInfo := range info.Indices {
		cols = cols[:0]
		for _, col := range indexInfo.Columns {
			cols = append(cols, int(col))
		}
		var tp metadata.IndexT
		switch indexInfo.Type {
		case aoe.ZoneMap:
			tp = metadata.ZoneMap
		case aoe.NumBsi:
			tp = metadata.NumBsi
		case aoe.FixStrBsi:
			tp = metadata.FixStrBsi
		default:
			panic("unsupported index type")
		}
		// if _, err = indice.MakeIndex(indexInfo.Name, metadata.IndexT(indexInfo.Type), cols...); err != nil {
		if _, err = indice.MakeIndex(indexInfo.Name, tp, cols...); err != nil {
			panic(err)
		}
	}

	return schema, indice
}

func IndiceInfoToIndiceSchema(info *aoe.IndexInfo) *db.IndexSchema {
	columns := make([]int, 0, len(info.Columns))
	for _, col := range info.Columns {
		columns = append(columns, int(col))
	}
	indice := metadata.NewIndexSchema()
	var tp metadata.IndexT
	switch info.Type{
	case aoe.NumBsi:
		tp=metadata.NumBsi
	case aoe.FixStrBsi:
		tp=metadata.FixStrBsi
	case aoe.ZoneMap:
		tp=metadata.ZoneMap
	}
	_, err := indice.MakeIndex(info.Name, tp, columns...)
	if err != nil {
		panic(err)
	}
	return indice
}

func GetLogIndexFromTableOpCtx(ctx *dbi.TableOpCtx) *shard.Index {
	return &shard.Index{
		ShardId: ctx.ShardId,
		Id: shard.IndexId{
			Id:     ctx.OpIndex,
			Offset: uint32(ctx.OpOffset),
			Size:   uint32(ctx.OpSize),
		},
	}
}

func GetLogIndexFromDropTableCtx(ctx *dbi.DropTableCtx) *shard.Index {
	return &shard.Index{
		ShardId: ctx.ShardId,
		Id: shard.IndexId{
			Id:     ctx.OpIndex,
			Offset: uint32(ctx.OpOffset),
			Size:   uint32(ctx.OpSize),
		},
	}
}

func GetLogIndexFromAppendCtx(ctx *dbi.AppendCtx) *shard.Index {
	return &shard.Index{
		ShardId: ctx.ShardId,
		Id: shard.IndexId{
			Id:     ctx.OpIndex,
			Offset: uint32(ctx.OpOffset),
			Size:   uint32(ctx.OpSize),
		},
		Capacity: uint64(vector.Length(ctx.Data.Vecs[0])),
	}
}
