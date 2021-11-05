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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
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
		if colInfo.PrimaryKey {
			schema.PrimaryKey = idx
			logutil.Debugf("Table to schema, schema.PrimaryKey is %d, its name is %v.", schema.PrimaryKey, colInfo.Name)
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

func GetLogIndexFromTableOpCtx(ctx *dbi.TableOpCtx) *shard.Index {
	return &shard.Index{
		ShardId: ctx.ShardId,
		Id:      shard.SimpleIndexId(ctx.OpIndex),
	}
}

func GetLogIndexFromDropTableCtx(ctx *dbi.DropTableCtx) *shard.Index {
	return &shard.Index{
		ShardId: ctx.ShardId,
		Id:      shard.SimpleIndexId(ctx.OpIndex),
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
		Capacity: uint64(ctx.Data.Vecs[0].Length()),
	}
}
