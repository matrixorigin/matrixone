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

package moengine

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
			Type:    aoe.IndexT(catalog.ZoneMap),
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
		Type:    aoe.IndexT(catalog.ZoneMap),
		Columns: []uint64{uint64(0)},
		Name:    fmt.Sprintf("idx-%d", 0),
	}
	return &idxInfo
}

func TableInfoToSchema(info *aoe.TableInfo) *catalog.Schema {
	schema := catalog.NewEmptySchema(info.Name)
	for idx, colInfo := range info.Columns {
		newInfo := &catalog.ColDef{
			Name: colInfo.Name,
			Idx:  idx,
			Type: colInfo.Type,
		}
		if colInfo.PrimaryKey {
			schema.PrimaryKey = int32(idx)
			logutil.Debugf("Table to schema, schema.PrimaryKey is %d, its name is %v.", schema.PrimaryKey, colInfo.Name)
		}
		schema.NameIndex[newInfo.Name] = len(schema.ColDefs)
		schema.ColDefs = append(schema.ColDefs, newInfo)
	}

	return schema
}
