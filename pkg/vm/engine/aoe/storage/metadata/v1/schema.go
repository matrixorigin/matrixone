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

package metadata

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe"
)

func (s *Schema) Valid() bool {
	if s == nil {
		return false
	}
	if len(s.ColDefs) == 0 {
		return false
	}

	names := make(map[string]bool)
	for idx, colDef := range s.ColDefs {
		if idx != colDef.Idx {
			return false
		}
		_, ok := names[colDef.Name]
		if ok {
			return false
		}
		names[colDef.Name] = true
	}
	return true
}

func (s *Schema) Types() (ts []types.Type) {
	for _, colDef := range s.ColDefs {
		ts = append(ts, colDef.Type)
	}
	return ts
}

func (s *Schema) GetColIdx(attr string) int {
	idx, ok := s.NameIdMap[attr]
	if !ok {
		return -1
	}
	return idx
}

func (s *Schema) Clone() *Schema {
	newSchema := &Schema{
		ColDefs:   make([]*ColDef, 0),
		Indices:   make([]*IndexInfo, 0),
		NameIdMap: make(map[string]int),
		Name:      s.Name,
	}
	for _, colDef := range s.ColDefs {
		newColDef := *colDef
		newSchema.NameIdMap[colDef.Name] = len(newSchema.ColDefs)
		newSchema.ColDefs = append(newSchema.ColDefs, &newColDef)
	}
	for _, indexInfo := range s.Indices {
		newInfo := *indexInfo
		newSchema.Indices = append(newSchema.Indices, &newInfo)
	}
	return newSchema
}

// MockSchemaAll if char/varchar is needed, colCnt = 14, otherwise colCnt = 12
func MockSchemaAll(colCnt int) *Schema {
	schema := &Schema{
		Indices:   make([]*IndexInfo, 0),
		ColDefs:   make([]*ColDef, colCnt),
		NameIdMap: make(map[string]int),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colDef := &ColDef{
			Name: name,
			Idx:  i,
		}
		schema.ColDefs[i] = colDef
		schema.NameIdMap[colDef.Name] = i
		switch i {
		case 0:
			colDef.Type = types.Type{
				Oid:       types.T_int8,
				Size:      1,
				Width:     8,
			}
		case 1:
			colDef.Type = types.Type{
				Oid:       types.T_int16,
				Size:      2,
				Width:     16,
			}
		case 2:
			colDef.Type = types.Type{
				Oid:       types.T_int32,
				Size:      4,
				Width:     32,
			}
		case 3:
			colDef.Type = types.Type{
				Oid:       types.T_int64,
				Size:      8,
				Width:     64,
			}
		case 4:
			colDef.Type = types.Type{
				Oid:       types.T_uint8,
				Size:      1,
				Width:     8,
			}
		case 5:
			colDef.Type = types.Type{
				Oid:       types.T_uint16,
				Size:      2,
				Width:     16,
			}
		case 6:
			colDef.Type = types.Type{
				Oid:       types.T_uint32,
				Size:      4,
				Width:     32,
			}
		case 7:
			colDef.Type = types.Type{
				Oid:       types.T_uint64,
				Size:      8,
				Width:     64,
			}
		case 8:
			colDef.Type = types.Type{
				Oid:       types.T_float32,
				Size:      4,
				Width:     32,
			}
		case 9:
			colDef.Type = types.Type{
				Oid:       types.T_float64,
				Size:      8,
				Width:     64,
			}
		case 10:
			colDef.Type = types.Type{
				Oid:       types.T_date,
				Size:      4,
				Width:     32,
			}
		case 11:
			colDef.Type = types.Type{
				Oid:       types.T_datetime,
				Size:      8,
				Width:     64,
			}
		case 12:
			colDef.Type = types.Type{
				Oid:       types.T_varchar,
				Size:      24,
				Width:     100,
			}
		case 13:
			colDef.Type = types.Type{
				Oid:       types.T_char,
				Size:      24,
				Width:     100,
			}
		}
	}
	return schema
}

func MockSchema(colCnt int) *Schema {
	schema := &Schema{
		ColDefs:   make([]*ColDef, colCnt),
		Indices:   make([]*IndexInfo, 0),
		NameIdMap: make(map[string]int),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colDef := &ColDef{
			Idx:  i,
			Name: name,
			Type: types.Type{Oid: types.T_int32, Size: 4, Width: 4},
		}
		schema.ColDefs[i] = colDef
		schema.NameIdMap[colDef.Name] = i
	}
	return schema
}

func MockVarCharSchema(colCnt int) *Schema {
	schema := &Schema{
		ColDefs:   make([]*ColDef, colCnt),
		Indices:   make([]*IndexInfo, 0),
		NameIdMap: make(map[string]int),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colDef := &ColDef{
			Idx:  i,
			Name: name,
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		}
		schema.ColDefs[i] = colDef
		schema.NameIdMap[colDef.Name] = i
	}
	return schema
}

func MockDateSchema(colCnt int) *Schema {
	schema := &Schema{
		ColDefs:   make([]*ColDef, colCnt),
		Indices:   make([]*IndexInfo, 0),
		NameIdMap: make(map[string]int),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		var colDef *ColDef
		if i == 0 {
			colDef = &ColDef{
				Name: name,
				Idx:  i,
				Type: types.Type{
					Oid:       types.T_date,
					Size:      4,
					Width:     4,
					Precision: 0,
				},
			}
		} else {
			colDef = &ColDef{
				Name: name,
				Idx:  i,
				Type: types.Type{
					Oid:       types.T_datetime,
					Size:      8,
					Width:     8,
					Precision: 0,
				},
			}
		}
		schema.ColDefs[i] = colDef
		schema.NameIdMap[colDef.Name] = i
	}
	return schema
}

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
		indexInfo := aoe.IndexInfo{Type: uint64(ZoneMap), Columns: []uint64{uint64(i)}}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	}
	return tblInfo
}

