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

