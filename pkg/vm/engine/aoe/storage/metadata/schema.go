package md

import (
	"fmt"
	"matrixone/pkg/container/types"
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

func MockSchema(colCnt int) *Schema {
	schema := &Schema{
		ColDefs: make([]*ColDef, colCnt),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colDef := &ColDef{
			Idx:  i,
			Name: name,
			Type: types.Type{types.T_int32, 4, 4, 0},
		}
		schema.ColDefs[i] = colDef
	}
	return schema
}
