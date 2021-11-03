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

package metadata

import (
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type IndexT uint16

const (
	ZoneMap IndexT = iota
	NumBsi
	FixStrBsi
)

type IndexInfo struct {
	Id      uint64   `json:"id"`
	Type    IndexT   `json:"type"`
	Columns []uint16 `json:"cols"`
}

type ColDef struct {
	Name string     `json:"name"`
	Idx  int        `json:"idx"`
	Type types.Type `json:"type"`
}

type Schema struct {
	Name             string         `json:"name"`
	Indices          []*IndexInfo   `json:"indice"`
	ColDefs          []*ColDef      `json:"cols"`
	NameIndex        map[string]int `json:"nindex"`
	BlockMaxRows     uint64         `json:"blkrows"`
	SegmentMaxBlocks uint64         `json:"segblocks"`
}

func (s *Schema) String() string {
	buf, _ := json.Marshal(s)
	return string(buf)
}

func (s *Schema) Types() []types.Type {
	ts := make([]types.Type, len(s.ColDefs))
	for i, colDef := range s.ColDefs {
		ts[i] = colDef.Type
	}
	return ts
}

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

// GetColIdx returns column index for the given column name
// if found, otherwise returns -1.
func (s *Schema) GetColIdx(attr string) int {
	idx, ok := s.NameIndex[attr]
	if !ok {
		return -1
	}
	return idx
}

func MockSchema(colCnt int) *Schema {
	schema := &Schema{
		ColDefs:   make([]*ColDef, colCnt),
		Indices:   make([]*IndexInfo, 0),
		NameIndex: make(map[string]int),
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
		schema.NameIndex[colDef.Name] = i
	}
	return schema
}

// MockSchemaAll if char/varchar is needed, colCnt = 14, otherwise colCnt = 12
func MockSchemaAll(colCnt int) *Schema {
	schema := &Schema{
		Indices:   make([]*IndexInfo, 0),
		ColDefs:   make([]*ColDef, colCnt),
		NameIndex: make(map[string]int),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colDef := &ColDef{
			Name: name,
			Idx:  i,
		}
		schema.ColDefs[i] = colDef
		schema.NameIndex[colDef.Name] = i
		switch i {
		case 0:
			colDef.Type = types.Type{
				Oid:   types.T_int8,
				Size:  1,
				Width: 8,
			}
		case 1:
			colDef.Type = types.Type{
				Oid:   types.T_int16,
				Size:  2,
				Width: 16,
			}
		case 2:
			colDef.Type = types.Type{
				Oid:   types.T_int32,
				Size:  4,
				Width: 32,
			}
		case 3:
			colDef.Type = types.Type{
				Oid:   types.T_int64,
				Size:  8,
				Width: 64,
			}
		case 4:
			colDef.Type = types.Type{
				Oid:   types.T_uint8,
				Size:  1,
				Width: 8,
			}
		case 5:
			colDef.Type = types.Type{
				Oid:   types.T_uint16,
				Size:  2,
				Width: 16,
			}
		case 6:
			colDef.Type = types.Type{
				Oid:   types.T_uint32,
				Size:  4,
				Width: 32,
			}
		case 7:
			colDef.Type = types.Type{
				Oid:   types.T_uint64,
				Size:  8,
				Width: 64,
			}
		case 8:
			colDef.Type = types.Type{
				Oid:   types.T_float32,
				Size:  4,
				Width: 32,
			}
		case 9:
			colDef.Type = types.Type{
				Oid:   types.T_float64,
				Size:  8,
				Width: 64,
			}
		case 10:
			colDef.Type = types.Type{
				Oid:   types.T_date,
				Size:  4,
				Width: 32,
			}
		case 11:
			colDef.Type = types.Type{
				Oid:   types.T_datetime,
				Size:  8,
				Width: 64,
			}
		case 12:
			colDef.Type = types.Type{
				Oid:   types.T_varchar,
				Size:  24,
				Width: 100,
			}
		case 13:
			colDef.Type = types.Type{
				Oid:   types.T_char,
				Size:  24,
				Width: 100,
			}
		}
	}
	return schema
}
