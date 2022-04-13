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
	"math/rand"
	"time"

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
	Name    string   `json:"name"`
	Type    IndexT   `json:"type"`
	Columns []uint16 `json:"cols"`
}

func NewIndexInfo(name string, typ IndexT, colIdx ...int) *IndexInfo {
	index := &IndexInfo{
		Name:    name,
		Type:    typ,
		Columns: make([]uint16, 0),
	}
	for _, col := range colIdx {
		index.Columns = append(index.Columns, uint16(col))
	}
	return index
}

type ColDef struct {
	Name string     `json:"name"`
	Idx  int        `json:"idx"`
	Type types.Type `json:"type"`
}

type IndexSchema struct {
	Indice []*IndexInfo `json:"indice"`
}

func (is *IndexSchema) MakeIndex(name string, typ IndexT, colIdx ...int) (*IndexInfo, error) {
	index := NewIndexInfo(name, typ, colIdx...)
	err := is.Append(index)
	return index, err
}

func (is *IndexSchema) Append(index *IndexInfo) error {
	// TODO: validation
	for _, idx := range is.Indice {
		if idx.Name == index.Name {
			return ErrDupIndex
		}
	}
	is.Indice = append(is.Indice, index)
	return nil
}

func (is *IndexSchema) Extend(indice []*IndexInfo) error {
	// TODO: validation
	names := make(map[string]bool)
	for _, index := range is.Indice {
		names[index.Name] = true
	}
	for _, index := range indice {
		_, ok := names[index.Name]
		if ok {
			return ErrDupIndex
		}
	}
	is.Indice = append(is.Indice, indice...)
	return nil
}

func (is *IndexSchema) DropByName(name string) error {
	found := false
	dropIdx := 0
	for i, index := range is.Indice {
		if name == index.Name {
			found = true
			dropIdx = i
			break
		}
	}
	if !found {
		return ErrIndexNotFound
	}
	is.Indice = append(is.Indice[:dropIdx], is.Indice[dropIdx+1:]...)
	return nil
}

func (is *IndexSchema) DropByNames(names []string) error {
	nameMap := make(map[string]int)
	idx := make([]int, 0)
	for i, index := range is.Indice {
		nameMap[index.Name] = i
	}
	for _, name := range names {
		pos, ok := nameMap[name]
		if !ok {
			return ErrIndexNotFound
		}
		idx = append(idx, pos)
	}
	for i := len(idx) - 1; i >= 0; i-- {
		pos := idx[i]
		is.Indice = append(is.Indice[:pos], is.Indice[pos+1:]...)
	}
	return nil
}

func (is *IndexSchema) Merge(schema *IndexSchema) error {
	return is.Extend(schema.Indice)
}

func (is *IndexSchema) String() string {
	if is == nil {
		return "null"
	}
	s := fmt.Sprintf("Indice[%d][", len(is.Indice))
	names := ""
	for _, index := range is.Indice {
		names = fmt.Sprintf("%s\"%s\",", names, index.Name)
	}
	s = fmt.Sprintf("%s%s]", s, names)
	return s
}

func (is *IndexSchema) IndiceNum() int {
	if is == nil {
		return 0
	}
	return len(is.Indice)
}

func NewIndexSchema() *IndexSchema {
	return &IndexSchema{
		Indice: make([]*IndexInfo, 0),
	}
}

type Schema struct {
	Name             string         `json:"name"`
	ColDefs          []*ColDef      `json:"cols"`
	NameIndex        map[string]int `json:"nindex"`
	BlockMaxRows     uint64         `json:"blkrows"`
	PrimaryKey       int            `json:"primarykey"`
	SegmentMaxBlocks uint64         `json:"segblocks"`
}

func NewEmptySchema(name string) *Schema {
	return &Schema{
		Name:      name,
		ColDefs:   make([]*ColDef, 0),
		NameIndex: make(map[string]int),
	}
}

func (s *Schema) AppendCol(name string, typ types.Type) {
	colDef := &ColDef{
		Name: name,
		Type: typ,
		Idx:  len(s.ColDefs),
	}
	s.ColDefs = append(s.ColDefs, colDef)
	s.NameIndex[name] = colDef.Idx
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
	rand.Seed(time.Now().UnixNano())
	schema := NewEmptySchema(fmt.Sprintf("%d", rand.Intn(1000000)))
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		schema.AppendCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.T_int32, Size: 4, Width: 4})
	}
	return schema
}

// MockSchemaAll if char/varchar is needed, colCnt = 14, otherwise colCnt = 12
func MockSchemaAll(colCnt int) *Schema {
	schema := NewEmptySchema(fmt.Sprintf("%d", rand.Intn(1000000)))
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		var typ types.Type
		switch i {
		case 0:
			typ = types.Type{
				Oid:   types.T_int8,
				Size:  1,
				Width: 8,
			}
		case 1:
			typ = types.Type{
				Oid:   types.T_int16,
				Size:  2,
				Width: 16,
			}
		case 2:
			typ = types.Type{
				Oid:   types.T_int32,
				Size:  4,
				Width: 32,
			}
		case 3:
			typ = types.Type{
				Oid:   types.T_int64,
				Size:  8,
				Width: 64,
			}
		case 4:
			typ = types.Type{
				Oid:   types.T_uint8,
				Size:  1,
				Width: 8,
			}
		case 5:
			typ = types.Type{
				Oid:   types.T_uint16,
				Size:  2,
				Width: 16,
			}
		case 6:
			typ = types.Type{
				Oid:   types.T_uint32,
				Size:  4,
				Width: 32,
			}
		case 7:
			typ = types.Type{
				Oid:   types.T_uint64,
				Size:  8,
				Width: 64,
			}
		case 8:
			typ = types.Type{
				Oid:   types.T_float32,
				Size:  4,
				Width: 32,
			}
		case 9:
			typ = types.Type{
				Oid:   types.T_float64,
				Size:  8,
				Width: 64,
			}
		case 10:
			typ = types.Type{
				Oid:   types.T_date,
				Size:  4,
				Width: 32,
			}
		case 11:
			typ = types.Type{
				Oid:   types.T_datetime,
				Size:  8,
				Width: 64,
			}
		case 12:
			typ = types.Type{
				Oid:   types.T_varchar,
				Size:  24,
				Width: 100,
			}
		case 13:
			typ = types.Type{
				Oid:   types.T_char,
				Size:  24,
				Width: 100,
			}
		case 14:
			typ = types.Type{
				Oid:   types.T_decimal128,
				Size:  16,
				Width: 128,
			}
		}
		schema.AppendCol(name, typ)
	}
	return schema
}
