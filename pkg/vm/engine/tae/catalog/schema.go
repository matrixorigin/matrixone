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

package catalog

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type IndexT uint16

const (
	ZoneMap IndexT = iota
)

type IndexInfo struct {
	Id      uint64
	Name    string
	Type    IndexT
	Columns []uint16
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
	Name          string
	Idx           int
	Type          types.Type
	Hidden        int8
	NullAbility   int8
	AutoIncrement int8
	PrimaryIdx    int8
	Comment       string
}

func (def *ColDef) IsHidden() bool  { return def.Hidden == int8(1) }
func (def *ColDef) IsPrimary() bool { return def.PrimaryIdx >= 0 }

type CompoundPK struct {
	Defs   []*ColDef
	search map[int]int
}

func NewCompoundPK() *CompoundPK {
	return &CompoundPK{
		Defs:   make([]*ColDef, 0),
		search: make(map[int]int),
	}
}

func (cpk *CompoundPK) AddDef(def *ColDef) (ok bool) {
	_, found := cpk.search[def.Idx]
	if found {
		return false
	}
	cpk.Defs = append(cpk.Defs, def)
	sort.Slice(cpk.Defs, func(i, j int) bool { return cpk.Defs[i].PrimaryIdx < cpk.Defs[j].PrimaryIdx })
	cpk.search[def.Idx] = int(def.PrimaryIdx)
	return true
}

func (cpk *CompoundPK) Size() int              { return len(cpk.Defs) }
func (cpk *CompoundPK) GetDef(pos int) *ColDef { return cpk.Defs[pos] }

type SinglePK struct {
	Idx int
}

func (uk *SinglePK) IsHidden(schema *Schema) bool {
	return schema.ColDefs[uk.Idx].IsHidden()
}

type Schema struct {
	Name             string
	ColDefs          []*ColDef
	NameIndex        map[string]int
	BlockMaxRows     uint32
	SegmentMaxBlocks uint16
	Comment          string
	SinglePK         *SinglePK
	CompoundPK       *CompoundPK
	HiddenIdx        int
}

func NewEmptySchema(name string) *Schema {
	return &Schema{
		Name:      name,
		ColDefs:   make([]*ColDef, 0),
		NameIndex: make(map[string]int),
		HiddenIdx: -1,
	}
}

func (s *Schema) HiddenKeyDef() *ColDef { return s.ColDefs[s.HiddenIdx] }

func (s *Schema) IsSinglePK() bool { return s.SinglePK != nil }

// Should be call only if IsSinglePK is checked
func (s *Schema) GetPrimaryKeyIdx() int { return s.SinglePK.Idx }

func (s *Schema) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &s.BlockMaxRows); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &s.SegmentMaxBlocks); err != nil {
		return
	}
	n = 4 + 4
	var sn int64
	if s.Name, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	if s.Comment, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	colCnt := uint16(0)
	if err = binary.Read(r, binary.BigEndian, &colCnt); err != nil {
		return
	}
	n += 2
	colBuf := make([]byte, encoding.TypeSize)
	for i := uint16(0); i < colCnt; i++ {
		if _, err = r.Read(colBuf); err != nil {
			return
		}
		n += int64(encoding.TypeSize)
		def := new(ColDef)
		def.Type = encoding.DecodeType(colBuf)
		if def.Name, sn, err = common.ReadString(r); err != nil {
			return
		}
		n += sn
		if def.Comment, sn, err = common.ReadString(r); err != nil {
			return
		}
		n += sn
		if err = binary.Read(r, binary.BigEndian, &def.NullAbility); err != nil {
			return
		}
		n += 1
		if err = binary.Read(r, binary.BigEndian, &def.Hidden); err != nil {
			return
		}
		n += 1
		if err = binary.Read(r, binary.BigEndian, &def.AutoIncrement); err != nil {
			return
		}
		n += 1
		if err = binary.Read(r, binary.BigEndian, &def.PrimaryIdx); err != nil {
			return
		}
		n += 1
		if err = s.AppendColDef(def); err != nil {
			return
		}
	}
	err = s.Finalize(true)
	return
}

func (s *Schema) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if err = binary.Write(&w, binary.BigEndian, s.BlockMaxRows); err != nil {
		return
	}
	if err = binary.Write(&w, binary.BigEndian, s.SegmentMaxBlocks); err != nil {
		return
	}
	if _, err = common.WriteString(s.Name, &w); err != nil {
		return
	}
	if _, err = common.WriteString(s.Comment, &w); err != nil {
		return
	}
	if err = binary.Write(&w, binary.BigEndian, uint16(len(s.ColDefs))); err != nil {
		return
	}
	for _, def := range s.ColDefs {
		if _, err = w.Write(encoding.EncodeType(def.Type)); err != nil {
			return
		}
		if _, err = common.WriteString(def.Name, &w); err != nil {
			return
		}
		if _, err = common.WriteString(def.Comment, &w); err != nil {
			return
		}
		if err = binary.Write(&w, binary.BigEndian, def.NullAbility); err != nil {
			return
		}
		if err = binary.Write(&w, binary.BigEndian, def.Hidden); err != nil {
			return
		}
		if err = binary.Write(&w, binary.BigEndian, def.AutoIncrement); err != nil {
			return
		}
		if err = binary.Write(&w, binary.BigEndian, def.PrimaryIdx); err != nil {
			return
		}
	}
	buf = w.Bytes()
	return
}

func (s *Schema) AppendColDef(def *ColDef) (err error) {
	def.Idx = len(s.ColDefs)
	s.ColDefs = append(s.ColDefs, def)
	_, existed := s.NameIndex[def.Name]
	if existed {
		err = fmt.Errorf("%w: duplicate column \"%s\"", ErrSchemaValidation, def.Name)
		return
	}
	if def.IsHidden() {
		s.HiddenIdx = def.Idx
	}
	s.NameIndex[def.Name] = def.Idx
	return
}

func (s *Schema) AppendPKCol(name string, typ types.Type, idx int) error {
	def := &ColDef{
		Name:       name,
		Type:       typ,
		PrimaryIdx: int8(idx),
	}
	return s.AppendColDef(def)
}

func (s *Schema) AppendCol(name string, typ types.Type) error {
	def := &ColDef{
		Name:       name,
		Type:       typ,
		PrimaryIdx: -1,
	}
	return s.AppendColDef(def)
}

func (s *Schema) String() string {
	buf, _ := json.Marshal(s)
	return string(buf)
}

func (s *Schema) IsPartOfPK(idx int) bool {
	if s.SinglePK != nil {
		return s.SinglePK.Idx == idx
	}
	panic("implement me")
}

func (s *Schema) GetSinglePKType() types.Type {
	return s.ColDefs[s.SinglePK.Idx].Type
}

func (s *Schema) GetSinglePKColDef() *ColDef {
	return s.ColDefs[s.SinglePK.Idx]
}

func (s *Schema) IsHiddenPK() bool { return s.ColDefs[s.SinglePK.Idx].IsHidden() }

func (s *Schema) Attrs() []string {
	attrs := make([]string, len(s.ColDefs)-1)
	for i, def := range s.ColDefs[:len(s.ColDefs)-1] {
		attrs[i] = def.Name
	}
	return attrs
}

func (s *Schema) Types() []types.Type {
	ts := make([]types.Type, len(s.ColDefs)-1)
	for i, def := range s.ColDefs[:len(s.ColDefs)-1] {
		ts[i] = def.Type
	}
	return ts
}

func (s *Schema) AllTypes() []types.Type {
	ts := make([]types.Type, len(s.ColDefs))
	for i, def := range s.ColDefs {
		ts[i] = def.Type
	}
	return ts
}

func (s *Schema) Finalize(rebuild bool) (err error) {
	if s == nil {
		err = fmt.Errorf("%w: nil schema", ErrSchemaValidation)
		return
	}
	if len(s.ColDefs) == 0 {
		err = fmt.Errorf("%w: empty schema", ErrSchemaValidation)
		return
	}
	if !rebuild {
		hiddenDef := &ColDef{
			Name:       HiddenColumnName,
			Comment:    HiddenColumnComment,
			Type:       HiddenColumnType,
			Hidden:     int8(1),
			PrimaryIdx: -1,
		}
		if err = s.AppendColDef(hiddenDef); err != nil {
			return
		}
	}

	pkIdx := make([]int, 0)
	names := make(map[string]bool)
	for idx, def := range s.ColDefs {
		// Check column idx validility
		if idx != def.Idx {
			err = fmt.Errorf("%w: wrong column index %d specified for \"%s\"", ErrSchemaValidation, def.Idx, def.Name)
			return
		}
		// Check unique name
		_, ok := names[def.Name]
		if ok {
			err = fmt.Errorf("%w: duplicate column \"%s\"", ErrSchemaValidation, def.Name)
			return
		}
		names[def.Name] = true
		if def.IsPrimary() {
			pkIdx = append(pkIdx, idx)
		}
		if def.IsHidden() {
			s.HiddenIdx = def.Idx
		}
	}

	if len(pkIdx) == 1 {
		// One pk defined
		def := s.ColDefs[pkIdx[0]]
		if def.PrimaryIdx != 0 {
			err = fmt.Errorf("%w: bad primary idx %d, should be 0", ErrSchemaValidation, def.PrimaryIdx)
			return
		}
		s.SinglePK = &SinglePK{Idx: def.Idx}
		s.CompoundPK = nil
	} else if len(pkIdx) == 0 {
		// No pk specified
		def := s.ColDefs[len(s.ColDefs)-1]
		def.PrimaryIdx = 0
		s.SinglePK = &SinglePK{Idx: def.Idx}
		s.CompoundPK = nil
	} else {
		// Compound pk defined
		s.CompoundPK = NewCompoundPK()
		for _, idx := range pkIdx {
			def := s.ColDefs[idx]
			if def.Idx != idx {
				err = fmt.Errorf("%w: bad column def", ErrSchemaValidation)
				return
			}
			if ok := s.CompoundPK.AddDef(def); !ok {
				err = fmt.Errorf("%w: duplicated primary idx specified", ErrSchemaValidation)
				return
			}
		}
		for i, def := range s.CompoundPK.Defs {
			if int(def.PrimaryIdx) != i {
				err = fmt.Errorf("%w: duplicated primary idx specified", ErrSchemaValidation)
				return
			}
		}
	}
	// if !s.ColDefs[s.HiddenIdx].IsHidden() {
	// 	err = fmt.Errorf("%w: wrong hidden column idx", ErrSchemaValidation)
	// }
	return
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

func MockSchema(colCnt int, pkIdx int) *Schema {
	rand.Seed(time.Now().UnixNano())
	schema := NewEmptySchema(fmt.Sprintf("%d", rand.Intn(1000000)))
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		if pkIdx == i {
			_ = schema.AppendPKCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.T_int32, Size: 4, Width: 4}, 0)
		} else {
			_ = schema.AppendCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.T_int32, Size: 4, Width: 4})
		}
	}
	_ = schema.Finalize(false)
	return schema
}

// MockSchemaAll if char/varchar is needed, colCnt = 14, otherwise colCnt = 12
func MockSchemaAll(colCnt int, pkIdx int) *Schema {
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
		}
		if pkIdx == i {
			_ = schema.AppendPKCol(name, typ, 0)
		} else {
			_ = schema.AppendCol(name, typ)
		}
	}
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10
	_ = schema.Finalize(false)
	return schema
}
