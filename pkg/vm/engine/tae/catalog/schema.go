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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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

type Default struct {
	Set   bool
	Null  bool
	Value any
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
	SortIdx       int8
	SortKey       int8
	Primary       int8
	Comment       string
	Default       Default
}

func (def *ColDef) GetName() string     { return def.Name }
func (def *ColDef) GetType() types.Type { return def.Type }

func (def *ColDef) Nullable() bool  { return def.NullAbility == int8(1) }
func (def *ColDef) IsHidden() bool  { return def.Hidden == int8(1) }
func (def *ColDef) IsPrimary() bool { return def.Primary == int8(1) }
func (def *ColDef) IsSortKey() bool { return def.SortKey == int8(1) }

type SortKey struct {
	Defs      []*ColDef
	search    map[int]int
	isPrimary bool
}

func NewSortKey() *SortKey {
	return &SortKey{
		Defs:   make([]*ColDef, 0),
		search: make(map[int]int),
	}
}

func (cpk *SortKey) AddDef(def *ColDef) (ok bool) {
	_, found := cpk.search[def.Idx]
	if found {
		return false
	}
	if def.IsPrimary() {
		cpk.isPrimary = true
	}
	cpk.Defs = append(cpk.Defs, def)
	sort.Slice(cpk.Defs, func(i, j int) bool { return cpk.Defs[i].SortIdx < cpk.Defs[j].SortIdx })
	cpk.search[def.Idx] = int(def.SortIdx)
	return true
}

func (cpk *SortKey) IsSinglePK() bool               { return cpk.isPrimary && cpk.Size() == 1 }
func (cpk *SortKey) IsPrimary() bool                { return cpk.isPrimary }
func (cpk *SortKey) Size() int                      { return len(cpk.Defs) }
func (cpk *SortKey) GetDef(pos int) *ColDef         { return cpk.Defs[pos] }
func (cpk *SortKey) HasColumn(idx int) (found bool) { _, found = cpk.search[idx]; return }
func (cpk *SortKey) GetSingleIdx() int              { return cpk.Defs[0].Idx }

type Schema struct {
	Name             string
	ColDefs          []*ColDef
	NameIndex        map[string]int
	BlockMaxRows     uint32
	SegmentMaxBlocks uint16
	Comment          string

	SortKey   *SortKey
	HiddenKey *ColDef
}

func NewEmptySchema(name string) *Schema {
	return &Schema{
		Name:      name,
		ColDefs:   make([]*ColDef, 0),
		NameIndex: make(map[string]int),
	}
}

func (s *Schema) Clone() *Schema {
	buf, err := s.Marshal()
	if err != nil {
		panic(err)
	}
	ns := NewEmptySchema(s.Name)
	r := bytes.NewBuffer(buf)
	if _, err = ns.ReadFrom(r); err != nil {
		panic(err)
	}
	return ns
}
func (s *Schema) GetSortKeyType() types.Type {
	if s.IsSinglePK() {
		return s.GetSingleSortKey().Type
	}
	t := types.CompoundKeyType
	// TODO: set correct width
	return t
}
func (s *Schema) IsSinglePK() bool        { return s.SortKey != nil && s.SortKey.IsSinglePK() }
func (s *Schema) IsSingleSortKey() bool   { return s.SortKey != nil && s.SortKey.Size() == 1 }
func (s *Schema) IsCompoundPK() bool      { return s.IsCompoundSortKey() && s.SortKey.IsPrimary() }
func (s *Schema) IsCompoundSortKey() bool { return s.SortKey != nil && s.SortKey.Size() > 1 }
func (s *Schema) HasPK() bool             { return s.SortKey != nil && s.SortKey.IsPrimary() }
func (s *Schema) HasSortKey() bool        { return s.SortKey != nil }

// Should be call only if IsSinglePK is checked
func (s *Schema) GetSingleSortKey() *ColDef { return s.SortKey.Defs[0] }
func (s *Schema) GetSingleSortKeyIdx() int  { return s.SortKey.Defs[0].Idx }

func (s *Schema) GetSortKeyCnt() int {
	if s.SortKey == nil {
		return 0
	}
	return s.SortKey.Size()
}

func MarshalDefault(w *bytes.Buffer, typ types.Type, data Default) (err error) {
	if err = binary.Write(w, binary.BigEndian, data.Set); err != nil {
		return
	}
	if !data.Set {
		return
	}
	if err = binary.Write(w, binary.BigEndian, data.Null); err != nil {
		return
	}
	if data.Null {
		return
	}
	value := types.EncodeValue(data.Value, typ)
	if err = binary.Write(w, binary.BigEndian, uint16(len(value))); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, value); err != nil {
		return
	}
	return nil
}
func UnMarshalDefault(r io.Reader, typ types.Type, data *Default) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &data.Set); err != nil {
		return
	}
	n = 1
	if !data.Set {
		return n, nil
	}
	if err = binary.Read(r, binary.BigEndian, &data.Null); err != nil {
		return
	}
	n += 1
	if data.Null {
		return n, nil
	}
	var valueLen uint16 = 0
	if err = binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return
	}
	n += 2
	buf := make([]byte, valueLen)
	if _, err = r.Read(buf); err != nil {
		return
	}
	data.Value = types.DecodeValue(buf, typ)
	n += int64(valueLen)
	return n, nil
}

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
	colBuf := make([]byte, types.TypeSize)
	for i := uint16(0); i < colCnt; i++ {
		if _, err = r.Read(colBuf); err != nil {
			return
		}
		n += int64(types.TypeSize)
		def := new(ColDef)
		def.Type = types.DecodeType(colBuf)
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
		if err = binary.Read(r, binary.BigEndian, &def.SortIdx); err != nil {
			return
		}
		n += 1
		if err = binary.Read(r, binary.BigEndian, &def.Primary); err != nil {
			return
		}
		n += 1
		if err = binary.Read(r, binary.BigEndian, &def.SortKey); err != nil {
			return
		}
		n += 1
		def.Default = Default{}
		if sn, err = UnMarshalDefault(r, def.Type, &def.Default); err != nil {
			return
		}
		n += sn
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
		if _, err = w.Write(types.EncodeType(def.Type)); err != nil {
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
		if err = binary.Write(&w, binary.BigEndian, def.SortIdx); err != nil {
			return
		}
		if err = binary.Write(&w, binary.BigEndian, def.Primary); err != nil {
			return
		}
		if err = binary.Write(&w, binary.BigEndian, def.SortKey); err != nil {
			return
		}
		if err = MarshalDefault(&w, def.Type, def.Default); err != nil {
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
	s.NameIndex[def.Name] = def.Idx
	return
}

func (s *Schema) AppendSortKey(name string, typ types.Type, idx int, isPrimary bool) error {
	def := &ColDef{
		Name:    name,
		Type:    typ,
		SortIdx: int8(idx),
		SortKey: int8(1),
	}
	if isPrimary {
		def.Primary = int8(1)
	}
	return s.AppendColDef(def)
}

func (s *Schema) AppendPKCol(name string, typ types.Type, idx int) error {
	def := &ColDef{
		Name:    name,
		Type:    typ,
		SortIdx: int8(idx),
		SortKey: int8(1),
		Primary: int8(1),
	}
	return s.AppendColDef(def)
}

func (s *Schema) AppendCol(name string, typ types.Type) error {
	def := &ColDef{
		Name:    name,
		Type:    typ,
		SortIdx: -1,
	}
	return s.AppendColDef(def)
}

func (s *Schema) AppendColWithDefault(name string, typ types.Type, val Default) error {
	def := &ColDef{
		Name:    name,
		Type:    typ,
		SortIdx: -1,
		Default: val,
	}
	return s.AppendColDef(def)
}

func (s *Schema) String() string {
	buf, _ := json.Marshal(s)
	return string(buf)
}

func (s *Schema) IsPartOfPK(idx int) bool {
	return s.ColDefs[idx].IsPrimary()
}

func (s *Schema) Attrs() []string {
	attrs := make([]string, 0, len(s.ColDefs)-1)
	for _, def := range s.ColDefs {
		if def.IsHidden() {
			continue
		}
		attrs = append(attrs, def.Name)
	}
	return attrs
}

func (s *Schema) Types() []types.Type {
	ts := make([]types.Type, 0, len(s.ColDefs)-1)
	for _, def := range s.ColDefs {
		if def.IsHidden() {
			continue
		}
		ts = append(ts, def.Type)
	}
	return ts
}

func (s *Schema) AllNullables() []bool {
	nulls := make([]bool, 0, len(s.ColDefs))
	for _, def := range s.ColDefs {
		nulls = append(nulls, def.Nullable())
	}
	return nulls
}

func (s *Schema) AllTypes() []types.Type {
	ts := make([]types.Type, 0, len(s.ColDefs))
	for _, def := range s.ColDefs {
		ts = append(ts, def.Type)
	}
	return ts
}

func (s *Schema) AllNames() []string {
	names := make([]string, 0, len(s.ColDefs))
	for _, def := range s.ColDefs {
		names = append(names, def.Name)
	}
	return names
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
			Name:    HiddenColumnName,
			Comment: HiddenColumnComment,
			Type:    HiddenColumnType,
			Hidden:  int8(1),
		}
		if err = s.AppendColDef(hiddenDef); err != nil {
			return
		}
	}

	sortIdx := make([]int, 0)
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
		if def.IsSortKey() {
			sortIdx = append(sortIdx, idx)
		}
		if def.IsHidden() {
			if s.HiddenKey != nil {
				err = fmt.Errorf("%w: duplicated hidden column \"%s\"", ErrSchemaValidation, def.Name)
				return
			}
			s.HiddenKey = def
		}
	}

	if len(sortIdx) == 1 {
		def := s.ColDefs[sortIdx[0]]
		if def.SortIdx != 0 {
			err = fmt.Errorf("%w: bad sort idx %d, should be 0", ErrSchemaValidation, def.SortIdx)
			return
		}
		s.SortKey = NewSortKey()
		s.SortKey.AddDef(def)
	} else if len(sortIdx) > 1 {
		s.SortKey = NewSortKey()
		for _, idx := range sortIdx {
			def := s.ColDefs[idx]
			if def.Idx != idx {
				err = fmt.Errorf("%w: bad column def", ErrSchemaValidation)
				return
			}
			if ok := s.SortKey.AddDef(def); !ok {
				err = fmt.Errorf("%w: duplicated sort idx specified", ErrSchemaValidation)
				return
			}
		}
		isPrimary := s.SortKey.Defs[0].IsPrimary()
		for i, def := range s.SortKey.Defs {
			if int(def.SortIdx) != i {
				err = fmt.Errorf("%w: duplicated sort idx specified", ErrSchemaValidation)
				return
			}
			if def.IsPrimary() != isPrimary {
				err = fmt.Errorf("%w: duplicated sort idx specified", ErrSchemaValidation)
				return
			}
		}
	}
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

func MockCompoundSchema(colCnt int, pkIdx ...int) *Schema {
	rand.Seed(time.Now().UnixNano())
	schema := NewEmptySchema(fmt.Sprintf("%d", rand.Intn(1000000)))
	prefix := "mock_"
	m := make(map[int]int)
	for i, idx := range pkIdx {
		m[idx] = i
	}
	for i := 0; i < colCnt; i++ {
		if pos, ok := m[i]; ok {
			if err := schema.AppendPKCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.Type_INT32, Size: 4, Width: 32}, pos); err != nil {
				panic(err)
			}
		} else {
			if err := schema.AppendCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.Type_INT32, Size: 4, Width: 32}); err != nil {
				panic(err)
			}
		}
	}
	if err := schema.Finalize(false); err != nil {
		panic(err)
	}
	return schema
}

func MockSchema(colCnt int, pkIdx int) *Schema {
	rand.Seed(time.Now().UnixNano())
	schema := NewEmptySchema(time.Now().String())
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		if pkIdx == i {
			_ = schema.AppendPKCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.Type_INT32, Size: 4, Width: 4}, 0)
		} else {
			_ = schema.AppendCol(fmt.Sprintf("%s%d", prefix, i), types.Type{Oid: types.Type_INT32, Size: 4, Width: 4})
		}
	}
	_ = schema.Finalize(false)
	return schema
}

// MockSchemaAll if char/varchar is needed, colCnt = 14, otherwise colCnt = 12
// pkIdx == -1 means no pk defined
func MockSchemaAll(colCnt int, pkIdx int) *Schema {
	schema := NewEmptySchema(time.Now().String())
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		var typ types.Type
		switch i % 18 {
		case 0:
			typ = types.Type_INT8.ToType()
			typ.Width = 8
		case 1:
			typ = types.Type_INT16.ToType()
			typ.Width = 16
		case 2:
			typ = types.Type_INT32.ToType()
			typ.Width = 32
		case 3:
			typ = types.Type_INT64.ToType()
			typ.Width = 64
		case 4:
			typ = types.Type_UINT8.ToType()
			typ.Width = 8
		case 5:
			typ = types.Type_UINT16.ToType()
			typ.Width = 16
		case 6:
			typ = types.Type_UINT32.ToType()
			typ.Width = 32
		case 7:
			typ = types.Type_UINT64.ToType()
			typ.Width = 64
		case 8:
			typ = types.Type_FLOAT32.ToType()
			typ.Width = 32
		case 9:
			typ = types.Type_FLOAT64.ToType()
			typ.Width = 64
		case 10:
			typ = types.Type_DATE.ToType()
			typ.Width = 32
		case 11:
			typ = types.Type_DATETIME.ToType()
			typ.Width = 64
		case 12:
			typ = types.Type_VARCHAR.ToType()
			typ.Width = 100
		case 13:
			typ = types.Type_CHAR.ToType()
			typ.Width = 100
		case 14:
			typ = types.Type_TIMESTAMP.ToType()
			typ.Width = 64
		case 15:
			typ = types.Type_DECIMAL64.ToType()
			typ.Width = 64
		case 16:
			typ = types.Type_DECIMAL128.ToType()
			typ.Width = 128
		case 17:
			typ = types.Type_BOOL.ToType()
			typ.Width = 8
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

func GetAttrIdx(attrs []string, name string) int {
	for i, attr := range attrs {
		if attr == name {
			return i
		}
	}
	panic("logic error")
}
