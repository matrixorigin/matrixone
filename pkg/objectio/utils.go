// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package objectio

import (
	"encoding/binary"
	"io"
	"math/rand"
	"strconv"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

var (
	RowidType types.Type
)

func init() {
	RowidType = types.T_Rowid.ToType()
}

type CreateObjOpt struct {
	Id *types.Objectid
}

func (o *CreateObjOpt) WithId(id *types.Objectid) *CreateObjOpt {
	o.Id = id
	return o
}

type CreateBlockOpt struct {
	Loc *struct {
		Metaloc  Location
		Deltaloc Location
	}

	Id *struct {
		Filen uint16
		Blkn  uint16
	}
}

func (o *CreateBlockOpt) WithMetaloc(s Location) *CreateBlockOpt {
	if o.Loc != nil {
		o.Loc.Metaloc = s
	} else {
		o.Loc = &struct {
			Metaloc  Location
			Deltaloc Location
		}{Metaloc: s}
	}
	return o
}

func (o *CreateBlockOpt) WithDetaloc(s Location) *CreateBlockOpt {
	if o.Loc != nil {
		o.Loc.Deltaloc = s
	} else {
		o.Loc = &struct {
			Metaloc  Location
			Deltaloc Location
		}{Deltaloc: s}
	}
	return o
}

func (o *CreateBlockOpt) WithFileIdx(s uint16) *CreateBlockOpt {
	if o.Id != nil {
		o.Id.Filen = s
	} else {
		o.Id = &struct {
			Filen uint16
			Blkn  uint16
		}{Filen: s}
	}
	return o
}

func (o *CreateBlockOpt) WithBlkIdx(s uint16) *CreateBlockOpt {
	if o.Id != nil {
		o.Id.Blkn = s
	} else {
		o.Id = &struct {
			Filen uint16
			Blkn  uint16
		}{Blkn: s}
	}
	return o
}

func WriteString(str string, w io.Writer) (n int64, err error) {
	buf := []byte(str)
	size := uint32(len(buf))
	if _, err = w.Write(types.EncodeUint32(&size)); err != nil {
		return
	}
	wn, err := w.Write(buf)
	return int64(wn + 4), err
}

func WriteBytes(b []byte, w io.Writer) (n int64, err error) {
	size := uint32(len(b))
	if _, err = w.Write(types.EncodeUint32(&size)); err != nil {
		return
	}
	wn, err := w.Write(b)
	return int64(wn + 4), err
}

func ReadString(r io.Reader) (str string, n int64, err error) {
	strLen := uint32(0)
	if _, err = io.ReadFull(r, types.EncodeUint32(&strLen)); err != nil {
		return
	}
	buf := make([]byte, strLen)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	str = string(buf)
	n = 4 + int64(strLen)
	return
}

func ReadBytes(r io.Reader) (buf []byte, n int64, err error) {
	strLen := uint32(0)
	if _, err = io.ReadFull(r, types.EncodeUint32(&strLen)); err != nil {
		return
	}
	buf = make([]byte, strLen)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	n = 4 + int64(strLen)
	return
}

type Seqnums struct {
	Seqs       []uint16
	MaxSeq     uint16 // do not consider special column like rowid and committs
	MetaColCnt uint16 // include special columns
}

func NewSeqnums(seqs []uint16) *Seqnums {
	s := &Seqnums{
		Seqs: make([]uint16, 0, len(seqs)),
	}
	if len(seqs) == 0 {
		return s
	}

	for _, v := range seqs {
		if v < SEQNUM_UPPER && v > s.MaxSeq {
			s.MaxSeq = v
		}
	}

	maxseq := s.MaxSeq
	for _, v := range seqs {
		if v >= SEQNUM_UPPER {
			s.Seqs = append(s.Seqs, maxseq+1)
			maxseq += 1
		} else {
			s.Seqs = append(s.Seqs, v)
		}
	}
	s.MetaColCnt = maxseq + 1
	return s
}

func (s *Seqnums) InitWithColCnt(colcnt int) {
	for i := 0; i < colcnt; i++ {
		s.Seqs = append(s.Seqs, uint16(i))
	}
	s.MaxSeq = uint16(colcnt) - 1
	s.MetaColCnt = uint16(colcnt)
}

func ConstructRowidColumn(
	id *Blockid, start, length uint32, mp *mpool.MPool,
) (vec *vector.Vector, err error) {
	vec = vector.NewVec(RowidType)
	if err = ConstructRowidColumnTo(vec, id, start, length, mp); err != nil {
		vec = nil
	}
	return
}

func ConstructRowidColumnTo(
	vec *vector.Vector,
	id *Blockid, start, length uint32, mp *mpool.MPool,
) (err error) {
	if err = vec.PreExtend(int(length), mp); err != nil {
		return
	}
	for i := uint32(0); i < length; i++ {
		rid := NewRowid(id, start+i)
		if err = vector.AppendFixed(vec, *rid, false, mp); err != nil {
			break
		}
	}
	if err != nil {
		vec.Free(mp)
	}
	return
}

func ConstructRowidColumnToWithSels(
	vec *vector.Vector,
	id *Blockid,
	sels []int32,
	mp *mpool.MPool,
) (err error) {
	if err = vec.PreExtend(len(sels), mp); err != nil {
		return
	}
	for _, row := range sels {
		rid := NewRowid(id, uint32(row))
		if err = vector.AppendFixed(vec, *rid, false, mp); err != nil {
			break
		}
	}
	if err != nil {
		vec.Free(mp)
	}
	return
}

//=============================================
// add following functions to solve the cycle reference problem.
// because,I add hakeeper client interface to the Process for supporting
// the table function mo_configurations to get info from the hakeeper.
//
// `Hakeeper logservice.CNHAKeeperClient`
//
// I get following cycle reference error.
// error:
//package github.com/matrixorigin/matrixone/pkg/objectio
//	imports github.com/matrixorigin/matrixone/pkg/catalog
//	imports github.com/matrixorigin/matrixone/pkg/hakeeper/operator
//	imports github.com/matrixorigin/matrixone/pkg/hakeeper/checkers
//	imports github.com/matrixorigin/matrixone/pkg/logservice
//	imports github.com/matrixorigin/matrixone/pkg/vm/process
//	imports github.com/matrixorigin/matrixone/pkg/testutil: import cycle not allowed in test
//
// To fix the error. I move some necessary functions here.
//=============================================

func NewBatch(ts []types.Type, random bool, n int, m *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.SetRowCount(n)
	for i := range bat.Vecs {
		bat.Vecs[i] = NewVector(n, ts[i], m, random, nil)
		// XXX do we need to init nulls here?   can we be lazy?
		bat.Vecs[i].GetNulls().InitWithSize(n)
	}
	return bat
}

func NewVector(n int, typ types.Type, m *mpool.MPool, random bool, Values interface{}) *vector.Vector {
	switch typ.Oid {
	case types.T_bool:
		if vs, ok := Values.([]bool); ok {
			return NewBoolVector(n, typ, m, random, vs)
		}
		return NewBoolVector(n, typ, m, random, nil)
	case types.T_bit:
		if vs, ok := Values.([]uint64); ok {
			return NewUInt64Vector(n, typ, m, random, vs)
		}
		return NewUInt64Vector(n, typ, m, random, nil)
	case types.T_int8:
		if vs, ok := Values.([]int8); ok {
			return NewInt8Vector(n, typ, m, random, vs)
		}
		return NewInt8Vector(n, typ, m, random, nil)
	case types.T_int16:
		if vs, ok := Values.([]int16); ok {
			return NewInt16Vector(n, typ, m, random, vs)
		}
		return NewInt16Vector(n, typ, m, random, nil)
	case types.T_int32:
		if vs, ok := Values.([]int32); ok {
			return NewInt32Vector(n, typ, m, random, vs)
		}
		return NewInt32Vector(n, typ, m, random, nil)
	case types.T_int64:
		if vs, ok := Values.([]int64); ok {
			return NewInt64Vector(n, typ, m, random, vs)
		}
		return NewInt64Vector(n, typ, m, random, nil)
	case types.T_uint8:
		if vs, ok := Values.([]uint8); ok {
			return NewUInt8Vector(n, typ, m, random, vs)
		}
		return NewUInt8Vector(n, typ, m, random, nil)
	case types.T_uint16:
		if vs, ok := Values.([]uint16); ok {
			return NewUInt16Vector(n, typ, m, random, vs)
		}
		return NewUInt16Vector(n, typ, m, random, nil)
	case types.T_uint32:
		if vs, ok := Values.([]uint32); ok {
			return NewUInt32Vector(n, typ, m, random, vs)
		}
		return NewUInt32Vector(n, typ, m, random, nil)
	case types.T_uint64:
		if vs, ok := Values.([]uint64); ok {
			return NewUInt64Vector(n, typ, m, random, vs)
		}
		return NewUInt64Vector(n, typ, m, random, nil)
	case types.T_float32:
		if vs, ok := Values.([]float32); ok {
			return NewFloat32Vector(n, typ, m, random, vs)
		}
		return NewFloat32Vector(n, typ, m, random, nil)
	case types.T_float64:
		if vs, ok := Values.([]float64); ok {
			return NewFloat64Vector(n, typ, m, random, vs)
		}
		return NewFloat64Vector(n, typ, m, random, nil)
	case types.T_date:
		if vs, ok := Values.([]string); ok {
			return NewDateVector(n, typ, m, random, vs)
		}
		return NewDateVector(n, typ, m, random, nil)
	case types.T_time:
		if vs, ok := Values.([]string); ok {
			return NewTimeVector(n, typ, m, random, vs)
		}
		return NewTimeVector(n, typ, m, random, nil)
	case types.T_datetime:
		if vs, ok := Values.([]string); ok {
			return NewDatetimeVector(n, typ, m, random, vs)
		}
		return NewDatetimeVector(n, typ, m, random, nil)
	case types.T_timestamp:
		if vs, ok := Values.([]string); ok {
			return NewTimestampVector(n, typ, m, random, vs)
		}
		return NewTimestampVector(n, typ, m, random, nil)
	case types.T_decimal64:
		if vs, ok := Values.([]types.Decimal64); ok {
			return NewDecimal64Vector(n, typ, m, random, vs)
		}
		return NewDecimal64Vector(n, typ, m, random, nil)
	case types.T_decimal128:
		if vs, ok := Values.([]types.Decimal128); ok {
			return NewDecimal128Vector(n, typ, m, random, vs)
		}
		return NewDecimal128Vector(n, typ, m, random, nil)
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		if vs, ok := Values.([]string); ok {
			return NewStringVector(n, typ, m, random, vs)
		}
		return NewStringVector(n, typ, m, random, nil)
	case types.T_array_float32:
		if vs, ok := Values.([][]float32); ok {
			return NewArrayVector[float32](n, typ, m, random, vs)
		}
		return NewArrayVector[float32](n, typ, m, random, nil)
	case types.T_array_float64:
		if vs, ok := Values.([][]float64); ok {
			return NewArrayVector[float64](n, typ, m, random, vs)
		}
		return NewArrayVector[float64](n, typ, m, random, nil)
	case types.T_json:
		if vs, ok := Values.([]string); ok {
			return NewJsonVector(n, typ, m, random, vs)
		}
		return NewJsonVector(n, typ, m, random, nil)
	case types.T_TS:
		if vs, ok := Values.([]types.TS); ok {
			return NewTsVector(n, typ, m, random, vs)
		}
		return NewTsVector(n, typ, m, random, nil)
	case types.T_Rowid:
		if vs, ok := Values.([]types.Rowid); ok {
			return NewRowidVector(n, typ, m, random, vs)
		}
		return NewRowidVector(n, typ, m, random, nil)
	case types.T_Blockid:
		if vs, ok := Values.([]types.Blockid); ok {
			return NewBlockidVector(n, typ, m, random, vs)
		}
		return NewBlockidVector(n, typ, m, random, nil)
	case types.T_enum:
		if vs, ok := Values.([]types.Enum); ok {
			return NewEnumVector(n, typ, m, random, vs)
		}
		return NewEnumVector(n, typ, m, random, nil)
	default:
		panic(moerr.NewInternalErrorNoCtx("unsupport vector's type '%v", typ))
	}
}

func NewTsVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.TS) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var t timestamp.Timestamp

		t.PhysicalTime = int64(i)
		if err := vector.AppendFixed(vec, types.TimestampToTS(t), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewRowidVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.Rowid) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var rowId types.Rowid
		binary.LittleEndian.PutUint64(
			unsafe.Slice(&rowId[types.RowidSize/2], 8),
			uint64(i),
		)
		if err := vector.AppendFixed(vec, rowId, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewBlockidVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.Blockid) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var blockId types.Blockid
		binary.LittleEndian.PutUint64(
			unsafe.Slice(&blockId[types.BlockidSize/2], 8),
			uint64(i),
		)
		if err := vector.AppendFixed(vec, blockId, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewJsonVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for _, v := range vs {
			json, err := types.ParseStringToByteJson(v)
			if err != nil {
				vec.Free(m)
				return nil
			}
			jbytes, err := json.Marshal()
			if err != nil {
				vec.Free(m)
				return nil
			}
			if err := vector.AppendFixed(vec, jbytes, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		json, _ := types.ParseStringToByteJson(`{"a":1}`)
		jbytes, _ := json.Marshal()
		if err := vector.AppendBytes(vec, jbytes, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewBoolVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []bool) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		if err := vector.AppendFixed(vec, bool(i%2 == 0), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int8) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int16) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, int64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint8) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, uint8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint16) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, uint16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, uint32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, uint64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []float32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := float32(i)
		if random {
			v = rand.Float32()
		}
		if err := vector.AppendFixed(vec, float32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []float64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := float64(i)
		if random {
			v = rand.Float64()
		}
		if err := vector.AppendFixed(vec, float64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		d := types.Decimal64(v)
		if err := vector.AppendFixed(vec, d, false, m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal128Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal128) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		d := types.Decimal128{B0_63: uint64(v), B64_127: 0}
		if err := vector.AppendFixed(vec, d, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDateVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseDateCast(vs[i])
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, types.Date(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimeVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseTime(vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, types.Time(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewEnumVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Enum) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}

	for i := 0; i < n; i++ {
		v := uint16(i)
		if random {
			v = uint16(rand.Int())
		}
		if err := vector.AppendFixed(vec, v, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDatetimeVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseDatetime(vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, types.Datetime(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimestampVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseTimestamp(time.Local, vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, types.Timestamp(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewStringVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendBytes(vec, []byte(vs[i]), false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendBytes(vec, []byte(strconv.Itoa(v)), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewArrayVector[T types.RealNumbers](n int, typ types.Type, m *mpool.MPool, random bool, vs [][]T) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendArray[T](vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendArray[T](vec, []T{T(v), T(v + 1)}, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}
