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
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var (
	RowidType types.Type
)

func init() {
	RowidType = types.T_Rowid.ToType()
}

type CreateSegOpt struct {
	Id *types.Uuid
}

func (o *CreateSegOpt) WithId(id *types.Uuid) *CreateSegOpt {
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
	if _, err = r.Read(types.EncodeUint32(&strLen)); err != nil {
		return
	}
	buf := make([]byte, strLen)
	if _, err = r.Read(buf); err != nil {
		return
	}
	str = string(buf)
	n = 4 + int64(strLen)
	return
}

func ReadBytes(r io.Reader) (buf []byte, n int64, err error) {
	strLen := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&strLen)); err != nil {
		return
	}
	buf = make([]byte, strLen)
	if _, err = r.Read(buf); err != nil {
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
	vec.PreExtend(int(length), mp)
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
