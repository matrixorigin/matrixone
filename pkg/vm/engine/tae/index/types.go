// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var (
	ErrNotFound  = moerr.NewInternalErrorNoCtx("tae index: key not found")
	ErrDuplicate = moerr.NewInternalErrorNoCtx("tae index: key duplicate")
	ErrPrefix    = moerr.NewInternalErrorNoCtx("tae index: prefix filter error")
)

const (
	BF = iota
	PBF
	HBF
)

const (
	PrefixFnID_Object uint8 = iota
	PrefixFnID_Block
)

var (
	ObjectPrefixFn = PrefixFn{Id: PrefixFnID_Object, Fn: func(b []byte) []byte { return b[:types.ObjectBytesSize] }}
	BlockPrefixFn  = PrefixFn{Id: PrefixFnID_Block, Fn: func(b []byte) []byte { return b[:types.BlockidSize] }}
)

type PrefixFn struct {
	Id uint8
	Fn func([]byte) []byte
}

// RowSelection describes selected rows in [MinRow, MaxRow). Holes contains
// absolute row offsets inside that range. MaxRow <= MinRow means empty.
type RowSelection struct {
	MinRow uint32
	MaxRow uint32
	Holes  *nulls.Bitmap
}

func (s RowSelection) IsEmpty() bool { return s.MaxRow <= s.MinRow }

func (s RowSelection) Contains(row uint32) bool {
	return row >= s.MinRow && row < s.MaxRow &&
		(s.Holes == nil || !s.Holes.Contains(uint64(row)))
}

// AddRange adds an ascending, non-overlapping selected range.
func (s *RowSelection) AddRange(start, end uint32) {
	if start >= end {
		return
	}
	if s.IsEmpty() {
		s.MinRow, s.MaxRow = start, end
		return
	}
	if start > s.MaxRow {
		if s.Holes == nil {
			s.Holes = &nulls.Bitmap{}
		}
		s.Holes.AddRange(uint64(s.MaxRow), uint64(start))
	}
	if end > s.MaxRow {
		s.MaxRow = end
	}
}

func (s *RowSelection) MakePrefix() {
	if s.IsEmpty() || s.MinRow == 0 {
		return
	}
	if s.Holes == nil {
		s.Holes = &nulls.Bitmap{}
	}
	s.Holes.AddRange(0, uint64(s.MinRow))
	s.MinRow = 0
}

func (s RowSelection) ForEachRange(fn func(start, end uint32) bool) {
	for row := s.MinRow; row < s.MaxRow; {
		for row < s.MaxRow && !s.Contains(row) {
			row++
		}
		start := row
		for row < s.MaxRow && s.Contains(row) {
			row++
		}
		if start < row && !fn(start, row) {
			return
		}
	}
}

type SecondaryIndex interface {
	Insert(key []byte, offset uint32) (err error)
	BatchInsert(keys *vector.Vector, offset, length int, startRow uint32) (err error)
	Delete(key any) (old uint32, err error)
	DeleteAt(key []byte, row uint32) error
	Search(key []byte) ([]uint32, error)
	String() string
	Size() int
}

type StaticFilter interface {
	MayContainsKey(key []byte) (bool, error)
	MayContainsAnyKeys(keys containers.Vector) (bool, *nulls.Bitmap, error)
	MayContainsAny(keys *vector.Vector, lowerBound int, upperBound int) bool

	PrefixMayContainsKey(key []byte, prefixFnId uint8, level uint8) (bool, error)
	PrefixMayContainsAny(
		keys *vector.Vector, lowerBound int, upperBound int, prefixFnId uint8, level uint8,
	) bool

	Marshal() ([]byte, error)
	// MarshalWithBuffer marshals the filter into an existing, reusable buffer.
	// The caller must copy buf.Bytes() before the next Reset/reuse of buf.
	MarshalWithBuffer(buf *bytes.Buffer) error
	Unmarshal(buf []byte) error
	String() string
	PrefixFnId(level uint8) uint8
	GetType() uint8
	MaxLevel() uint8
}
