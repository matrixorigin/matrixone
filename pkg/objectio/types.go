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

package objectio

import (
	"bytes"
	"context"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type WriteType int8

const (
	WriteTS WriteType = iota
)

type ZoneMap = index.ZM
type StaticFilter = index.StaticFilter

var NewZM = index.NewZM
var BuildZM = index.BuildZM

type ColumnMetaFetcher interface {
	MustGetColumn(seqnum uint16) ColumnMeta
}

type ReadFilterSearchFuncType func(containers.Vectors) []int64

type readFilterSearchKind uint8

const (
	readFilterSearchExact readFilterSearchKind = iota
	readFilterSearchPrefix
	readFilterSearchLess
	readFilterSearchGreater
	readFilterSearchBetween
	readFilterSearchPrefixBetween
)

type readFilterSearchTerm struct {
	kind   readFilterSearchKind
	values [][]byte
	lb     []byte
	ub     []byte
	closed bool
	hint   uint8
}

// ReadFilterSearch is an immutable search description for a single varlen
// column. It lets ObjectIO execute supported PK predicates without exposing
// its borrowed cache-backed Vector to a callback.
type ReadFilterSearch struct {
	oid   types.T
	terms []readFilterSearchTerm
}

// NewReadFilterSearch creates an exact byte-membership search. It is used for
// EQ/IN predicates and for exact tombstone PK checks.
func NewReadFilterSearch(oid types.T, values [][]byte) *ReadFilterSearch {
	switch oid {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text, types.T_array_float32,
		types.T_array_float64, types.T_datalink:
	default:
		return nil
	}
	return newReadFilterSearch(oid, readFilterSearchTerm{
		kind:   readFilterSearchExact,
		values: values,
	})
}

// The remaining constructors are deliberately limited to varchar. MatrixOne's
// hidden compound primary key is varchar; keeping this boundary avoids changing
// predicate support for unrelated logical types.
func NewReadFilterPrefixSearch(oid types.T, values [][]byte) *ReadFilterSearch {
	if oid != types.T_varchar {
		return nil
	}
	return newReadFilterSearch(oid, readFilterSearchTerm{
		kind:   readFilterSearchPrefix,
		values: values,
	})
}

func NewReadFilterLessSearch(oid types.T, bound []byte, closed bool) *ReadFilterSearch {
	if oid != types.T_varchar {
		return nil
	}
	return newReadFilterSearch(oid, readFilterSearchTerm{
		kind:   readFilterSearchLess,
		ub:     bound,
		closed: closed,
	})
}

func NewReadFilterGreaterSearch(oid types.T, bound []byte, closed bool) *ReadFilterSearch {
	if oid != types.T_varchar {
		return nil
	}
	return newReadFilterSearch(oid, readFilterSearchTerm{
		kind:   readFilterSearchGreater,
		lb:     bound,
		closed: closed,
	})
}

func NewReadFilterBetweenSearch(oid types.T, lb, ub []byte, hint uint8) *ReadFilterSearch {
	if oid != types.T_varchar || hint > 3 {
		return nil
	}
	return newReadFilterSearch(oid, readFilterSearchTerm{
		kind: readFilterSearchBetween,
		lb:   lb,
		ub:   ub,
		hint: hint,
	})
}

func NewReadFilterPrefixBetweenSearch(oid types.T, lb, ub []byte, hint uint8) *ReadFilterSearch {
	if oid != types.T_varchar || hint > 3 {
		return nil
	}
	return newReadFilterSearch(oid, readFilterSearchTerm{
		kind: readFilterSearchPrefixBetween,
		lb:   lb,
		ub:   ub,
		hint: hint,
	})
}

func newReadFilterSearch(oid types.T, term readFilterSearchTerm) *ReadFilterSearch {
	term.lb = bytes.Clone(term.lb)
	term.ub = bytes.Clone(term.ub)
	if term.values != nil {
		copied := make([][]byte, len(term.values))
		for i := range term.values {
			copied[i] = bytes.Clone(term.values[i])
		}
		slices.SortFunc(copied, bytes.Compare)
		term.values = copied
	}
	return &ReadFilterSearch{oid: oid, terms: []readFilterSearchTerm{term}}
}

// CombineReadFilterSearch combines disjunct terms. All inputs must target the
// same physical OID; nil denotes an unsupported predicate and fails closed to
// the legacy search path at construction time.
func CombineReadFilterSearch(searches ...*ReadFilterSearch) *ReadFilterSearch {
	if len(searches) == 0 || searches[0] == nil {
		return nil
	}
	combined := &ReadFilterSearch{oid: searches[0].oid}
	for _, search := range searches {
		if search == nil || search.oid != combined.oid {
			return nil
		}
		combined.terms = append(combined.terms, search.terms...)
	}
	return combined
}

type BlockReadFilter struct {
	HasFakePK          bool
	Valid              bool
	SortedSearchFunc   ReadFilterSearchFuncType
	UnSortedSearchFunc ReadFilterSearchFuncType
	CachedSearch       *ReadFilterSearch
	Cleanup            func() // Cleanup function to release resources (e.g., reusableTempVec)
}

func (f BlockReadFilter) DecideSearchFunc(isSortedBlk bool) ReadFilterSearchFuncType {
	if (f.HasFakePK || !isSortedBlk) && f.UnSortedSearchFunc != nil {
		return f.UnSortedSearchFunc
	}

	if isSortedBlk && f.SortedSearchFunc != nil {
		return f.SortedSearchFunc
	}

	return nil
}

type Float64Heap []float64

func (h Float64Heap) Len() int           { return len(h) }
func (h Float64Heap) Less(i, j int) bool { return h[i] > h[j] }
func (h Float64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Float64Heap) Push(x any) {
	*h = append(*h, x.(float64))
}

func (h *Float64Heap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type IndexReaderTopOp struct {
	Typ          types.T
	MetricType   metric.MetricType
	ColPos       int32
	NumVec       []byte
	Limit        uint64
	OrderedLimit bool
	Desc         bool

	LowerBoundType plan.BoundType
	UpperBoundType plan.BoundType
	LowerBound     float64
	UpperBound     float64

	DistHeap Float64Heap
}

type WriteOptions struct {
	Type WriteType
	Val  any
}

type ReadBlockOptions struct {
	Id       uint16
	DataType uint16
	Idxes    map[uint16]bool
}

// Writer is to virtualize batches into multiple blocks
// and write them into filefservice at one time
type Writer interface {
	// Write writes one batch to the Buffer at a time,
	// one batch corresponds to a virtual block,
	// and returns the handle of the block.
	Write(batch *batch.Batch) (BlockObject, error)

	// Write metadata for every column of all blocks
	WriteObjectMeta(ctx context.Context, totalRow uint32, metas []ColumnMeta)

	// WriteEnd is to write multiple batches written to
	// the buffer to the fileservice at one time
	WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error)
}

// Reader is to read data from fileservice
type Reader interface {
	// Read is to read columns data of a block from fileservice at one time
	// extent is location of the block meta
	// idxs is the column serial number of the data to be read
	Read(ctx context.Context,
		extent *Extent, idxs []uint16,
		id uint32,
		m *mpool.MPool,
		readFunc CacheConstructorFactory) (*fileservice.IOVector, error)

	ReadAll(
		ctx context.Context,
		extent *Extent,
		idxs []uint16,
		m *mpool.MPool,
		readFunc CacheConstructorFactory,
	) (*fileservice.IOVector, error)

	ReadBlocks(ctx context.Context,
		extent *Extent,
		ids map[uint32]*ReadBlockOptions,
		m *mpool.MPool,
		readFunc CacheConstructorFactory) (*fileservice.IOVector, error)

	// ReadMeta is the meta that reads a block
	// extent is location of the block meta
	ReadMeta(ctx context.Context, extent *Extent, m *mpool.MPool) (ObjectDataMeta, error)

	// ReadAllMeta is read the meta of all blocks in an object
	ReadAllMeta(ctx context.Context, m *mpool.MPool) (ObjectDataMeta, error)

	GetObject() *Object
}
