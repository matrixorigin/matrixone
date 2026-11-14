// Copyright 2026 Matrix Origin
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

package vector

import (
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// DictionarySelectionScope lends read-only logical headers to synchronous,
// non-retaining consumers. Sources and rows must remain live and immutable
// until End. These views cannot be published, retained, mutated or freed by
// their consumer. Public retained dictionary constructors remain independent.
// The execution lane owns this scope and its reusable metadata buffers.
type DictionarySelectionScope struct {
	columns []scopedDictionaryColumn
	storage Vector
	rows    RowSelection
}

type scopedDictionaryColumn struct {
	view    *Vector
	base    DictionaryBase
	backing dictionaryVector
}

// Bind declines dictionary sources, leaving the caller's owned fallback
// available. It never freezes a source or copies the row map or value payload.
// On failure all borrowed references are retired; metadata stays scope-owned.
func (s *DictionarySelectionScope) Bind(sources, destinations []*Vector, columns []int, rows []int64, domain int, mp *mpool.MPool, account *AllocationAccountSelection) (admitted bool, retErr error) {
	s.End()
	clear(destinations)
	if len(sources) != len(destinations) || domain < 0 || len(rows) > math.MaxInt/8 {
		return false, moerr.NewInvalidInputNoCtx("invalid scoped dictionary domain")
	}
	for _, col := range columns {
		if col < 0 || col >= len(sources) || sources[col] == nil || !sources[col].CoversLogicalRows(0, domain) {
			return false, moerr.NewInvalidInputNoCtx("invalid scoped dictionary column")
		}
		if sources[col].IsDictionary() {
			return false, nil
		}
	}
	for _, row := range rows {
		if row < 0 || row >= int64(domain) {
			return false, moerr.NewInvalidInputNoCtx("scoped dictionary row out of bounds")
		}
	}
	if len(s.columns) != len(sources) {
		s.Free(mp)
		s.columns = make([]scopedDictionaryColumn, len(sources))
	}
	// The selection header aliases caller-owned indices only within this scope.
	s.storage = Vector{typ: types.T_int64.ToType(), data: types.EncodeSlice(rows), length: len(rows)}
	s.rows = RowSelection{storage: &s.storage, domain: domain, monotonic: slices.IsSorted(rows)}
	defer func() {
		if retErr != nil {
			clear(destinations)
			s.End()
		}
	}()
	for _, col := range columns {
		c := &s.columns[col]
		source := sources[col]
		if source.typ.TypeSize() <= 0 || len(rows) > math.MaxInt/source.typ.TypeSize() {
			return false, moerr.NewInvalidInputNoCtx("scoped dictionary size overflow")
		}
		if c.view != nil && !AllocationAccountSelectionsEqual(c.view.allocationAccount, account) {
			c.view.Free(mp)
			c.view = nil
		}
		if c.view == nil {
			var err error
			c.view, err = NewOffHeapVecWithTypeAndAllocation(source.typ, account)
			if err != nil {
				return false, err
			}
		} else {
			c.view.Reset(source.typ)
		}
		if err := copyDictionaryMetadata(c.view, source, rows, mp); err != nil {
			return false, err
		}
		area := 0
		if source.typ.IsVarlen() && !source.IsConstNull() && len(source.area) > 0 {
			for i, row := range rows {
				if c.view.IsNull(uint64(i)) {
					continue
				}
				size := len(source.GetBytesAt(int(row)))
				if size > types.VarlenaInlineSize {
					if size > math.MaxInt-area {
						return false, moerr.NewInvalidInputNoCtx("scoped dictionary size overflow")
					}
					area += size
				}
			}
		}
		if area > math.MaxInt-len(rows)*source.typ.TypeSize() {
			return false, moerr.NewInvalidInputNoCtx("scoped dictionary size overflow")
		}
		c.base.vec = source
		c.backing.base, c.backing.rows = &c.base, &s.rows
		// A negative refcount denotes a lane-local non-owning backing.
		// Retain already rejects nonpositive counts; no public handle is reused.
		c.backing.logicalArea = area
		c.backing.refs.Store(-1)
		c.view.class, c.view.dataLease = DICTIONARY, &c.backing
		c.view.sorted = source.sorted && s.rows.monotonic
		destinations[col] = c.view
	}
	return true, nil
}

// End retires every borrowed pointer before row scratch or source buffers can
// be reused. Detach the non-owning backing before resetting/freeing metadata.
func (s *DictionarySelectionScope) End() {
	for i := range s.columns {
		c := &s.columns[i]
		if c.view != nil {
			c.view.dataLease = nil
			c.view.class = FLAT
			c.view.length = 0
		}
		c.base.vec = nil
		c.backing.base, c.backing.rows = nil, nil
	}
	s.storage.data = nil
	s.storage.length = 0
	s.rows = RowSelection{}
}

func (s *DictionarySelectionScope) Free(mp *mpool.MPool) {
	s.End()
	for i := range s.columns {
		if s.columns[i].view != nil {
			s.columns[i].view.Free(mp)
		}
	}
	*s = DictionarySelectionScope{}
}
