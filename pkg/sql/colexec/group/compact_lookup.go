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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const compactCharDomain = 257

// This is a derived index, never the owner of grouping identity. Its entries
// name committed groups in the current resident hash table only.
type compactGroupLookup struct {
	data     []byte
	ids      []uint64
	values   []uint64
	codes    []uint32
	inserted [hashmap.UnitLimit]uint8
	eligible bool
}

func compactGroupExpressions(exprs []*plan.Expr) bool {
	if len(exprs) == 0 || len(exprs) > 2 {
		return false
	}
	for _, expr := range exprs {
		if expr == nil || types.T(expr.Typ.Id) != types.T_char || expr.Typ.Width != 1 || expr.Typ.Charset > 255 || types.IsUnicodeCollation(uint8(expr.Typ.Charset)) {
			return false
		}
	}
	return true
}

func compactGroupColumns(vecs []*vector.Vector, start, count int) bool {
	if len(vecs) == 0 || len(vecs) > 2 || start < 0 || count <= 0 || count > hashmap.UnitLimit {
		return false
	}
	for _, vec := range vecs {
		if vec == nil {
			return false
		}
		typ := vec.GetType()
		if typ.Oid != types.T_char || typ.Width != 1 || types.IsUnicodeCollation(typ.Charset) ||
			vec.IsConst() || vec.HasNull() || vec.HasGrouping() || vec.HasStringSourceMetadata() ||
			start > vec.Length() || count > vec.Length()-start {
			return false
		}
	}
	return true
}

func (ctr *container) previewCompactGroups(vecs []*vector.Vector, start, count int) (groupInsertPreview, bool, error) {
	hr := &ctr.hr
	if hr.compact != nil {
		hr.compact.eligible = false
	}
	if hr.compactDisabled || ctr.groupKeyStringSourceMetadata || !compactGroupColumns(vecs, start, count) {
		return groupInsertPreview{}, false, nil
	}
	if hr.compact == nil {
		hr.compactRows += count
		if hr.compactRows < 2*hashmap.UnitLimit {
			return groupInsertPreview{}, false, nil
		}
		slots := compactCharDomain
		if len(vecs) == 2 {
			slots *= compactCharDomain
		}
		size := (slots+hashmap.UnitLimit)*8 + hashmap.UnitLimit*4
		var data []byte
		var err error
		if ctr.allocationAccount == nil {
			data, err = ctr.mp.Alloc(size, false)
		} else {
			data, err = ctr.mp.AllocAccounted(size, ctr.allocationAccount, mpool.AllocationOwnerGroup, GroupAllocationSiteCompactLookup)
		}
		if err != nil {
			if mpool.IsRetryableAllocationCapacity(err) {
				hr.compactDisabled = true
				return groupInsertPreview{}, false, nil
			}
			return groupInsertPreview{}, false, err
		}
		clear(data)
		hr.compact = &compactGroupLookup{
			data:   data,
			ids:    types.DecodeSlice[uint64](data[:slots*8]),
			values: types.DecodeSlice[uint64](data[slots*8 : (slots+hashmap.UnitLimit)*8]),
			codes:  types.DecodeSlice[uint32](data[(slots+hashmap.UnitLimit)*8:]),
		}
	}
	lookup := hr.compact
	if (len(lookup.ids) == compactCharDomain) != (len(vecs) == 1) {
		return groupInsertPreview{}, false, nil
	}
	clear(lookup.codes[:count])
	for col, vec := range vecs {
		vals, _ := vector.MustVarlenaRawData(vec)
		for i := 0; i < count; i++ {
			value := &vals[start+i]
			// CHAR(1) normally has an inline payload. Other representations retain
			// the generic encoder's validation and canonicalization.
			if !value.IsSmall() {
				return groupInsertPreview{}, false, nil
			}
			raw := value.ByteSlice()
			for len(raw) > 0 && raw[len(raw)-1] == ' ' {
				raw = raw[:len(raw)-1]
			}
			if len(raw) > 1 {
				return groupInsertPreview{}, false, nil
			}
			var code uint32
			if len(raw) != 0 {
				code = uint32(raw[0]) + 1
			}
			if col == 0 {
				lookup.codes[i] = code
			} else {
				lookup.codes[i] = lookup.codes[i]*compactCharDomain + code
			}
		}
	}
	lookup.eligible = true
	allHit := true
	for i, code := range lookup.codes[:count] {
		id := lookup.ids[code]
		lookup.values[i] = id
		allHit = allHit && id != 0
	}
	if !allHit {
		return groupInsertPreview{}, false, nil
	}
	return groupInsertPreview{values: lookup.values[:count], inserted: lookup.inserted[:count], existingOnly: true}, true, nil
}

func (hr *ResHashRelated) publishCompactGroups(values []uint64) {
	if hr.compact == nil || !hr.compact.eligible {
		return
	}
	for i, code := range hr.compact.codes[:len(values)] {
		hr.compact.ids[code] = values[i]
	}
}

func (hr *ResHashRelated) freeCompactGroups() {
	if hr.compact != nil {
		hr.mp.Free(hr.compact.data)
		hr.compact = nil
	}
	hr.compactRows = 0
	hr.compactDisabled = false
}
