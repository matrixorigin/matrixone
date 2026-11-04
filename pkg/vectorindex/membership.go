// Copyright 2026 Matrix Origin
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

package vectorindex

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// BuildMembership builds the exact primary-key filter of a membership runtime filter
// payload (a serialized key vector) for an index search of algo over sourceTableDef.
// The search tests it in C or C++, so it is built in C memory (docfilter.NewForC:
// cbitmap, CRoaring or sorted64) and charged to the CN memory admission. empty
// reports an exact empty key set, an empty payload included; then filter is nil. The
// caller frees filter.
func BuildMembership(service, algo string, sourceTableDef *plan.TableDef, payload []byte) (
	filter docfilter.MembershipFilter, count uint64, empty bool, err error) {
	if len(payload) == 0 {
		return nil, 0, true, nil
	}
	var keys vector.Vector
	if err = keys.UnmarshalBinary(payload); err != nil {
		return nil, 0, false, err
	}
	defer keys.Free(nil)
	if keys.Length() == 0 {
		return nil, 0, true, nil
	}
	pk := sourceTableDef.GetPkey()
	if pk == nil {
		return nil, 0, false, moerr.NewInvalidInputNoCtxf("%s membership search source has no primary key", algo)
	}
	pos, ok := sourceTableDef.Name2ColIndex[pk.PkeyColName]
	if !ok || pos < 0 || int(pos) >= len(sourceTableDef.Cols) || keys.HasNull() ||
		!types.MembershipKeyTypeCompatible(keys.GetType().Oid, types.T(sourceTableDef.Cols[pos].Typ.Id)) ||
		!docfilter.SupportsBitset(*keys.GetType()) {
		return nil, 0, false, moerr.NewInvalidInputNoCtxf("%s membership key type does not match the source primary key", algo)
	}
	filter, err = docfilter.NewForC(&keys, docfilter.AdmissionForService(service))
	if err != nil {
		return nil, 0, false, err
	}
	if !filter.Exact() {
		filter.Free()
		return nil, 0, false, moerr.NewInvalidStateNoCtxf("%s membership filter is not exact", algo)
	}
	return filter, uint64(keys.Length()), false, nil
}

// FilterMaskAdmissionBytes is the smallest filter-mask estimate AdmitFilterMask
// reserves; a smaller estimate is not admitted.
const FilterMaskAdmissionBytes = int64(1 << 20)

// AdmitFilterMask reserves with admission the host memory a filtered cuVS search over
// rows builds on the CPU: its per-row filter mask and the copy of the delete bitmap
// ANDed into it, rows/8 bytes each. A search without a filter, or a reservation below
// FilterMaskAdmissionBytes, reserves nothing. The returned release is never nil.
func AdmitFilterMask(admission docfilter.MemoryAdmission, rt RuntimeConfig, rows uint64) (func(), error) {
	noop := func() {}
	if rt.FilterJSON == "" && rt.Membership == nil {
		return noop, nil
	}
	mask := (rows + 7) / 8
	if mask > uint64(math.MaxInt64)/2 {
		return noop, nil
	}
	bytes := 2 * mask
	if int64(bytes) < FilterMaskAdmissionBytes {
		return noop, nil
	}
	release, err := docfilter.AcquireMemory(admission, int64(bytes))
	if err != nil {
		return nil, err
	}
	if release == nil {
		return noop, nil
	}
	return release, nil
}
