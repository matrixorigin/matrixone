// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ValidateAppendMissingExtension checks an immutable history extension before
// either owner mutates its catalog. Caller separately proves the restricted
// source layout, table/transaction eligibility and complete physical objects.
func ValidateAppendMissingExtension(before, after *AppendMissingState, base, version uint32, existing []Type, added *ColDef) error {
	invalid := func() error { return moerr.NewInternalErrorNoCtx("append missing state: invalid history extension") }
	if err := ValidateAppendMissingState(after); err != nil {
		return err
	}
	if after == nil || base > uint32(len(existing)) || version != uint32(len(existing))-base ||
		after.BaseWidth != base || after.PkSeqnum != base-1 || uint32(len(after.Columns)) != version+1 ||
		added == nil || added.Seqnum != uint32(len(existing)) || added.ColId != uint64(len(existing))+1 ||
		added.Hidden || added.Primary || added.ClusterBy || added.Typ.AutoIncr || added.GeneratedCol != nil || added.OnUpdate != nil || added.Comment != "" ||
		!IsLiteralFalseDefault(added) {
		return invalid()
	}
	if err := ValidateAppendMissingState(before); err != nil {
		return err
	}
	if before != nil && (before.BaseWidth != base || len(before.Columns) != int(version) || !bytes.Equal(before.LayoutId, after.LayoutId)) {
		return invalid()
	}
	for i := uint32(0); i < version; i++ {
		fill := after.Columns[i]
		if !BirthFillTypeEqual(fill.Type, existing[base+i]) {
			return invalid()
		}
		if before == nil {
			// The only pre-descriptor history is the existing NULL-only M0
			// domain, whose full layout is separately proved by each owner.
			if fill.Kind != MissingFillKind_MISSING_FILL_NULL_V1 {
				return invalid()
			}
		} else if !proto.Equal(before.Columns[i], fill) {
			return invalid()
		}
	}
	last := after.Columns[len(after.Columns)-1]
	if last.Kind != MissingFillKind_MISSING_FILL_BOOL_FALSE_V1 || !BirthFillTypeEqual(last.Type, added.Typ) {
		return invalid()
	}
	return nil
}
