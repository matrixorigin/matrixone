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

package objectio

import (
	"bytes"
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// NewM0ObjectProof follows a successful full physical-prefix proof. The caller
// has obtained the object from the relation's transaction-visible object set.
// layout_id identifies inherited layout history, not relation ownership.
func NewM0ObjectProof(name string, width, size uint32, relationID uint64, version uint32, birth *plan.AppendMissingState) (M0ObjectProof, error) {
	if name == "" || width > size {
		return M0ObjectProof{}, ErrMissingColumnWithoutProof
	}
	if err := plan.ValidateAppendMissingState(birth); err != nil {
		return M0ObjectProof{}, err
	}
	if birth != nil && (relationID == 0 || version != uint32(len(birth.Columns)) ||
		size != birth.BaseWidth+uint32(len(birth.Columns)) || width < birth.BaseWidth) {
		return M0ObjectProof{}, ErrMissingColumnWithoutProof
	}
	return M0ObjectProof{ObjectName: name, WrittenSize: width, SchemaSize: size,
		relationID: relationID, version: version, birth: plan.CloneAppendMissingState(birth)}, nil
}

// MatchesReadSchema prevents reuse just because two clones reference the same
// object. Schema version and immutable layout history must also match.
func (p M0ObjectProof) MatchesReadSchema(table *plan.TableDef) bool {
	if table == nil {
		return p.birth == nil && p.relationID == 0
	}
	if p.birth == nil && table.AppendMissingState == nil {
		// Retain the legacy unscoped test/NULL-only proof contract.
		return p.relationID == 0 || (p.relationID == table.TblId && p.version == table.Version)
	}
	state := table.AppendMissingState
	return state != nil && p.birth != nil && p.relationID == table.TblId && p.version == table.Version &&
		bytes.Equal(p.birth.LayoutId, state.LayoutId) && proto.Equal(p.birth, state)
}

// newMissingColumnVector owns any allocated payload. The caller must Free it
// after serialization, including error paths. Special columns keep NULL fill.
func newMissingColumnVector(ctx context.Context, name string, seq uint16, typ types.Type, length int, mp *mpool.MPool) (*vector.Vector, error) {
	if seq >= SEQNUM_UPPER {
		return vector.NewConstNull(typ, length, mp), nil
	}
	kind, ok := MissingColumnFill(ctx, name, seq)
	if !ok {
		return nil, ErrMissingColumnWithoutProof
	}
	switch kind {
	case plan.MissingFillKind_MISSING_FILL_NULL_V1:
		return vector.NewConstNull(typ, length, mp), nil
	case plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1:
		if typ.Oid != types.T_bool || typ.Width != 0 || typ.Scale != 0 {
			return nil, ErrMissingColumnWithoutProof
		}
		return vector.NewConstFixed(typ, false, length, mp)
	default:
		return nil, ErrMissingColumnWithoutProof
	}
}
