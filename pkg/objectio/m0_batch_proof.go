// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// M0BatchProof is separate from an object/header proof. Its caller owns the
// relation-visible logtail batch and supplies TN's captured version and width.
// It is bounded by one batch and must not be reused across reader schemas.
type M0BatchProof struct {
	written    uint32
	relationID uint64
	version    uint32
	birth      *plan.AppendMissingState
}

func NewM0BatchProof(table *plan.TableDef, attrs []string, vecs []*vector.Vector, capturedVersion, next uint32, offset int) (M0BatchProof, error) {
	if table == nil || table.TblId == 0 || table.AppendMissingState == nil {
		return M0BatchProof{}, ErrMissingColumnWithoutProof
	}
	if err := plan.ValidateAppendMissingTable(table); err != nil {
		return M0BatchProof{}, err
	}
	state := table.AppendMissingState
	if next < state.BaseWidth || next > state.BaseWidth+uint32(len(state.Columns)) ||
		capturedVersion != next-state.BaseWidth || offset < 0 || len(attrs) != len(vecs) ||
		uint64(next)+uint64(offset) != uint64(len(vecs)) {
		return M0BatchProof{}, ErrMissingColumnWithoutProof
	}
	for seq := uint32(0); seq < next; seq++ {
		pos := offset + int(seq)
		col := table.Cols[seq]
		if attrs[pos] != col.Name || vecs[pos] == nil {
			return M0BatchProof{}, wrapM0Error(ErrMissingColumnWithoutProof, "memory prefix seqnum=%d attr=%q expected=%q missing-vector=%t", seq, attrs[pos], col.Name, vecs[pos] == nil)
		}
		expected := vector.ProtoTypeToType(col.Typ)
		if *vecs[pos].GetType() != expected {
			return M0BatchProof{}, wrapM0Error(ErrMissingColumnWithoutProof, "memory prefix seqnum=%d type=%#v expected=%#v", seq, *vecs[pos].GetType(), expected)
		}
	}
	return M0BatchProof{written: next, relationID: table.TblId, version: table.Version, birth: plan.CloneAppendMissingState(state)}, nil
}

func (p M0BatchProof) MatchesReadSchema(table *plan.TableDef) bool {
	return (M0ObjectProof{relationID: p.relationID, version: p.version, birth: p.birth}).MatchesReadSchema(table)
}

func (p M0BatchProof) AppendFill(dst *vector.Vector, seq uint16, mp *mpool.MPool) error {
	if p.birth == nil || uint32(seq) < p.written || uint32(seq) < p.birth.BaseWidth ||
		uint32(seq) >= p.birth.BaseWidth+uint32(len(p.birth.Columns)) {
		return ErrMissingColumnWithoutProof
	}
	fill := p.birth.Columns[uint32(seq)-p.birth.BaseWidth]
	if *dst.GetType() != vector.ProtoTypeToType(fill.Type) {
		return ErrMissingColumnWithoutProof
	}
	switch fill.Kind {
	case plan.MissingFillKind_MISSING_FILL_NULL_V1:
		return vector.AppendNull(dst, mp)
	case plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1:
		if dst.GetType().Oid != types.T_bool {
			return ErrMissingColumnWithoutProof
		}
		return vector.AppendFixed(dst, false, false, mp)
	default:
		return ErrMissingColumnWithoutProof
	}
}
