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

package projection

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// selectionState lets projection read a selected input. Expressions run on
// the input's physical rows masked to the selection, so filtered-out rows are
// not evaluated; the projected batch then either keeps the input's selection
// (borrowed, valid until the next Call) or is compacted into compact.
type selectionState struct {
	output   bool
	physical batch.Batch
	compact  *batch.Batch
}

// AcceptsSelection: grouping-set expansion reads compacted rows.
func (projection *Projection) AcceptsSelection(child int) bool {
	return child == 0 && projection.GroupingSetCount == 0
}

// SetSelectionOutput implements vm.SelectionProducer.
func (projection *Projection) SetSelectionOutput(allowed bool) {
	projection.ctr.selection.output = allowed && projection.GroupingSetCount == 0
}

// finishSelected publishes buf, whose vectors hold physical rows, with the
// input's selection, or compacts it when the parent needs flat rows.
func (projection *Projection) finishSelected(input *batch.Batch, mp *mpool.MPool) (*batch.Batch, error) {
	buf := projection.ctr.buf
	buf.SetRowCount(input.PhysicalRowCount())
	buf.SetSelection(input.PhysicalRowCount(), input.Selection())
	if projection.ctr.selection.output {
		return buf, nil
	}
	s := &projection.ctr.selection
	if s.compact == nil {
		s.compact = batch.NewWithSize(len(buf.Vecs))
	}
	for i, vec := range buf.Vecs {
		if s.compact.Vecs[i] == nil || *s.compact.Vecs[i].GetType() != *vec.GetType() {
			if s.compact.Vecs[i] != nil {
				s.compact.Vecs[i].Free(mp)
			}
			s.compact.Vecs[i] = vector.NewVec(*vec.GetType(), mp)
		}
	}
	if err := buf.CompactInto(s.compact, mp); err != nil {
		return nil, err
	}
	buf.ClearSelection()
	return s.compact, nil
}

func (s *selectionState) free(mp *mpool.MPool) {
	if s.compact != nil {
		s.compact.Clean(mp)
		s.compact = nil
	}
}
