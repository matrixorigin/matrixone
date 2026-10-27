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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ValidateM0WritePrefix binds a physical write to its captured version. A full
// current batch uses that version; an old complete batch may keep its genuine
// old version. Neither a hole nor a truncated new batch can borrow an old header.
func ValidateM0WritePrefix(state *plan.AppendMissingState, readVersion, capturedVersion uint32, seqnums []uint16) error {
	if state == nil {
		return nil
	}
	if err := plan.ValidateAppendMissingState(state); err != nil {
		return err
	}
	if readVersion != uint32(len(state.Columns)) {
		return wrapM0Error(ErrMissingColumnWithoutProof, "write schema version differs from birth history")
	}

	// Writer batches may place the hidden compound key last. seqnums bind
	// vectors to physical column identities; qualify the complete set rather
	// than inventing an ordering constraint on the batch projection.
	seen := make([]bool, state.BaseWidth+uint32(len(state.Columns)))
	width := uint32(0)
	var specials uint8
	for _, seq := range seqnums {
		if seq >= SEQNUM_UPPER {
			var bit uint8
			switch seq {
			case SEQNUM_ROWID:
				bit = 1
			case SEQNUM_COMMITTS:
				bit = 2
			case SEQNUM_ABORT:
				bit = 4
			default:
				return ErrMissingColumnWithoutProof
			}
			if specials&bit != 0 {
				return ErrMissingColumnWithoutProof
			}
			specials |= bit
			continue
		}
		if uint32(seq) >= uint32(len(seen)) || seen[seq] {
			return wrapM0Error(ErrMissingColumnWithoutProof, "write column identity %d is outside the schema or repeated", seq)
		}
		seen[seq] = true
		width++
	}
	for seq := uint32(0); seq < width; seq++ {
		if !seen[seq] {
			return wrapM0Error(ErrMissingColumnWithoutProof, "write prefix has a hole at %d", seq)
		}
	}
	if width < state.BaseWidth || width > state.BaseWidth+uint32(len(state.Columns)) ||
		capturedVersion != width-state.BaseWidth || capturedVersion > readVersion {
		return wrapM0Error(ErrMissingColumnWithoutProof, "write captured-version=%d width=%d base=%d read-version=%d", capturedVersion, width, state.BaseWidth, readVersion)
	}
	return nil
}
