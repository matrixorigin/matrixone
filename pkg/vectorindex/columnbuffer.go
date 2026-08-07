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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ColumnBuffer is a batch of primary keys in a TYPED, box-free columnar form, used by
// the fulltext2 no-LIMIT streaming path (RuntimeConfig.Emit). A pk column has ONE type,
// so every key in a batch is uniformly fixed-width or varlena and Type alone
// disambiguates Data:
//   - fixed-width type (int/uint/temporal/decimal): Data is N contiguous width-byte
//     little-endian values (no length prefix; width is implied by Type).
//   - varlena type (varchar/blob/json/uuid): Data is N [u32 len][content] entries.
//
// Neither form boxes into any, and Data is a copy (not a view into the segment mmap),
// so an in-flight batch survives segment eviction.
type ColumnBuffer struct {
	Type types.T
	Data []byte // fixed: N×width bytes; varlena: [u32 len][content] entries
	N    int    // element count
	// Nulls is the per-element SQL-NULL flag, used only by NULLABLE columns (fulltext2
	// INCLUDE cols). It stays nil for pk columns (a pk is never NULL) — a nil Nulls means
	// "all non-null", so the pk path is unchanged. When non-nil it is kept parallel to N
	// (len(Nulls) == N): a NULL element still carries a well-formed placeholder in Data (a
	// zero-filled fixed value / a [u32 0] varlena entry) so the Data cursor stays aligned.
	Nulls []bool
}

// Reset clears a ColumnBuffer for reuse without dropping its backing buffer. The
// streaming producer/consumer recycle these through fulltext2's pool (GetColumnBuffer /
// PutColumnBuffer); the pooling POLICY lives with its user, not in this shared struct.
func (k *ColumnBuffer) Reset() {
	k.Data = k.Data[:0]
	k.N = 0
	if k.Nulls != nil {
		k.Nulls = k.Nulls[:0]
	}
}
