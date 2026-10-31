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

package analyze

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
)

const (
	ByteRangeVersion     = 1
	ByteRangeValueBytes  = 64
	byteRangeColumnSlots = 2048
	byteRangeTableSlots  = 16384
)

// ByteRangeSampleCapacity shares one bounded table budget across column passes.
func ByteRangeSampleCapacity(columns int) int {
	if columns <= 0 {
		return 0
	}
	return min(byteRangeColumnSlots, byteRangeTableSlots/columns)
}

func SupportsByteRange(typ types.Type) bool {
	return typ.Oid.IsMySQLString() && typ.Oid != types.T_char && !types.IsUnicodeCollation(typ.Charset)
}

type byteRangeRow struct {
	priority [16]byte
	identity [types.RowidSize]byte
	value    [ByteRangeValueBytes]byte
	length   uint8
	null     bool
}

// ByteRangeSampler samples rows, not distinct values. NULLs and duplicates must
// retain their frequency. Its fixed-size max heap never retains a wide value.
// The caller owns the snapshot, uniform-row admission and publication boundary.
type ByteRangeSampler struct {
	limit    int
	observed uint64
	rows     []byteRangeRow
}

func NewByteRangeSampler(limit int) *ByteRangeSampler {
	limit = max(0, min(limit, byteRangeColumnSlots))
	return &ByteRangeSampler{limit: limit, rows: make([]byteRangeRow, 0, limit)}
}

// ByteRangePriority is independent of both row admission and column values.
func ByteRangePriority(seed [32]byte, identity types.Rowid) [16]byte {
	// Preserve hash128 framing without its interface-induced allocations in
	// this per-row path. All three fields have fixed, known sizes.
	const domain = "analyze/byte-range/v1"
	var input [8 + len(domain) + 8 + 32 + 8 + types.RowidSize]byte
	binary.BigEndian.PutUint64(input[:8], uint64(len(domain)))
	offset := 8 + copy(input[8:], domain)
	binary.BigEndian.PutUint64(input[offset:], uint64(len(seed)))
	offset += 8
	offset += copy(input[offset:], seed[:])
	binary.BigEndian.PutUint64(input[offset:], uint64(len(identity)))
	offset += 8
	copy(input[offset:], identity[:])
	digest := sha256.Sum256(input[:])
	return [16]byte(digest[:16])
}

func byteRangeLess(a, b *byteRangeRow) bool {
	if cmp := bytes.Compare(a.priority[:], b.priority[:]); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(a.identity[:], b.identity[:]) < 0
}

func (s *ByteRangeSampler) Observe(priority [16]byte, identity, value []byte, null bool) error {
	if len(identity) != types.RowidSize || s.observed == math.MaxUint64 {
		return ErrInvalidPopulation
	}
	s.observed++
	if s.limit == 0 {
		return nil
	}
	var row byteRangeRow
	row.priority = priority
	copy(row.identity[:], identity)
	if len(s.rows) == s.limit && !byteRangeLess(&row, &s.rows[0]) {
		return nil
	}
	row.null = null
	if !null {
		row.length = uint8(copy(row.value[:], value))
	}
	if len(s.rows) < s.limit {
		s.rows = append(s.rows, row)
		for child := len(s.rows) - 1; child > 0; {
			parent := (child - 1) / 2
			if !byteRangeLess(&s.rows[parent], &s.rows[child]) {
				break
			}
			s.rows[parent], s.rows[child] = s.rows[child], s.rows[parent]
			child = parent
		}
		return nil
	}
	s.rows[0] = row
	for parent := 0; ; {
		child := 2*parent + 1
		if child >= len(s.rows) {
			break
		}
		if child+1 < len(s.rows) && byteRangeLess(&s.rows[child], &s.rows[child+1]) {
			child++
		}
		if !byteRangeLess(&s.rows[parent], &s.rows[child]) {
			break
		}
		s.rows[parent], s.rows[child] = s.rows[child], s.rows[parent]
		parent = child
	}
	return nil
}

func (s *ByteRangeSampler) Distribution(population uint64) *statsinfo.ByteRangeDistribution {
	if s == nil || population == 0 || s.observed > population || len(s.rows) == 0 {
		return nil
	}
	result := &statsinfo.ByteRangeDistribution{Version: ByteRangeVersion, PopulationRows: population, SampleRows: uint64(len(s.rows)), CompleteSample: s.observed == population && s.observed == uint64(len(s.rows))}
	result.Values = make([][]byte, 0, len(s.rows))
	for i := range s.rows {
		if !s.rows[i].null {
			result.Values = append(result.Values, slices.Clone(s.rows[i].value[:s.rows[i].length]))
		}
	}
	slices.SortFunc(result.Values, bytes.Compare)
	return result
}

// ByteRangeUpper estimates a byte-ordered half-open interval. For <=64-byte
// bounds, truncating a value to 64 bytes preserves membership, including edge
// equality. This does not extend to strict lower or inclusive upper bounds.
// The uncertainty margin models uniform row sampling at the analyzed snapshot;
// it is not a guarantee against stale statistics or adversarial seeded data.
func ByteRangeUpper(d *statsinfo.ByteRangeDistribution, lower, upper []byte) (float64, bool) {
	if d == nil || d.Version != ByteRangeVersion || d.PopulationRows == 0 || d.SampleRows == 0 || d.SampleRows > byteRangeColumnSlots || d.SampleRows > d.PopulationRows || len(d.Values) > int(d.SampleRows) || len(lower) > ByteRangeValueBytes || len(upper) > ByteRangeValueBytes || bytes.Compare(lower, upper) >= 0 || d.CompleteSample && d.SampleRows != d.PopulationRows {
		return 0, false
	}
	for i, v := range d.Values {
		if len(v) > ByteRangeValueBytes || i > 0 && bytes.Compare(d.Values[i-1], v) > 0 {
			return 0, false
		}
	}
	lo, _ := slices.BinarySearchFunc(d.Values, lower, bytes.Compare)
	hi, _ := slices.BinarySearchFunc(d.Values, upper, bytes.Compare)
	fraction := float64(hi-lo) / float64(d.SampleRows)
	if !d.CompleteSample {
		fraction += 2 * math.Sqrt(math.Log(2/0.01)/(2*float64(d.SampleRows)))
	}
	return min(1, fraction), true
}
