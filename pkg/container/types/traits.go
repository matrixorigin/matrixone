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

package types

import (
	"fmt"
	"sort"
)

// Per-type facts live in one table, indexed by T, instead of in a switch per
// fact. The rows are in traits_table.go: adding a T_ constant means adding a
// row there, and TestTraitsCoverEveryOid fails until it is added. See
// docs/design/20261126-type-vft.md.

// traitFlags are the type families the T.IsXxx predicates report.
type traitFlags uint16

const (
	flagSignedInt         traitFlags = 1 << iota // IsSignedInt: int8..int64 (not int128)
	flagUnsignedInt                              // IsUnsignedInt: uint8..uint64 (not uint128)
	flagFloat                                    // IsFloat: float32, float64 and the low-precision floats
	flagLowPrecisionFloat                        // IsLowPrecisionFloat: bf16, float16, float8, float4
	flagDecimal                                  // IsDecimal
	flagDateRelate                               // IsDateRelate: date, time, datetime, timestamp, year
	flagMySQLString                              // IsMySQLString: char, varchar, binary, varbinary, blob, text
	flagArrayRelate                              // IsArrayRelate: fixed-element vector types
	flagBlockScaledArray                         // IsBlockScaledArray: vecf8, vecf4
	flagOrdered                                  // IsOrdered
)

// size is a size in bytes that a type may not have. The zero value is "no
// size": a method asked for it panics, as for a type id with no row, so a row
// that forgets a size fails loudly instead of reporting 0.
type size struct {
	n  int32
	ok bool
}

func sz(n int32) size { return size{n: n, ok: true} }

// defaultType is what T.TryToType returns. The zero value means TryToType
// reports false.
type defaultType struct {
	size  int32
	width int32
	ok    bool
}

func def(size, width int32) defaultType { return defaultType{size: size, width: width, ok: true} }

// typeTraits is one row of the table.
type typeTraits struct {
	flags traitFlags
	// defined marks a row for a T_ constant; a T value with no row reads the
	// zero row.
	defined bool

	typeLen  size // T.TypeLen
	fixedLen size // T.FixedLength: negative for variable-length types
	deflt    defaultType

	name    string // T.String; "" reports "unexpected type: <n>"
	oidName string // T.OidString; "" reports "unknown_type"
}

// traitRow is a typeTraits with the type id it belongs to, as written in
// traits_table.go.
type traitRow struct {
	oid T
	typeTraits
}

// traits holds a row for every T value; T is uint8, so indexing is never out
// of range. Built by a package-level initializer, it is filled before any
// init function or other package runs.
var traits, allOids = buildTraits(traitRows)

func buildTraits(rows []traitRow) (tbl [256]typeTraits, oids []T) {
	for _, r := range rows {
		if tbl[r.oid].defined {
			panic(fmt.Sprintf("types: two traits rows for type id %d", r.oid))
		}
		tbl[r.oid] = r.typeTraits
		tbl[r.oid].defined = true
		oids = append(oids, r.oid)
	}
	sort.Slice(oids, func(i, j int) bool { return oids[i] < oids[j] })
	return
}

// AllOids returns every type id that has a row in the traits table, in
// ascending order: the T_ constants. The caller owns the returned slice.
//
// A table keyed by type id elsewhere can test its completeness against it.
func AllOids() []T {
	return append([]T(nil), allOids...)
}

func (t T) is(f traitFlags) bool { return traits[t].flags&f != 0 }
