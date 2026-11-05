// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package m0read

import (
	"encoding/binary"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ColumnSemantic is the common CN/TN projection. Its caller supplies identity
// from the authoritative schema; a matching encoding is not object admission.
type ColumnSemantic struct {
	ID                                                    uint64
	Seqnum                                                uint32
	Name                                                  string
	Type                                                  plan.Type
	Nullable, Hidden, Primary, Unique, ClusterBy, SortKey bool
	Default                                               *plan.Default
	OnUpdate                                              *plan.OnUpdate
	Generated                                             *plan.GeneratedCol
	KeyColumns                                            []uint64
}

// EncodeColumnSemantic defines format 1: little-endian uint64 scalars, then
// length-prefixed byte strings. Planner source tables/coercibility/binding and
// display-only comments are excluded. Unsupported expressions fail explicitly.
func EncodeColumnSemantic(c ColumnSemantic) ([]byte, error) {
	if c.ID == 0 || c.Name == "" || c.OnUpdate != nil || c.Generated != nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: unsupported canonical column semantics")
	}
	var literal []byte
	defaultKind := uint64(0)
	if c.Default != nil && c.Default.Expr == nil {
		origin := strings.TrimSpace(c.Default.OriginString)
		if origin != "" && !(c.Nullable && strings.EqualFold(origin, "null")) {
			return nil, moerr.NewInternalErrorNoCtx("M0: default source has no authoritative expression")
		}
	}
	if c.Default != nil && c.Default.Expr != nil {
		lit := c.Default.Expr.GetLit()
		if lit == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: unsupported default expression in evolution root")
		}
		if !lit.Isnull {
			var err error
			literal, err = lit.Marshal()
			if err != nil {
				return nil, err
			}
			defaultKind = 1
		} else if !c.Nullable {
			return nil, moerr.NewInternalErrorNoCtx("M0: NULL default on nonnullable evolution column")
		}
	}
	if c.Default != nil && c.Default.NullAbility != c.Nullable {
		return nil, moerr.NewInternalErrorNoCtx("M0: contradictory evolution nullability")
	}
	result := make([]byte, 0, 192)
	number := func(n uint64) { result = binary.LittleEndian.AppendUint64(result, n) }
	blob := func(b []byte) { number(uint64(len(b))); result = append(result, b...) }
	number(1)
	number(c.ID)
	number(uint64(c.Seqnum))
	blob([]byte(strings.ToLower(c.Name)))
	for _, n := range []uint64{uint64(c.Type.Id), uint64(int64(c.Type.Width)), uint64(int64(c.Type.Scale)), uint64(c.Type.Charset), uint64(c.Type.CollationVersion), defaultKind} {
		number(n)
	}
	for _, flag := range []bool{c.Nullable, c.Hidden, c.Primary, c.Unique, c.ClusterBy, c.SortKey, c.Type.AutoIncr, c.Type.PadSpace} {
		if flag {
			number(1)
		} else {
			number(0)
		}
	}
	blob([]byte(c.Type.Enumvalues))
	blob(literal)
	number(uint64(len(c.KeyColumns)))
	for _, id := range c.KeyColumns {
		if id == 0 {
			return nil, moerr.NewInternalErrorNoCtx("M0: invalid primary key identity")
		}
		number(id)
	}
	return result, nil
}

// PlanColumnSemantic projects only stored semantics, not the whole protobuf.
func PlanColumnSemantic(c *plan.ColDef, sortKey bool) (ColumnSemantic, error) {
	if c == nil {
		return ColumnSemantic{}, moerr.NewInternalErrorNoCtx("M0: nil authoritative column")
	}
	nullable := !c.NotNull && !c.Typ.NotNullable
	if c.Default != nil {
		if c.Default.NullAbility && !nullable {
			return ColumnSemantic{}, moerr.NewInternalErrorNoCtx("M0: conflicting authoritative nullability")
		}
		nullable = c.Default.NullAbility
	}
	return ColumnSemantic{ID: c.ColId, Seqnum: c.Seqnum, Name: c.Name, Type: c.Typ, Nullable: nullable,
		Hidden: c.Hidden, Primary: c.Primary, Unique: c.Unique, ClusterBy: c.ClusterBy, SortKey: sortKey,
		Default: c.Default, OnUpdate: c.OnUpdate, Generated: c.GeneratedCol}, nil
}

// PlanEvolutionColumnSemantic includes the ordered constituent identities of
// the hidden composite key. Its scalar physical type alone is insufficient.
func PlanEvolutionColumnSemantic(table *plan.TableDef, col *plan.ColDef) ([]byte, error) {
	s, err := PlanColumnSemantic(col, col != nil && (col.Primary || col.ClusterBy))
	if err != nil {
		return nil, err
	}
	if col.Hidden && col.Primary {
		if table == nil || table.Pkey == nil || table.Pkey.PkeyColName != col.Name || len(table.Pkey.Names) < 2 {
			return nil, moerr.NewInternalErrorNoCtx("M0: composite key semantics unavailable")
		}
		seen := make(map[uint64]struct{}, len(table.Pkey.Names))
		for _, name := range table.Pkey.Names {
			var id uint64
			for _, candidate := range table.Cols {
				if candidate != nil && strings.EqualFold(candidate.Name, name) && !candidate.Hidden {
					id = candidate.ColId
					break
				}
			}
			if id == 0 {
				return nil, moerr.NewInternalErrorNoCtx("M0: primary key column identity unavailable")
			}
			if _, ok := seen[id]; ok {
				return nil, moerr.NewInternalErrorNoCtx("M0: duplicate primary key identity")
			}
			seen[id] = struct{}{}
			s.KeyColumns = append(s.KeyColumns, id)
		}
	}
	return EncodeColumnSemantic(s)
}
