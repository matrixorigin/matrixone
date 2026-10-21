//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func m0ColumnNeedsBackfill(def *plan.Default) bool {
	if def == nil || def.Expr == nil {
		return false
	}
	lit := def.Expr.GetLit()
	return lit == nil || !lit.GetIsnull()
}

func m0EncodedColumnNeedsBackfill(encoded []byte) (bool, error) {
	if len(encoded) == 0 {
		return false, nil
	}
	var def plan.Default
	if err := types.Decode(encoded, &def); err != nil {
		return false, err
	}
	return m0ColumnNeedsBackfill(&def), nil
}

func M0AppendLayoutFromSchema(s *Schema) (objectio.M0AppendLayout, error) {
	if s == nil {
		return objectio.M0AppendLayout{}, fmt.Errorf("%w: nil schema", ErrM0Layout)
	}
	pk := s.GetPrimaryKey()
	if pk == nil {
		return objectio.M0AppendLayout{}, fmt.Errorf("%w: missing primary key", ErrM0Layout)
	}
	layout := objectio.M0AppendLayout{
		Version:       s.Version,
		NextColSeqnum: s.Extra.NextColSeqnum,
		PKSeqnum:      pk.SeqNum,
		CompositePK:   pk.IsCompositeColumn(),
		HiddenPK:      pk.Hidden,
		SingleSortKey: s.SortKey != nil && len(s.SortKey.Defs) == 1,
	}
	for _, col := range s.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		backfill, err := m0EncodedColumnNeedsBackfill(col.Default)
		if err != nil {
			return objectio.M0AppendLayout{}, err
		}
		layout.Columns = append(layout.Columns, objectio.M0AppendColumn{
			Seqnum:        col.SeqNum,
			Type:          col.Type.Oid,
			Nullable:      col.NullAbility,
			Hidden:        col.Hidden,
			Primary:       col.Primary,
			SortKey:       col.SortKey,
			ClusterBy:     col.ClusterBy,
			AutoIncrement: col.AutoIncrement,
			Generated:     len(col.GeneratedCol) != 0,
			OnUpdate:      len(col.OnUpdate) != 0,
			Backfill:      backfill,
		})
	}
	return layout, nil
}

// M0DerivedAppendVersion recovers the schema version represented by a complete
// append-only schema. It is for the CN/TN writers that materialize the full
// current batch after an in-memory or catalog version marker has gone stale.
func M0DerivedAppendVersion(s *Schema) (uint32, error) {
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return 0, err
	}
	base := uint32(layout.PKSeqnum) + 1
	if layout.NextColSeqnum <= base {
		return 0, fmt.Errorf("%w: schema has no append version", ErrM0Layout)
	}
	layout.Version = layout.NextColSeqnum - base
	if err = objectio.ValidateM0AppendLayout(layout); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrM0Layout, err)
	}
	return layout.Version, nil
}

func M0AppendLayoutFromPlan(table *plan.TableDef, nextColSeqnum uint32) (objectio.M0AppendLayout, error) {
	if table == nil || len(table.Cols) < 2 || table.Pkey == nil ||
		table.Pkey.CompPkeyCol == nil || (table.Version == 0 && nextColSeqnum == 0) {
		return objectio.M0AppendLayout{}, fmt.Errorf("%w: incomplete CN layout", ErrM0Layout)
	}
	pk := table.Pkey.CompPkeyCol
	if !pk.Hidden || !pk.Primary || table.ClusterBy != nil {
		return objectio.M0AppendLayout{}, fmt.Errorf("%w: CN primary-key anchor", ErrM0Layout)
	}
	layout := objectio.M0AppendLayout{
		Version:       table.Version,
		NextColSeqnum: nextColSeqnum,
		PKSeqnum:      uint16(pk.Seqnum),
		CompositePK:   table.Pkey.PkeyColName != "" && pk.Name == table.Pkey.PkeyColName,
		HiddenPK:      pk.Hidden,
		SingleSortKey: true,
	}
	for pos, col := range table.Cols {
		if col == nil {
			return objectio.M0AppendLayout{}, fmt.Errorf("%w: nil CN column", ErrM0Layout)
		}
		if objectio.IsPhysicalAddr(col.Name) {
			if pos != len(table.Cols)-1 {
				return objectio.M0AppendLayout{}, fmt.Errorf("%w: rowid is not last", ErrM0Layout)
			}
			continue
		}
		layout.Columns = append(layout.Columns, objectio.M0AppendColumn{
			Seqnum:        uint16(col.Seqnum),
			Type:          types.T(col.Typ.Id),
			Nullable:      col.Default == nil || col.Default.NullAbility,
			Hidden:        col.Hidden,
			Primary:       col.Primary,
			SortKey:       col.Primary,
			ClusterBy:     col.ClusterBy,
			AutoIncrement: col.Typ.AutoIncr,
			Generated:     col.GeneratedCol != nil,
			OnUpdate:      col.OnUpdate != nil,
			Backfill:      m0ColumnNeedsBackfill(col.Default),
		})
	}
	return layout, nil
}

func M0PlanLooksLikeAppendDomain(table *plan.TableDef) bool {
	if table == nil || table.IsTemporary || table.Pkey == nil || table.Pkey.CompPkeyCol == nil ||
		!table.Pkey.CompPkeyCol.Hidden || !table.Pkey.CompPkeyCol.Primary ||
		table.Pkey.PkeyColName != table.Pkey.CompPkeyCol.Name || table.ClusterBy != nil ||
		len(table.Cols) < 2 || !objectio.IsPhysicalAddr(table.Cols[len(table.Cols)-1].Name) {
		return false
	}
	base := uint32(table.Pkey.CompPkeyCol.Seqnum) + 1
	next := uint32(len(table.Cols) - 1)
	if table.Version != next-base {
		return false
	}
	for i, col := range table.Cols {
		if col == nil || uint32(col.Seqnum) != uint32(i) {
			return false
		}
	}
	return true
}

// M0AppendBase validates one deliberately narrow transition domain:
//   - version-zero CREATE/COPY puts the hidden composite PK last;
//   - each independent autocommit ADD appends exactly one nullable column and
//     advances the version once;
//   - clone preserves the full layout AND version; other INPLACE DDL is refused;
//   - COPY starts a new version-zero domain and rewrites all data objects.
//
// The mo_m0 SQL/CN/TN hooks enforce these transitions. This predicate remains
// scoped to the restricted prototype and is not authorization for arbitrary
// legacy tables.
func M0AppendBase(s *Schema) (uint32, error) {
	if err := validateM0Schema(s); err != nil {
		return 0, err
	}
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return 0, err
	}
	if err = objectio.ValidateM0AppendLayout(layout); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrM0Layout, err)
	}
	return uint32(layout.PKSeqnum) + 1, nil
}

// M0ProveAbsentColumn returns true only for a nullable suffix that did not exist
// at the object's version in the above domain. It validates every block's
// complete expected prefix, rather than accepting MaxSeqnum alone. Metadata
// must come from the standard object reader. An object must already be a member
// of the relation being read; this does not invent cross-relation ownership.
// There is no historical-schema lookup, NULL vector allocation or pruning hook.
func M0ProveAbsentColumn(s *Schema, h objectio.Header, meta objectio.ObjectDataMeta, seq uint16) (bool, error) {
	if _, err := M0AppendBase(s); err != nil {
		return false, err
	}
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return false, err
	}
	absent, err := objectio.ProveM0AbsentColumn(layout, h, meta, seq)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrM0Layout, err)
	}
	return absent, nil
}
