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
	"bytes"
	"github.com/gogo/protobuf/proto"
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

// A non-NULL default is backfill-free only with the immutable, validated
// descriptor created by the admitted append transition. Mutable SQL defaults
// alone never authorize interpreting old objects.
func m0BirthColumnNeedsBackfill(state *plan.AppendMissingState, seq uint32, def *plan.Default) bool {
	if !m0ColumnNeedsBackfill(def) {
		return false
	}
	if state == nil || seq < state.BaseWidth || seq-state.BaseWidth >= uint32(len(state.Columns)) {
		return true
	}
	fill := state.Columns[seq-state.BaseWidth]
	return fill.Kind != plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1 ||
		!plan.IsLiteralFalseDefault(&plan.ColDef{Typ: fill.Type, Default: def})
}

func M0AppendLayoutFromSchema(s *Schema) (objectio.M0AppendLayout, error) {
	if s == nil {
		return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "nil schema")
	}
	if err := s.validateAppendMissingColumns(); err != nil {
		return objectio.M0AppendLayout{}, err
	}
	pk := s.GetPrimaryKey()
	if pk == nil {
		return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "missing primary key")
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
		var def *plan.Default
		if len(col.Default) != 0 {
			def = &plan.Default{}
			if err := types.Decode(col.Default, def); err != nil {
				return objectio.M0AppendLayout{}, err
			}
		}
		backfill := m0BirthColumnNeedsBackfill(s.Extra.AppendMissingState, uint32(col.SeqNum), def)
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
	layout, err := M0DerivedAppendLayout(s)
	if err != nil {
		return 0, err
	}
	return layout.Version, nil
}

// M0DerivedAppendLayout reconstructs the append-domain layout from complete
// column identities. The local catalog Version is intentionally not used:
// relation creation can restore all columns without restoring the inherited
// append version, while background merge must still qualify read-only objects.
func M0DerivedAppendLayout(s *Schema) (objectio.M0AppendLayout, error) {
	layout, err := M0AppendLayoutFromSchema(s)
	if err != nil {
		return objectio.M0AppendLayout{}, err
	}
	base := uint32(layout.PKSeqnum) + 1
	if layout.NextColSeqnum <= base {
		return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "schema has no append version")
	}
	layout.Version = layout.NextColSeqnum - base
	if err = objectio.ValidateM0AppendLayout(layout); err != nil {
		return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "%v", err)
	}
	return layout, nil
}

// M0SchemaHasAppendDomainShape identifies a schema that is structurally in
// the restricted append domain. It deliberately does not validate identity or
// semantics: callers use it only to distinguish a failed M0 proof, which must
// stay observable, from an ordinary schema that is outside this domain.
func M0SchemaHasAppendDomainShape(s *Schema) bool {
	if s == nil || s.Extra == nil || len(s.ColDefs) < 3 || !s.HasPK() ||
		s.HasFakePK() || len(s.SortKey.Defs) != 1 {
		return false
	}
	pk := s.GetPrimaryKey()
	if pk == nil || !pk.Hidden || !pk.Primary || !pk.IsCompositeColumn() ||
		!pk.SortKey || pk.ClusterBy {
		return false
	}
	base := uint32(pk.SeqNum) + 1
	if s.Extra.NextColSeqnum <= base {
		return false
	}
	for i, col := range s.ColDefs {
		if col == nil || col.Idx != i {
			return false
		}
		if col.IsPhyAddr() {
			return i == len(s.ColDefs)-1
		}
		if col.SeqNum != uint16(i) {
			return false
		}
	}
	return false
}

func M0AppendLayoutFromPlan(table *plan.TableDef, nextColSeqnum uint32) (objectio.M0AppendLayout, error) {
	if table == nil || len(table.Cols) < 2 || table.Pkey == nil ||
		table.Pkey.CompPkeyCol == nil || (table.Version == 0 && nextColSeqnum == 0) {
		return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "incomplete CN layout")
	}
	if err := plan.ValidateAppendMissingTable(table); err != nil {
		return objectio.M0AppendLayout{}, err
	}
	pk := table.Pkey.CompPkeyCol
	if !pk.Hidden || !pk.Primary || table.ClusterBy != nil {
		return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "CN primary-key anchor")
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
			return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "nil CN column")
		}
		if objectio.IsPhysicalAddr(col.Name) {
			if pos != len(table.Cols)-1 {
				return objectio.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "rowid is not last")
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
			Backfill:      m0BirthColumnNeedsBackfill(table.AppendMissingState, col.Seqnum, col.Default),
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
		return 0, wrapM0Error(ErrM0Layout, "%v", err)
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
		return false, wrapM0Error(ErrM0Layout, "%v", err)
	}
	return absent, nil
}

// M0MemorySuffixKind proves a captured in-memory schema is a complete old
// prefix of the read layout. No object header or latest-schema fallback is used.
func M0MemorySuffixKind(read, write *Schema, seq uint16) (plan.MissingFillKind, error) {
	reject := func() (plan.MissingFillKind, error) { return plan.MissingFillKind_MISSING_FILL_INVALID, ErrM0Layout }
	if read == nil || write == nil || read.Extra.AppendMissingState == nil {
		return reject()
	}
	current, err := M0AppendLayoutFromSchema(read)
	if err != nil {
		return plan.MissingFillKind_MISSING_FILL_INVALID, err
	}
	old, err := M0AppendLayoutFromSchema(write)
	if err != nil {
		return plan.MissingFillKind_MISSING_FILL_INVALID, err
	}
	if err = objectio.ValidateM0AppendLayout(current); err != nil {
		return plan.MissingFillKind_MISSING_FILL_INVALID, err
	}
	if err = objectio.ValidateM0AppendLayout(old); err != nil {
		return plan.MissingFillKind_MISSING_FILL_INVALID, err
	}
	if old.PKSeqnum != current.PKSeqnum || old.NextColSeqnum > current.NextColSeqnum ||
		uint32(seq) < old.NextColSeqnum || uint32(seq) >= current.NextColSeqnum {
		return reject()
	}
	for i := range old.Columns {
		if old.Columns[i] != current.Columns[i] || read.ColDefs[i].Name != write.ColDefs[i].Name ||
			read.ColDefs[i].Type != write.ColDefs[i].Type {
			return reject()
		}
	}
	birth := read.Extra.AppendMissingState
	if previous := write.Extra.AppendMissingState; previous != nil {
		if !bytes.Equal(previous.LayoutId, birth.LayoutId) || len(previous.Columns) > len(birth.Columns) {
			return reject()
		}
		for i, c := range previous.Columns {
			if !proto.Equal(c, birth.Columns[i]) {
				return reject()
			}
		}
	} else {
		for _, c := range birth.Columns {
			if c.Seqnum < old.NextColSeqnum && c.Kind != plan.MissingFillKind_MISSING_FILL_NULL_V1 {
				return reject()
			}
		}
	}
	return birth.Columns[uint32(seq)-birth.BaseWidth].Kind, nil
}
