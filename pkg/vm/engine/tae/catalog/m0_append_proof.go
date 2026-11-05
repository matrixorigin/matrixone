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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
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

func M0AppendLayoutFromSchema(s *Schema) (m0read.M0AppendLayout, error) {
	if s == nil {
		return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "nil schema")
	}
	pk := s.GetPrimaryKey()
	if pk == nil {
		return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "missing primary key")
	}
	layout := m0read.M0AppendLayout{
		Version:       s.Version,
		NextColSeqnum: s.Extra.NextColSeqnum,
		PKSeqnum:      pk.SeqNum,
		CompositePK:   pk.IsCompositeColumn(),
		HiddenPK:      pk.Hidden,
		SingleSortKey: s.SortKey != nil && len(s.SortKey.Defs) == 1,
	}
	if c := s.Extra.M0Evolution; c != nil {
		if err := m0read.ValidateEvolution(c, m0read.EvolutionBinding{Account: s.AcInfo.TenantID, Database: c.DatabaseId, Relation: c.RelationId}); err != nil {
			return m0read.M0AppendLayout{}, err
		}
		layout.Version = c.LayoutGeneration
	}
	for _, col := range s.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		backfill, err := m0EncodedColumnNeedsBackfill(col.Default)
		if err != nil {
			return m0read.M0AppendLayout{}, err
		}
		layout.Columns = append(layout.Columns, m0read.M0AppendColumn{
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

func M0AppendLayoutFromPlan(table *plan.TableDef, nextColSeqnum uint32) (m0read.M0AppendLayout, error) {
	if table == nil || len(table.Cols) < 2 || table.Pkey == nil ||
		table.Pkey.CompPkeyCol == nil || (table.Version == 0 && nextColSeqnum == 0) {
		return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "incomplete CN layout")
	}
	pk := table.Pkey.CompPkeyCol
	if !pk.Hidden || !pk.Primary || table.ClusterBy != nil {
		return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "CN primary-key anchor")
	}
	layout := m0read.M0AppendLayout{
		Version:       table.Version,
		NextColSeqnum: nextColSeqnum,
		PKSeqnum:      uint16(pk.Seqnum),
		CompositePK:   table.Pkey.PkeyColName != "" && pk.Name == table.Pkey.PkeyColName,
		HiddenPK:      pk.Hidden,
		SingleSortKey: true,
	}
	if c, err := m0read.PlanEvolution(table); err != nil {
		return m0read.M0AppendLayout{}, err
	} else if c != nil {
		if err := m0read.ValidateEvolution(c, m0read.EvolutionBinding{Account: c.AccountId, Database: table.DbId, Relation: table.TblId}); err != nil {
			return m0read.M0AppendLayout{}, err
		}
		layout.Version = c.LayoutGeneration
	}
	for pos, col := range table.Cols {
		if col == nil {
			return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "nil CN column")
		}
		if objectio.IsPhysicalAddr(col.Name) {
			if pos != len(table.Cols)-1 {
				return m0read.M0AppendLayout{}, wrapM0Error(ErrM0Layout, "rowid is not last")
			}
			continue
		}
		layout.Columns = append(layout.Columns, m0read.M0AppendColumn{
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
	generation := table.Version
	if c, err := m0read.PlanEvolution(table); err != nil {
		return false
	} else if c != nil {
		generation = c.LayoutGeneration
	}
	if generation != next-base {
		return false
	}
	for i, col := range table.Cols {
		if col == nil || !objectio.IsPhysicalAddr(col.Name) && uint32(col.Seqnum) != uint32(i) {
			return false
		}
	}
	return true
}
