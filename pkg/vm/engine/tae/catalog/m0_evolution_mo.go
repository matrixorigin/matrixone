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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// Runs under the schema lock on the transaction's private MVCC payload.
func advanceM0SchemaEvolution(entry *TableEntry, schema *Schema, req *api.AlterTableReq) error {
	if req.Kind != api.AlterKind_AddColumn {
		return nil
	}
	old := schema.Extra.M0Evolution
	owner := m0read.EvolutionBinding{Account: schema.AcInfo.TenantID, Database: entry.GetDB().ID, Relation: entry.ID}
	if err := m0read.ValidateEvolution(old, owner); err != nil {
		return err
	}
	if len(schema.ColDefs)-1 != len(old.Columns)+1 {
		return moerr.NewInternalErrorNoCtx("M0: TN evolution width changed unexpectedly")
	}
	cols := make([]*api.M0EvolutionColumn, 0, len(old.Columns)+1)
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		pos := len(cols)
		id := uint64(col.SeqNum) + 1 // identity assigned by the restricted ADD protocol
		if pos < len(old.Columns) {
			id = old.Columns[pos].ColumnId
		}
		semantics, err := EvolutionSchemaColumnSemantic(schema, col, id)
		if err != nil {
			return err
		}
		generation := uint32(0)
		if uint32(pos) >= old.RootColumns {
			generation = uint32(pos) - old.RootColumns + 1
		}
		if pos < len(old.Columns) && !bytes.Equal(semantics, old.Columns[pos].Semantics) {
			return moerr.NewInternalErrorNoCtxf("M0: TN authoritative semantics differ at seqnum %d", col.SeqNum)
		}
		cols = append(cols, &api.M0EvolutionColumn{ColumnId: id, Seqnum: uint32(col.SeqNum), IntroducedLayout: generation, Semantics: semantics})
	}
	next := api.CloneExtra(schema.Extra)
	next.M0Evolution.LayoutGeneration++
	next.M0Evolution.Columns = cols
	next.M0Evolution.SchemaDigest = m0read.EvolutionDigest(cols)
	if err := m0read.ValidateEvolution(next.M0Evolution, owner); err != nil {
		return err
	}
	schema.Extra = next
	return nil
}
