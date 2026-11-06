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

package disttae

import (
	"bytes"
	"context"
	"crypto/rand"
	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

func init() {
	activeM0CreateEvolution = createM0Evolution
	activeM0CheckReadEvolution = checkM0ReadEvolution
	activeM0CheckEvolution = func(ctx context.Context, tbl *txnTable) error { return validateM0Evolution(tbl, tbl.GetTableDef(ctx)) }
	activeM0AdvanceEvolution = advanceM0Evolution
}

func evolutionOwner(tbl *txnTable) m0read.EvolutionBinding {
	return m0read.EvolutionBinding{Account: tbl.accountId, Database: tbl.db.databaseId, Relation: tbl.tableId}
}

func evolutionColumns(def *plan.TableDef, root uint32) ([]*api.M0EvolutionColumn, error) {
	var cols []*api.M0EvolutionColumn
	for _, c := range def.Cols {
		if c == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: missing evolution schema column")
		}
		if objectio.IsPhysicalAddr(c.Name) {
			continue
		}
		semantics, err := m0read.PlanEvolutionColumnSemantic(def, c)
		if err != nil {
			return nil, err
		}
		g := uint32(0)
		if uint32(len(cols)) >= root {
			g = uint32(len(cols)) - root + 1
		}
		cols = append(cols, &api.M0EvolutionColumn{ColumnId: c.ColId, Seqnum: c.Seqnum, IntroducedLayout: g, Semantics: semantics})
	}
	return cols, nil
}

func validateM0Evolution(tbl *txnTable, def *plan.TableDef) error {
	if tbl == nil || tbl.db == nil || tbl.extraInfo == nil || def == nil {
		return moerr.NewInternalErrorNoCtx("M0: invalid relation evolution contract")
	}
	c := tbl.extraInfo.M0Evolution
	if err := m0read.ValidateEvolution(c, evolutionOwner(tbl)); err != nil {
		return err
	}
	if _, ok := m0AppendBaseFromPlan(def); !ok {
		return moerr.NewInternalErrorNoCtx("M0: authoritative schema outside evolution contract")
	}
	cols, err := evolutionColumns(def, c.RootColumns)
	if err != nil {
		return err
	}
	if !bytes.Equal(c.SchemaDigest, m0read.EvolutionDigest(cols)) {
		return moerr.NewInternalErrorNoCtx("M0: evolution contract disagrees with authoritative schema")
	}
	return nil
}

func createM0Evolution(ctx context.Context, tbl *txnTable, altering bool) error {
	if source := m0read.CloneEvolution(ctx); source != nil && !altering {
		copy := api.CloneExtra(&api.SchemaExtra{M0Evolution: source}).M0Evolution
		copy.AccountId, copy.DatabaseId, copy.RelationId = tbl.accountId, tbl.db.databaseId, tbl.tableId
		tbl.extraInfo.M0Evolution = copy
		tbl.tableDef = nil
		return validateM0Evolution(tbl, tbl.GetTableDef(ctx))
	}
	if tbl.extraInfo.M0Evolution != nil {
		return validateM0Evolution(tbl, tbl.GetTableDef(ctx))
	}
	if m0read.IsCloneEvolution(ctx) {
		return nil
	}
	if altering {
		return nil
	}
	def := tbl.GetTableDef(ctx)
	if def.Version != 0 {
		return nil
	}
	if _, ok := m0AppendBaseFromPlan(def); !ok {
		return nil
	}
	root := uint32(len(def.Cols) - 1)
	cols, err := evolutionColumns(def, root)
	if err != nil {
		return nil
	} // unsupported ordinary CREATE remains ordinary
	// Negative fixtures can model a pre-contract CREATE by omitting authority.
	// This never grants authority or changes any object bytes.
	if _, _, omit := fault.TriggerFaultWithContext(ctx, "fj/m0/evolution/omit-create"); omit {
		return nil
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: id, AccountId: tbl.accountId, DatabaseId: tbl.db.databaseId, RelationId: tbl.tableId,
		RootColumns: root, Columns: cols, SchemaDigest: m0read.EvolutionDigest(cols)}
	if err := m0read.ValidateEvolution(c, evolutionOwner(tbl)); err != nil {
		return err
	}
	tbl.extraInfo.M0Evolution = c
	tbl.tableDef = nil
	return nil
}

func advanceM0Evolution(ctx context.Context, tbl *txnTable) error {
	old := tbl.extraInfo.M0Evolution
	if err := m0read.ValidateEvolution(old, evolutionOwner(tbl)); err != nil {
		return err
	}
	cols, err := evolutionColumns(tbl.GetTableDef(ctx), old.RootColumns)
	if err != nil {
		return err
	}
	if len(cols) != len(old.Columns)+1 {
		return moerr.NewInternalErrorNoCtx("M0: evolution must append exactly one column")
	}
	for i, c := range old.Columns {
		if c.ColumnId != cols[i].ColumnId || c.Seqnum != cols[i].Seqnum || !bytes.Equal(c.Semantics, cols[i].Semantics) {
			return moerr.NewInternalErrorNoCtx("M0: existing evolution column changed")
		}
	}
	next := api.CloneExtra(tbl.extraInfo)
	next.M0Evolution.LayoutGeneration++
	next.M0Evolution.Columns = cols
	next.M0Evolution.SchemaDigest = m0read.EvolutionDigest(cols)
	if err := m0read.ValidateEvolution(next.M0Evolution, evolutionOwner(tbl)); err != nil {
		return err
	}
	tbl.extraInfo = next
	tbl.tableDef = nil
	return nil
}

// A cached immutable plan and transaction-owned metadata may have separate
// owners. They must describe the same authority before selecting any blocks.
func checkM0ReadEvolution(ctx context.Context, tbl *txnTable) error {
	def := tbl.GetTableDef(ctx)
	planContract, err := m0read.PlanEvolution(def)
	if err != nil {
		return err
	}
	var current *api.M0EvolutionContract
	if tbl.extraInfo != nil {
		current = tbl.extraInfo.M0Evolution
	}
	if current == nil && planContract == nil {
		return nil
	}
	if !proto.Equal(current, planContract) {
		return moerr.NewInternalErrorNoCtx("M0: evolution read owner and plan disagree")
	}
	return validateM0Evolution(tbl, def)
}
