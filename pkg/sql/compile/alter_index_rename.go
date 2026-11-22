// Copyright 2021 Matrix Origin
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

package compile

import (
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (c *Compile) renameIndexes(db engine.Database, tableName string, qry *plan.AlterTable) error {
	ctx := c.proc.Ctx
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.shouldBlockCCPRReadOnly(qry.TableDef) {
		return moerr.NewCCPRReadOnly(ctx)
	}
	txn := c.proc.GetTxnOperator().Txn()
	if !txn.IsPessimistic() || !txn.IsRCIsolation() {
		return moerr.NewNotSupported(ctx, "RENAME INDEX requires a pessimistic read-committed transaction")
	}
	// The INPLACE locks already cover table identity, concurrent DDL and FK
	// publication. A wildcard publication does not take a source database lock.
	if err := c.checkRenameIndexPublication(qry.TableDef.DbId); err != nil {
		return err
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return err
	}
	if rel.GetTableID(ctx) != qry.TableDef.TblId || rel.GetTableDef(ctx).GetVersion() != qry.TableDef.Version {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	tableDef := rel.GetTableDef(ctx)
	renames, err := plan2.ResolveIndexRenames(ctx, tableDef, qry.Actions)
	if err != nil || len(renames) == 0 {
		return err
	}

	constraints, err := GetConstraintDef(ctx, rel)
	if err != nil {
		return err
	}
	changed := make([]*plan.IndexDef, 0, len(renames))
	for _, constraint := range constraints.Cts {
		if def, ok := constraint.(*engine.IndexDef); ok {
			for _, index := range def.Indexes {
				if _, ok := renames[index.IndexName]; ok {
					changed = append(changed, index)
				}
			}
		}
	}
	if len(changed) != len(renames) {
		return moerr.NewInternalError(ctx, "inconsistent RENAME INDEX constraint definition")
	}
	if err := c.checkRenameIndexForeignKeys(qry.Database, tableName, tableDef, changed); err != nil {
		return err
	}
	if err := c.renameIndexCatalogRows(rel.GetDBID(ctx), rel.GetTableID(ctx), changed, renames); err != nil {
		return err
	}
	for _, index := range changed {
		index.IndexName = renames[index.IndexName]
	}
	return rel.UpdateConstraint(ctx, constraints)
}

func (c *Compile) checkRenameIndexPublication(databaseID uint64) error {
	ctx := c.proc.Ctx
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	sysCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	db, err := c.e.Database(sysCtx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	pubs, err := db.Relation(sysCtx, catalog.MO_PUBS, nil)
	if err != nil {
		return err
	}
	// Account-wide publications have no database key to pin. Lock just
	// this publisher's existing (account_id, pub_name) primary-key range.
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeInt32(int32(accountID))
	packer.EncodeStringType([]byte{})
	first := packer.Bytes()
	packer.Reset()
	packer.EncodeInt32(int32(accountID))
	packer.EncodeStringTypeMax()
	last := packer.Bytes()
	if err := lockop.LockVarlenaPKRange(sysCtx, c.e, c.proc, pubs.GetTableID(sysCtx), first, last, lock.LockMode_Shared); err != nil {
		return err
	}
	// The range lock can be granted without a conflicting holder. In that
	// case its result need not include the latest commit on a publication row.
	barrier, ok := getLogtailReadBarrier(c.e)
	if !ok {
		return moerr.NewNotSupported(ctx, "RENAME INDEX requires a logtail read barrier")
	}
	txn := c.proc.GetTxnOperator()
	if err := advanceCatalogLifecycleSnapshot(ctx, catalogLifecycleSnapshotHooks{
		acquireReadBarrier: barrier.AcquireLogtailReadBarrier,
		currentSnapshot:    txn.SnapshotTS,
		updateSnapshot:     txn.UpdateSnapshot,
	}, "index rename publication admission"); err != nil {
		return err
	}
	query := fmt.Sprintf("select 1 from mo_catalog.mo_pubs where database_id = %d or (database_id = 0 and account_id = %d) limit 1", databaseID, accountID)
	res, err := c.runSqlWithResult(query, int32(catalog.System_Account))
	if err != nil {
		return err
	}
	defer res.Close()
	if len(res.Batches) > 0 && res.Batches[0].RowCount() > 0 {
		return moerr.NewNotSupported(ctx, "RENAME INDEX on a published source database")
	}
	return nil
}

func (c *Compile) checkRenameIndexForeignKeys(database, table string, def *plan.TableDef, changed []*plan.IndexDef) error {
	ctx := c.proc.Ctx
	var uniqueIndexes []*plan.IndexDef
	children := make(map[uint64]*plan.TableDef)
	for _, index := range changed {
		if !index.Unique {
			continue
		}
		uniqueIndexes = append(uniqueIndexes, index)
		if err := plan2.CheckDropReferencedKeyForeignKeyDependency(ctx, def, index.IndexName, nil,
			func(id uint64) (*plan.TableDef, error) {
				if child := children[id]; child != nil {
					return child, nil
				}
				_, _, rel, err := c.e.GetRelationById(ctx, c.proc.GetTxnOperator(), id)
				if err != nil {
					return nil, err
				}
				children[id] = rel.GetTableDef(ctx)
				return children[id], nil
			}); err != nil {
			return err
		}
	}
	if len(uniqueIndexes) == 0 {
		return nil
	}
	// A pending catalog declaration may already carry a selected key. Read
	// it through the same tenant-layout decoder used by FK publication.
	fkCatalog, err := getRelFromMoCatalog(c, catalog.MOForeignKeys)
	if err != nil {
		return err
	}
	declarations, _, err := plan2.ReadForeignKeysReferredTo(fkCatalog.GetTableDef(ctx), database, table,
		func(query string) (executor.Result, error) {
			return c.runSqlWithResultAndOptions(query, NoAccountId, executor.StatementOption{}.WithDisableLog())
		})
	if err != nil {
		return err
	}
	for _, constraints := range declarations {
		for _, parts := range constraints {
			for _, index := range uniqueIndexes {
				if len(parts) == 0 || len(parts) > len(index.Parts) ||
					!plan2.IndexNamesEqual(parts[0].ReferencedIndexName, index.IndexName) {
					continue
				}
				// A declaration left by an old table may reuse the name.
				// The catalog reader orders composite parts by constraint_id.
				if slices.EqualFunc(index.Parts[:len(parts)], parts, func(column string, part *plan2.FkReferDef) bool {
					return column == part.ReferCol
				}) {
					return moerr.NewErrDropIndexNeededInForeignKey(ctx, index.IndexName)
				}
			}
		}
	}
	return nil
}

func (c *Compile) renameIndexCatalogRows(databaseID, tableID uint64, indexes []*plan.IndexDef, renames map[string]string) error {
	ctx := c.proc.Ctx
	physicalNames := make([]string, 0, len(indexes))
	byPhysical := make(map[string]*plan.IndexDef, len(indexes))
	for _, index := range indexes {
		physicalNames = append(physicalNames, sqlquote.String(index.IndexTableName))
		byPhysical[index.IndexTableName] = index
	}
	filter := fmt.Sprintf("database_id = %d and table_id = %d", databaseID, tableID)
	query := "select id, index_table_name, name, count(*) from mo_catalog.mo_indexes where " + filter +
		" and index_table_name in (" + strings.Join(physicalNames, ", ") + ") group by id, index_table_name, name"
	res, err := c.runSqlWithResult(query, NoAccountId)
	if err != nil {
		return err
	}
	defer res.Close()

	var cases strings.Builder
	ids := make([]string, 0, len(indexes))
	res.ReadRows(func(rows int, columns []*vector.Vector) bool {
		for row := 0; row < rows; row++ {
			physical := columns[1].GetStringAt(row)
			index := byPhysical[physical]
			name := columns[2].GetStringAt(row)
			if index == nil || name != index.IndexName || vector.GetFixedAtNoTypeCheck[int64](columns[3], row) != int64(len(index.Parts)) {
				err = moerr.NewInternalError(ctx, "inconsistent RENAME INDEX catalog rows")
				return false
			}
			delete(byPhysical, physical)
			id := vector.GetFixedAtNoTypeCheck[uint64](columns[0], row)
			ids = append(ids, fmt.Sprint(id))
			fmt.Fprintf(&cases, " when %d then %s", id, sqlquote.String(renames[name]))
		}
		return true
	})
	if err != nil {
		return err
	}
	if len(byPhysical) != 0 {
		return moerr.NewInternalError(ctx, "missing RENAME INDEX catalog rows")
	}
	query = "update mo_catalog.mo_indexes set name = case id" + cases.String() + " else name end where " +
		filter + " and id in (" + strings.Join(ids, ", ") + ")"
	return c.runSqlWithOptions(query, executor.StatementOption{}.WithDisableLog())
}
