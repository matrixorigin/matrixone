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

package compile

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// prepareBranchReclaimRC pins every touched component before physical DROP
// work. The returned closure performs the metadata transition after that
// work, still in the same transaction and under the retained component locks.
func (c *Compile) prepareBranchReclaimRC(deadTIDs []uint64, exclusiveSnapshotGate bool) (func() error, databranchutils.BranchReclaimDag, error) {
	if len(deadTIDs) == 0 {
		return nil, databranchutils.BranchReclaimDag{}, nil
	}
	// Even an absent root must be pinned: a concurrent clone can publish its
	// first child after an unlocked empty-row probe.
	dag, err := c.loadBranchReclaimComponentsRC(deadTIDs)
	if err != nil {
		return nil, databranchutils.BranchReclaimDag{}, err
	}
	// An ALTER generation first published between the admission probe and K
	// must restart with G exclusive before physical DROP work or compaction.
	if !exclusiveSnapshotGate && dag.ComponentsHaveAlterLineage(deadTIDs) {
		if err := c.admitLifecycleRC(nil, true, false); err != nil {
			return nil, databranchutils.BranchReclaimDag{}, err
		}
		return nil, databranchutils.BranchReclaimDag{}, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	if len(dag.Info) == 0 {
		return nil, dag, nil
	}
	// Ordinary UPDATE can choose a range lock or fall back to a table lock if
	// its matching rows exceed the lock budget. Pin every row it will change
	// exactly, while the component keys still precede all physical DROP work.
	present := make([]uint64, 0, len(deadTIDs))
	for _, id := range deadTIDs {
		if _, ok := dag.Info[id]; ok {
			present = append(present, id)
		}
	}
	slices.Sort(present)
	present = slices.Compact(present)
	if len(present) == 0 {
		return nil, dag, nil
	}
	keys := batch.NewWithSize(1)
	keys.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	for _, id := range present {
		if err = vector.AppendFixed(keys.Vecs[0], id, false, c.proc.Mp()); err != nil {
			keys.Vecs[0].Free(c.proc.Mp())
			return nil, databranchutils.BranchReclaimDag{}, err
		}
	}
	_, err = c.lockBranchCatalogRowsRC(catalog.MO_BRANCH_METADATA, keys)
	keys.Vecs[0].Free(c.proc.Mp())
	if err != nil {
		return nil, databranchutils.BranchReclaimDag{}, err
	}
	batchSize := max(1, int(c.proc.GetLockService().GetConfig().MaxLockRowCount))
	return func() error {
		if err := databranchutils.MarkAndReclaimBranchSnapshotsCore(
			deadTIDs,
			func() (databranchutils.BranchReclaimDag, error) { return dag, nil },
			func() error {
				for start := 0; start < len(present); start += batchSize {
					end := min(start+batchSize, len(present))
					if err := c.runExactBranchMutationRC(fmt.Sprintf(
						"update %s.%s set table_deleted = true where table_id in (%s)",
						catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, branchReclaimIDList(present[start:end]),
					)); err != nil {
						return err
					}
				}
				return nil
			},
			func(names []string) error {
				if err := c.lockBranchSnapshotNamesRC(names); err != nil {
					return err
				}
				for start := 0; start < len(names); start += batchSize {
					if err := c.runExactBranchMutationRC(databranchutils.BuildBranchSnapshotDeleteSQL(
						names[start:min(start+batchSize, len(names))],
					)); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			return err
		}
		if exclusiveSnapshotGate {
			return c.compactExpiredAlterDataBranchLineageRC(deadTIDs)
		}
		return nil
	}, dag, nil
}

// compactExpiredAlterDataBranchLineageRC finishes the synchronous DROP
// transition. Admission already holds C shared and G exclusive, ahead of D and K;
// the applied RC frontier therefore sees every committed Snapshot/PITR owner.
// Read the complete ownership graph again after the DROP's own mutations, then
// lock and delete only the selected primary keys in this same transaction.
func (c *Compile) compactExpiredAlterDataBranchLineageRC(deadTIDs []uint64) error {
	dag, err := c.loadAlterDataBranchDAG(false)
	if err != nil || len(dag.Info) == 0 {
		return err
	}
	componentIDs := make(map[uint64]struct{})
	for _, id := range dag.ComponentsIDs(deadTIDs) {
		componentIDs[id] = struct{}{}
	}
	edges, err := c.loadAlterDataBranchLineageEdges()
	if err != nil {
		return err
	}
	now := c.proc.GetTxnOperator().SnapshotTS().ToStdTime().UTC()
	sources, err := c.loadAlterDataBranchHistoricalSources(now)
	if err != nil {
		return err
	}
	plan := databranchutils.ComputeAlterLineageCompactionPlan(dag, edges, sources)
	selectedIDs := make([]uint64, 0, len(plan.TableIDs))
	selectedNames := make([]string, 0, len(plan.SnapshotNames))
	for _, id := range plan.TableIDs {
		if _, ok := componentIDs[id]; ok {
			selectedIDs = append(selectedIDs, id)
			selectedNames = append(selectedNames, databranchutils.BranchSnapshotName(id))
		}
	}
	if len(selectedIDs) == 0 {
		return nil
	}
	if err = c.runSqlWithSystemTenant(databranchutils.LineageOwnerLifecycleLockSQL()); err != nil {
		return err
	}
	batchSize := min(128, max(1, int(c.proc.GetLockService().GetConfig().MaxLockRowCount)))
	for start := 0; start < len(selectedIDs); start += batchSize {
		keys := batch.NewWithSize(1)
		keys.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		for _, id := range selectedIDs[start:min(start+batchSize, len(selectedIDs))] {
			if err = vector.AppendFixed(keys.Vecs[0], id, false, c.proc.Mp()); err != nil {
				keys.Vecs[0].Free(c.proc.Mp())
				return err
			}
		}
		_, err = c.lockBranchCatalogRowsRC(catalog.MO_BRANCH_METADATA, keys)
		keys.Vecs[0].Free(c.proc.Mp())
		if err != nil {
			return err
		}
	}
	for start := 0; start < len(selectedNames); start += batchSize {
		if err = c.lockBranchSnapshotNamesRC(selectedNames[start:min(start+batchSize, len(selectedNames))]); err != nil {
			return err
		}
	}
	for start := 0; start < len(selectedIDs); start += batchSize {
		end := min(start+batchSize, len(selectedIDs))
		if err = c.runExactBranchMutationRC(databranchutils.BuildAlterLineageSnapshotDeleteSQL(selectedNames[start:end])); err != nil {
			return err
		}
		if err = c.runExactBranchMutationRC(databranchutils.BuildAlterLineageMetadataDeleteSQL(selectedIDs[start:end])); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) runExactBranchMutationRC(sql string) error {
	oldCtx := c.proc.Ctx
	c.proc.Ctx = lockop.WithExactMutationRows(oldCtx)
	defer func() { c.proc.Ctx = oldCtx }()
	return c.runSqlWithSystemTenant(sql)
}

func branchReclaimIDList(ids []uint64) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return b.String()
}

// DROP DATABASE holds D exclusive, so a branch publisher cannot add a source
// or target edge in that database while this probe runs. When no lineage
// touches its tables, skip thousands of otherwise unnecessary component keys.
// DROP TABLE only holds D shared and must pin even absent roots instead.
func (c *Compile) databaseHasBranchLineageRC(tableIDs []uint64) (bool, error) {
	const batchSize = 256
	for start := 0; start < len(tableIDs); start += batchSize {
		end := min(start+batchSize, len(tableIDs))
		var ids strings.Builder
		for i, id := range tableIDs[start:end] {
			if i > 0 {
				ids.WriteByte(',')
			}
			ids.WriteString(strconv.FormatUint(id, 10))
		}
		for _, column := range [...]string{"p_table_id", "table_id"} {
			res, err := c.runSqlWithResult(fmt.Sprintf(
				"select 1 from %s.%s where %s in (%s) limit 1",
				catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, column, ids.String(),
			), int32(catalog.System_Account))
			if err != nil {
				return false, err
			}
			found := false
			res.ReadRows(func(n int, _ []*vector.Vector) bool {
				found = found || n > 0
				return !found
			})
			res.Close()
			if found {
				return true, nil
			}
		}
	}
	return false, nil
}

// lockBranchCatalogRowsRC retains exact PK locks through the outer owner.
// Admission locks disable later cumulative range coarsening on this table.
func (c *Compile) lockBranchCatalogRowsRC(relationName string, keys *batch.Batch) (uint64, error) {
	ctx := context.WithValue(
		defines.AttachAccountId(c.proc.Ctx, catalog.System_Account),
		defines.LockWriterFairKey{}, false,
	)
	db, err := c.e.Database(ctx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
	if err != nil {
		return 0, err
	}
	relation, err := db.Relation(ctx, relationName, nil)
	if err != nil {
		return 0, err
	}
	id := relation.GetTableID(ctx)
	_, err = lockop.LockRowsForAdmissionWithContext(
		ctx, c.e, c.proc, id, keys, 0, *keys.Vecs[0].GetType(),
		lock.LockMode_Exclusive, catalog.System_Account,
	)
	return id, err
}

func (c *Compile) branchCatalogRelationIDRC(relationName string) (uint64, error) {
	ctx := defines.AttachAccountId(c.proc.Ctx, catalog.System_Account)
	db, err := c.e.Database(ctx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
	if err != nil {
		return 0, err
	}
	relation, err := db.Relation(ctx, relationName, nil)
	if err != nil {
		return 0, err
	}
	return relation.GetTableID(ctx), nil
}

func (c *Compile) loadBranchReclaimComponentsRC(deadTIDs []uint64) (databranchutils.BranchReclaimDag, error) {
	return databranchutils.LoadLockedBranchComponents(
		c.proc.Ctx, deadTIDs,
		func(ctx context.Context, sql string) (executor.Result, error) {
			if err := ctx.Err(); err != nil {
				return executor.Result{}, err
			}
			return c.runSqlWithResult(sql, int32(catalog.System_Account))
		},
		func(roots []uint64) error {
			keys := batch.NewWithSize(1)
			keys.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
			defer keys.Vecs[0].Free(c.proc.Mp())
			for _, root := range roots {
				if err := vector.AppendFixed(keys.Vecs[0], root, false, c.proc.Mp()); err != nil {
					return err
				}
			}
			lockedID, err := c.lockBranchCatalogRowsRC(catalog.MO_BRANCH_METADATA, keys)
			if err != nil {
				return err
			}
			if err = c.advanceLifecycleAdmissionSnapshot(); err != nil {
				return err
			}
			currentID, err := c.branchCatalogRelationIDRC(catalog.MO_BRANCH_METADATA)
			if err != nil {
				return err
			}
			if currentID != lockedID {
				return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
			}
			return nil
		},
	)
}

func (c *Compile) lockBranchSnapshotNamesRC(names []string) error {
	if len(names) == 0 {
		return nil
	}
	keys := batch.NewWithSize(1)
	keys.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	defer keys.Vecs[0].Free(c.proc.Mp())
	for _, name := range names {
		if err := vector.AppendBytes(keys.Vecs[0], []byte(name), false, c.proc.Mp()); err != nil {
			return err
		}
	}
	_, err := c.lockBranchCatalogRowsRC(catalog.MO_SNAPSHOTS, keys)
	return err
}

func (c *Compile) branchDeleteTargetRC() (*databranchutils.BranchDeleteTarget, error) {
	target, err := databranchutils.BranchDeleteTargetFromContext(c.proc.Ctx, c.proc.GetTxnOperator())
	if err != nil || target == nil {
		return target, err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	if !c.isLifecycleRC() || target.AccountID != accountID || target.Database == "" {
		return nil, moerr.NewInternalError(c.proc.Ctx, "invalid DATA BRANCH DELETE owner")
	}
	return target, nil
}

func validateActiveBranchDeleteRows(
	ctx context.Context, target *databranchutils.BranchDeleteTarget,
	dag databranchutils.BranchReclaimDag,
) error {
	for _, id := range target.TableIDs {
		row, ok := dag.Info[id]
		if !ok || row.Deleted || row.Level == databranchutils.AlterLineageLevel {
			name := target.Database
			if target.Table != "" {
				name += "." + target.Table
			}
			return moerr.NewInternalErrorf(ctx,
				"DATA BRANCH DELETE target %s is not an active branch table", name)
		}
	}
	return nil
}

func (c *Compile) validateBranchDeleteTableRC(
	tables []*plan.DropTable, domain map[uint64]dropLifecycleIdentity,
	dag databranchutils.BranchReclaimDag,
) error {
	target, err := c.branchDeleteTargetRC()
	if err != nil || target == nil {
		return err
	}
	if target.Table == "" || target.DatabaseID != 0 || target.MembershipSQL != "" ||
		len(tables) != 1 || tables[0] == nil || len(target.TableIDs) != 1 {
		return moerr.NewInternalError(c.proc.Ctx, "invalid DATA BRANCH DELETE table receipt")
	}
	id := target.TableIDs[0]
	identity, ok := domain[id]
	if !ok || tables[0].Database != target.Database || tables[0].Table != target.Table ||
		identity.database != target.Database || identity.table != target.Table {
		return moerr.NewInternalErrorf(c.proc.Ctx,
			"DATA BRANCH DELETE target %s.%s changed during admission", target.Database, target.Table)
	}
	return validateActiveBranchDeleteRows(c.proc.Ctx, target, dag)
}

func (c *Compile) validateBranchDeleteDatabaseRC(
	database string, databaseID uint64, dag databranchutils.BranchReclaimDag,
) error {
	target, err := c.branchDeleteTargetRC()
	if err != nil || target == nil {
		return err
	}
	if target.Table != "" || target.Database != database || target.DatabaseID == 0 ||
		target.MembershipSQL == "" {
		return moerr.NewInternalError(c.proc.Ctx, "invalid DATA BRANCH DELETE database receipt")
	}
	if target.DatabaseID != databaseID {
		return moerr.NewInternalErrorf(c.proc.Ctx,
			"DATA BRANCH DELETE target %s changed during admission", target.Database)
	}
	res, err := c.runSqlWithResult(target.MembershipSQL, int32(catalog.System_Account))
	if err != nil {
		return err
	}
	defer res.Close()
	current := make([]uint64, 0, len(target.TableIDs))
	names := make(map[uint64]string, len(target.TableIDs))
	var readErr error
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if len(cols) != 2 || cols[0] == nil || cols[0].GetType().Oid != types.T_uint64 ||
			cols[0].Length() != n || cols[0].GetNulls().Any() || cols[1] == nil ||
			cols[1].GetType().Oid != types.T_varchar || cols[1].Length() != n || cols[1].GetNulls().Any() {
			readErr = moerr.NewInternalError(c.proc.Ctx, "invalid DATA BRANCH DELETE membership")
			return false
		}
		for i := 0; i < n; i++ {
			id := vector.GetFixedAtWithTypeCheck[uint64](cols[0], i)
			current = append(current, id)
			names[id] = cols[1].GetStringAt(i)
		}
		return true
	})
	if readErr != nil {
		return readErr
	}
	slices.Sort(current)
	if !slices.Equal(current, target.TableIDs) {
		for _, id := range current {
			if !slices.Contains(target.TableIDs, id) {
				row, ok := dag.Info[id]
				if !ok || row.Deleted || row.Level == databranchutils.AlterLineageLevel {
					return moerr.NewInternalErrorf(c.proc.Ctx,
						"DATA BRANCH DELETE target %s.%s is not an active branch table",
						target.Database, names[id])
				}
			}
		}
		return moerr.NewInternalErrorf(c.proc.Ctx,
			"DATA BRANCH DELETE target %s changed during admission", target.Database)
	}
	return validateActiveBranchDeleteRows(c.proc.Ctx, target, dag)
}
