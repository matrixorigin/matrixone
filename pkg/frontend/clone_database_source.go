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

package frontend

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type cloneDatabaseSource struct {
	srcResolveDBName   string
	srcPrivilegeDBName string
	srcTblInfos        []*tableInfo
	viewMap            map[string]*tableInfo
	sortedFkTbls       []string
	fkTableMap         map[string]*tableInfo
	hasFkCycle         bool
	snapshot           *plan.Snapshot
	readTS             int64
	opAccountId        uint32
	toAccountId        uint32
}

var cloneDatabaseSourceReadHook atomic.Pointer[func(string)]

// SetCloneDatabaseSourceReadHookForTest observes a completed source inventory.
// Production leaves it unset; tests can coordinate DDL before target creation.
func SetCloneDatabaseSourceReadHookForTest(hook func(string)) func() {
	var next *func(string)
	if hook != nil {
		next = &hook
	}
	previous := cloneDatabaseSourceReadHook.Swap(next)
	return func() { cloneDatabaseSourceReadHook.Store(previous) }
}

func (source *cloneDatabaseSource) sortViews(ctx context.Context, ses *Session, bh BackgroundExec, snapshotTS int64) ([]string, error) {
	snapshot := prepareCloneSourceSnapshot(source.snapshot, snapshotTS)
	account := source.opAccountId
	if snapshot != nil && snapshot.Tenant != nil {
		account = snapshot.Tenant.TenantID
	}
	// CLONE plans in the publisher's namespace without changing the caller's
	// session. Keep this guard here: RESTORE owns its existing database state.
	compCtx := ses.GetTxnCompileCtx()
	oldContext, oldDatabase := compCtx.GetContext(), compCtx.GetDatabase()
	compCtx.SetContext(defines.AttachAccountId(oldContext, account))
	defer func() {
		compCtx.SetContext(oldContext)
		compCtx.SetDatabase(oldDatabase)
	}()
	return sortedViewInfos(ctx, ses, bh, "", snapshot, source.viewMap, account, source.toAccountId)
}

func (source *cloneDatabaseSource) branchTableCount() int64 {
	var count int64
	for _, table := range source.srcTblInfos {
		if table.typ != view {
			count++
		}
	}
	return count
}

func collectCloneDatabaseSource(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
	privateWorkspace bool,
) (cloneDatabaseSource, error) {
	source := cloneDatabaseSource{
		srcPrivilegeDBName: stmt.SrcDatabase.String(),
		viewMap:            make(map[string]*tableInfo),
	}

	opAccountId, toAccountId, snapshot, err := getOpAndToAccountId(
		ctx, ses, bh, stmt.ToAccountOpt, stmt.AtTsExpr,
	)
	if err != nil {
		return source, err
	}
	if snapshot == nil && opAccountId != toAccountId {
		return source, moerr.NewInternalErrorNoCtxf("clone database between different accounts need a snapshot")
	}
	if opAccountId != sysAccountID && opAccountId != toAccountId {
		return source, moerr.NewInternalError(ctx, "only sys can clone table to another account")
	}

	srcDBName := stmt.SrcDatabase.String()
	subMeta, err := ses.GetTxnCompileCtx().GetSubscriptionMeta(srcDBName, snapshot)
	if err != nil {
		return source, err
	}
	if subMeta != nil {
		srcDBName = subMeta.DbName
		if snapshot != nil {
			snapshot.Tenant = &plan.SnapshotTenant{TenantID: uint32(subMeta.AccountId)}
		} else {
			snapshot = &plan.Snapshot{
				Tenant: &plan.SnapshotTenant{TenantID: uint32(subMeta.AccountId)},
			}
		}
	}

	// Keep inventory, dependency ordering and table readers at one source
	// boundary. Target-name admission may later advance the write transaction,
	// but must not change which source schema this inventory describes.
	if privateWorkspace && stmt.AtTsExpr == nil {
		if source.readTS, err = tryToIncreaseTxnPhysicalTS(ctx, cloneSnapshotTxnOperator(ses, bh)); err != nil {
			return source, err
		}
	}

	source.srcResolveDBName = srcDBName
	source.snapshot = snapshot
	source.opAccountId = opAccountId
	source.toAccountId = toAccountId
	err = source.collectTables(ctx, ses.GetService(), bh, prepareCloneSourceSnapshot(snapshot, source.readTS))
	if err == nil {
		if hook := cloneDatabaseSourceReadHook.Load(); hook != nil {
			(*hook)(source.srcResolveDBName)
		}
	}
	return source, err
}

func (source *cloneDatabaseSource) collectTables(ctx context.Context, sid string, bh BackgroundExec, snapshot *plan.Snapshot) error {
	srcDBName := source.srcResolveDBName
	srcTblInfos, err := getTableInfos(ctx, sid, bh, snapshot, srcDBName, "")
	if err != nil {
		return err
	}
	fkDeps, err := getFkDeps(ctx, bh, snapshot, srcDBName, "")
	if err != nil {
		return err
	}
	schemaFkDeps, err := readTableDefinitionFKDeps(ctx, srcTblInfos, func(info *tableInfo, create *tree.CreateTable) {
		info.batchCloneIndexCatalog = info.relKind == catalog.SystemOrdinaryRel && compile.CloneIndexCatalogCanBatch(create)
		info.parallelClone = info.batchCloneIndexCatalog && create.PartitionOption == nil
	})
	if err != nil {
		return err
	}
	mergeFkDeps(fkDeps, schemaFkDeps)
	sortedFkTbls, hasFkCycle := cloneFkTableOrder(fkDeps)
	// Reuse the same definitions used to derive FK order in this collection.
	var inventory map[restoreObjectName]*tableInfo
	if len(sortedFkTbls) != 0 {
		inventory = make(map[restoreObjectName]*tableInfo, len(srcTblInfos))
		for _, info := range srcTblInfos {
			inventory[restoreObjectName{info.dbName, info.tblName}] = info
		}
	}
	fkTableMap, err := getTableInfoMap(ctx, sid, bh, snapshot, srcDBName, "", sortedFkTbls, inventory)
	if err != nil {
		return err
	}

	for _, srcTbl := range srcTblInfos {
		if srcTbl.typ == view {
			source.viewMap[genKey(srcTbl.dbName, srcTbl.tblName)] = srcTbl
		}
	}

	source.srcTblInfos = srcTblInfos
	source.sortedFkTbls = sortedFkTbls
	source.fkTableMap = fkTableMap
	source.hasFkCycle = hasFkCycle
	return nil
}

func cloneFkTableOrder(fkDeps map[string][]string) (sortedTbls []string, hasCycle bool) {
	g := toposort{next: make(map[string][]string)}
	for key, deps := range fkDeps {
		g.addVertex(key)
		for _, depTbl := range deps {
			if key != depTbl {
				g.addEdge(depTbl, key)
			}
		}
	}

	sortedTbls, err := g.sort()
	if err == nil {
		return sortedTbls, false
	}

	// CREATE TABLE resolves forward foreign-key references while
	// foreign_key_checks is disabled. A deterministic order is sufficient for
	// a cyclic component because creating its later tables backfills those
	// references.
	sortedTbls = sortedTbls[:0]
	for key := range g.next {
		sortedTbls = append(sortedTbls, key)
	}
	sort.Strings(sortedTbls)
	return sortedTbls, true
}
