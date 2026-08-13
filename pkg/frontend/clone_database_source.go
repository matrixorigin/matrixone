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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	opAccountId        uint32
	toAccountId        uint32
}

type cloneDatabaseAccountResolution struct {
	opAccountId uint32
	toAccountId uint32
	snapshot    *plan.Snapshot
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
	resolvedAccounts *cloneDatabaseAccountResolution,
) (cloneDatabaseSource, error) {
	source := cloneDatabaseSource{
		srcPrivilegeDBName: stmt.SrcDatabase.String(),
		viewMap:            make(map[string]*tableInfo),
	}

	accounts := cloneDatabaseAccountResolution{}
	if resolvedAccounts != nil {
		accounts = *resolvedAccounts
	} else {
		var err error
		if accounts, err = resolveCloneDatabaseAccounts(ctx, ses, bh, stmt); err != nil {
			return source, err
		}
	}
	if err := validateCloneDatabaseAccounts(ctx, accounts); err != nil {
		return source, err
	}
	snapshot := accounts.snapshot

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

	sourceExists, err := checkDatabaseExistsAtSnapshot(ctx, bh, snapshot, srcDBName)
	if err != nil {
		return source, err
	}
	if !sourceExists {
		return source, moerr.NewBadDB(ctx, srcDBName)
	}

	srcTblInfos, err := getTableInfos(ctx, ses.GetService(), bh, snapshot, srcDBName, "")
	if err != nil {
		return source, err
	}
	fkDeps, err := getFkDeps(ctx, bh, snapshot, srcDBName, "")
	if err != nil {
		return source, err
	}
	schemaFkDeps, err := getFkDepsFromTableInfos(ctx, srcTblInfos)
	if err != nil {
		return source, err
	}
	mergeFkDeps(fkDeps, schemaFkDeps)
	sortedFkTbls, hasFkCycle := cloneFkTableOrder(fkDeps)
	fkTableMap, err := getTableInfoMap(ctx, ses.GetService(), bh, snapshot, srcDBName, "", sortedFkTbls)
	if err != nil {
		return source, err
	}

	for _, srcTbl := range srcTblInfos {
		if srcTbl.typ == view {
			source.viewMap[genKey(srcTbl.dbName, srcTbl.tblName)] = srcTbl
		}
	}

	source.srcResolveDBName = srcDBName
	source.srcTblInfos = srcTblInfos
	source.sortedFkTbls = sortedFkTbls
	source.fkTableMap = fkTableMap
	source.hasFkCycle = hasFkCycle
	source.snapshot = snapshot
	source.opAccountId = accounts.opAccountId
	source.toAccountId = accounts.toAccountId
	return source, nil
}

func resolveCloneDatabaseAccounts(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
) (cloneDatabaseAccountResolution, error) {
	opAccountId, toAccountId, snapshot, err := getOpAndToAccountId(
		ctx, ses, bh, stmt.ToAccountOpt, stmt.AtTsExpr,
	)
	if err != nil {
		return cloneDatabaseAccountResolution{}, err
	}
	return cloneDatabaseAccountResolution{
		opAccountId: opAccountId,
		toAccountId: toAccountId,
		snapshot:    snapshot,
	}, nil
}

func validateCloneDatabaseAccounts(
	ctx context.Context,
	accounts cloneDatabaseAccountResolution,
) error {
	if accounts.snapshot == nil && accounts.opAccountId != accounts.toAccountId {
		return moerr.NewInternalErrorNoCtxf("clone database between different accounts need a snapshot")
	}
	if accounts.opAccountId != sysAccountID && accounts.opAccountId != accounts.toAccountId {
		return moerr.NewInternalError(ctx, "only sys can clone table to another account")
	}
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
