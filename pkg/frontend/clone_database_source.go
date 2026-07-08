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
	snapshot           *plan.Snapshot
	opAccountId        uint32
	toAccountId        uint32
}

func collectCloneDatabaseSource(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
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

	srcTblInfos, err := getTableInfos(ctx, ses.GetService(), bh, snapshot, srcDBName, "")
	if err != nil {
		return source, err
	}
	sortedFkTbls, err := fkTablesTopoSort(ctx, bh, snapshot, srcDBName, "")
	if err != nil {
		return source, err
	}
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
	source.snapshot = snapshot
	source.opAccountId = opAccountId
	source.toAccountId = toAccountId
	return source, nil
}
