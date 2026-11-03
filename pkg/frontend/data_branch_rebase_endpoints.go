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

package frontend

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planutil "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func validateRebaseTimestamp(ctx context.Context, snapshot *plan.Snapshot) error {
	if snapshot == nil || snapshot.TS == nil || snapshot.TS.PhysicalTime <= 0 || snapshot.TS.LogicalTime != 0 {
		return moerr.NewNotSupported(ctx, "Rebase requires a positive fixed timestamp with zero logical time")
	}
	return nil
}

// A common LCA alone does not establish a base/head relationship. Walk the
// actual parent edges, bounding visibility by every intervening fork timestamp.
func rebaseBaseIsAncestor(dag databranchutils.BranchReclaimDag, baseID uint64, baseTS int64, headID uint64, headTS int64) bool {
	if baseID == 0 || headID == 0 || baseTS <= 0 || headTS < baseTS {
		return false
	}
	seen := make(map[uint64]bool)
	found := false
	for {
		if seen[headID] {
			return false
		}
		seen[headID] = true
		if headID == baseID {
			if baseTS > headTS {
				return false
			}
			found = true
			headTS = baseTS
		}
		node, exists := dag.Info[headID]
		if !exists || node.ParentTableID == 0 {
			return found
		}
		if node.CloneTS <= 0 || node.CloneTS > headTS {
			return false
		}
		headID, headTS = node.ParentTableID, node.CloneTS
	}
}

type rebaseSnapshotGuard struct {
	account  uint32
	name     string
	snapshot *plan.Snapshot
	branch   *snapshotRecord
}

// All name/component domains and the quota Q are held before existing E keys.
// Protected MO_TS references are checked against both the locked DAG and the
// exact branch-pin row, never treated as self-authenticating timestamps.
func protectRebaseEndpoints(ctx context.Context, ses *Session, bh BackgroundExec, admission *lifecycleRCAdmission,
	sources [4]rebaseBoundDatabase, dag databranchutils.BranchReclaimDag,
) error {
	type guardKey struct {
		account uint32
		name    string
	}
	guards := make(map[guardKey]rebaseSnapshotGuard)
	for _, source := range sources {
		snapshot := source.source.snapshot
		if err := validateRebaseTimestamp(ctx, snapshot); err != nil {
			return err
		}
		if snapshot.ExtraInfo != nil && snapshot.ExtraInfo.Name != "" {
			if snapshot.ExtraInfo.Level != dataBranchLevel_Database && snapshot.ExtraInfo.Level != dataBranchLevel_Account {
				return moerr.NewNotSupported(ctx, "Rebase requires a database/account snapshot or protected branch reference")
			}
			// Resolve identity under the same admitted transaction and historical
			// timestamp, including empty databases. A snapshot of another database
			// must not be used merely as a retention token for this endpoint.
			ids, err := readRebaseCounts(ctx, bh, fmt.Sprintf("select dat_id from mo_catalog.mo_database {MO_TS = %d} where account_id=%d and datname=%s",
				snapshot.TS.PhysicalTime, source.source.opAccountId, rebaseTextLiteral(source.source.srcResolveDBName)), 1)
			if err != nil {
				return err
			}
			if err = planutil.ValidateSnapshotDatabaseScope(snapshot, source.source.srcResolveDBName, ids[0]); err != nil {
				return err
			}
			for _, def := range source.defs {
				if def.DbId != ids[0] {
					return moerr.NewTxnNeedRetryWithDefChanged(ctx)
				}
			}
			key := guardKey{source.source.opAccountId, snapshot.ExtraInfo.Name}
			if old, exists := guards[key]; exists && (old.snapshot == nil || old.snapshot.TS.PhysicalTime != snapshot.TS.PhysicalTime ||
				old.snapshot.ExtraInfo.ObjId != snapshot.ExtraInfo.ObjId) {
				return moerr.NewTxnNeedRetryWithDefChanged(ctx)
			}
			guards[key] = rebaseSnapshotGuard{key.account, key.name, snapshot, nil}
			continue
		}
		if len(source.defs) == 0 {
			return moerr.NewNotSupported(ctx, "Rebase cannot prove retention of a bare timestamp for an empty database")
		}
		for table, def := range source.defs {
			query := fmt.Sprintf("%s where kind=%s and ts=%d and obj_id=%d and database_name=%s and table_name=%s and account_name=%s order by sname limit 1",
				getSnapshotFormat, quoteSQLStringLiteral(branchSnapshotKind), snapshot.TS.PhysicalTime, def.TblId,
				rebaseTextLiteral(source.source.srcResolveDBName), rebaseTextLiteral(table), rebaseTextLiteral(ses.GetTenantInfo().GetTenant()))
			records, err := getSnapshotRecords(defines.AttachAccountId(ctx, catalog.System_Account), bh, query)
			clearRebaseResults(bh)
			if err != nil {
				return err
			}
			if len(records) != 1 || !rebaseBranchPinMatches(dag, records[0], def.TblId, snapshot.TS.PhysicalTime) {
				return moerr.NewNotSupported(ctx, "Rebase timestamp is not protected by an existing branch reference")
			}
			record := records[0]
			key := guardKey{catalog.System_Account, record.snapshotName}
			guards[key] = rebaseSnapshotGuard{key.account, key.name, nil, record}
		}
	}
	keys := make([]guardKey, 0, len(guards))
	for key := range guards {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b guardKey) int {
		if a.account < b.account {
			return -1
		} else if a.account > b.account {
			return 1
		}
		return strings.Compare(a.name, b.name)
	})
	for _, key := range keys {
		guard := guards[key]
		if err := admission.lockSnapshotName(ctx, guard.name, guard.account); err != nil {
			return err
		}
		guardCtx := defines.AttachAccountId(ctx, guard.account)
		if guard.snapshot != nil {
			if err := lockNamedDataBranchCloneSnapshot(guardCtx, bh, guard.snapshot); err != nil {
				return err
			}
		} else {
			query := fmt.Sprintf("%s where sname=%s", getSnapshotFormat, rebaseTextLiteral(guard.name))
			records, err := getSnapshotRecords(guardCtx, bh, query)
			clearRebaseResults(bh)
			if err != nil {
				return err
			}
			if len(records) != 1 || *records[0] != *guard.branch {
				return moerr.NewTxnNeedRetryWithDefChanged(ctx)
			}
		}
		clearRebaseResults(bh)
	}
	return nil
}

func rebaseBranchPinMatches(dag databranchutils.BranchReclaimDag, pin *snapshotRecord, tableID uint64, ts int64) bool {
	if pin == nil || pin.kind != branchSnapshotKind || pin.level != dataBranchLevel_Table || pin.objId != tableID || pin.ts != ts {
		return false
	}
	child, err := strconv.ParseUint(strings.TrimPrefix(pin.snapshotName, "__mo_branch_"), 10, 64)
	if err != nil || branchSnapshotName(child) != pin.snapshotName {
		return false
	}
	edge, exists := dag.Info[child]
	return exists && edge.ParentTableID == tableID && edge.CloneTS == ts
}
