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
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// reserveRebaseFeatureSource runs after endpoint preflight and lifecycle
// admission, before any candidate write. Its partial writes belong to the
// existing owner and must be rolled back by that owner's normal error path.
func reserveRebaseFeatureSource(ctx context.Context, eng engine.Engine, op client.TxnOperator,
	feature rebaseBoundDatabase, expected databaseInventoryToken) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	if !expected.tracked || expected.accountID != account || expected.databaseID == 0 ||
		expected.epoch == 0 || expected.epoch == math.MaxUint64 || op == nil || op.IsSnapOp() {
		return moerr.NewInvalidInput(ctx, "invalid Rebase source expectation")
	}
	meta := op.Txn()
	if !meta.IsPessimistic() || !meta.IsRCIsolation() {
		return moerr.NewNotSupported(ctx, "Rebase publication requires pessimistic RC")
	}
	source := feature.source
	if source.opAccountId != account || source.toAccountId != account ||
		source.srcResolveDBName == "" || strings.ContainsRune(source.srcResolveDBName, 0) ||
		source.snapshot == nil || source.snapshot.TS == nil || source.snapshot.TS.PhysicalTime <= 0 ||
		(source.snapshot.Tenant != nil && source.snapshot.Tenant.TenantID != account) || len(feature.defs) == 0 {
		return moerr.NewInvalidInput(ctx, "invalid fixed Rebase feature source")
	}
	reservation, ok := eng.(interface {
		ReserveDatabaseInventory(context.Context, string, uint64, uint64, client.TxnOperator) error
	})
	if !ok {
		return moerr.NewNotSupported(ctx, "Rebase inventory reservation unavailable")
	}
	live, err := eng.Database(ctx, source.srcResolveDBName, op)
	if err != nil {
		return err
	}
	id, err := strconv.ParseUint(live.GetDatabaseId(ctx), 10, 64)
	if err != nil || id != expected.databaseID {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	names := make([]string, 0, len(feature.defs))
	for name := range feature.defs {
		names = append(names, name)
	}
	slices.Sort(names)
	dependencies := make([]engine.RebaseSnapshotDependency, 0, len(names))
	for _, name := range names {
		def := feature.defs[name]
		if def == nil || def.DbId != id || def.TblId == 0 || def.Name != name {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		relation, err := live.Relation(ctx, name, nil)
		if err != nil {
			return err
		}
		if relation.GetTableID(ctx) != def.TblId {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		dependency, ok := relation.(engine.RebaseSnapshotDependency)
		if !ok {
			return moerr.NewNotSupported(ctx, "Rebase source write dependency unavailable")
		}
		dependencies = append(dependencies, dependency)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := reservation.ReserveDatabaseInventory(ctx, source.srcResolveDBName, id, expected.epoch, op); err != nil {
		return err
	}
	for _, dependency := range dependencies {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := dependency.RegisterRebaseSnapshotDependency(ctx, source.snapshot.TS.PhysicalTime); err != nil {
			return err
		}
	}
	return ctx.Err()
}
