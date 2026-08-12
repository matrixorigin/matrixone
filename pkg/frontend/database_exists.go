// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// checkDatabaseExists checks whether a database exists in the current account.
func checkDatabaseExists(ctx context.Context, bh BackgroundExec, dbName string) (bool, error) {
	return checkDatabaseExistsAtSnapshot(ctx, bh, nil, dbName)
}

// checkDatabaseExistsAtSnapshot checks whether a database exists at the source
// represented by snapshot. A snapshot tenant selects the source account.
func checkDatabaseExistsAtSnapshot(
	ctx context.Context,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
	dbName string,
) (bool, error) {
	newCtx := ctx
	snapshotSpec := ""
	if snapshot != nil {
		if snapshot.TS != nil {
			snapshotSpec = fmt.Sprintf(" {MO_TS = %d}", snapshot.TS.PhysicalTime)
		}
		if snapshot.Tenant != nil {
			newCtx = defines.AttachAccountId(newCtx, snapshot.Tenant.TenantID)
		}
	}

	sql := fmt.Sprintf(
		"SELECT 1 FROM mo_catalog.mo_database%s WHERE datname = %s LIMIT 1",
		snapshotSpec,
		quoteSQLStringLiteral(dbName),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(newCtx, sql); err != nil {
		return false, err
	}
	erArray, err := getResultSet(newCtx, bh)
	if err != nil {
		return false, err
	}
	return len(erArray) > 0 && erArray[0].GetRowCount() > 0, nil
}
