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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func DataBranchLineageGCExecutor(
	sqlExecutor executor.SQLExecutor,
) taskservice.TaskExecutor {
	return func(ctx context.Context, _ task.Task) error {
		return compactExpiredAlterDataBranchLineageWithExecutor(
			ctx, sqlExecutor, time.Now().UTC(),
		)
	}
}

func compactExpiredAlterDataBranchLineageWithExecutor(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	now time.Time,
) error {
	return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		statementOpts := executor.StatementOption{}.WithAccountID(catalog.System_Account)
		query := func(sql string) (executor.Result, error) {
			return txn.Exec(sql, statementOpts)
		}

		dag, err := loadAlterDataBranchDAGWithQuery(query, true)
		if err != nil || len(dag.Info) == 0 {
			return err
		}
		edges, err := loadAlterDataBranchLineageEdgesWithQuery(query)
		if err != nil {
			return err
		}
		sources, err := loadAlterDataBranchHistoricalSourcesWithQuery(query, now)
		if err != nil {
			return err
		}
		plan := databranchutils.ComputeAlterLineageCompactionPlan(dag, edges, sources)
		if len(plan.TableIDs) == 0 {
			return nil
		}
		for _, sql := range []string{
			databranchutils.BuildAlterLineageSnapshotDeleteSQL(plan.SnapshotNames),
			databranchutils.BuildAlterLineageMetadataDeleteSQL(plan.TableIDs),
		} {
			res, execErr := txn.Exec(sql, statementOpts)
			res.Close()
			if execErr != nil {
				return execErr
			}
		}
		return nil
	}, executor.Options{}.WithAccountID(catalog.System_Account))
}
