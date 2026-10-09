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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func lifecycleStageLockSQL(stageName string) string {
	return fmt.Sprintf(
		`select stage_id from mo_catalog.mo_stages where stage_name=%s for update`,
		quoteSQLStringLiteral(stageName),
	)
}

func lifecycleStageBindingReferenceSQL(stageID uint64) string {
	return fmt.Sprintf(
		`select binding_id from mo_catalog.mo_lifecycle_bindings
where stage_id=%d limit 1`,
		stageID,
	)
}

func lifecycleStageDatasetReferenceSQL(stageID uint64) string {
	return fmt.Sprintf(
		`select dataset_id from mo_catalog.mo_lifecycle_datasets
where stage_id=%d and state<>'PURGED' limit 1`,
		stageID,
	)
}

// rejectReferencedLifecycleStageMutation runs only in ALTER/DROP STAGE and
// REMOVE @stage. Its caller keeps the transaction and mo_stages row lock until
// the catalog mutation or external REMOVE completes. SET LIFECYCLE locks the
// same row before creating a Binding, closing the first-use race without a
// Feature Guard or changes to ordinary queries, DML, or Merge.
func rejectReferencedLifecycleStageMutation(
	ctx context.Context,
	background BackgroundExec,
	stageName string,
) error {
	background.ClearExecResultSet()
	if err := background.Exec(ctx, lifecycleStageLockSQL(stageName)); err != nil {
		return err
	}
	results, err := getResultSet(ctx, background)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(results) {
		return nil
	}
	stageID, err := results[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return err
	}

	for _, sql := range []string{
		lifecycleStageBindingReferenceSQL(stageID),
		lifecycleStageDatasetReferenceSQL(stageID),
	} {
		background.ClearExecResultSet()
		if err := background.Exec(ctx, sql); err != nil {
			if ignoreErr := ignoreMissingLifecycleCatalog(err); ignoreErr != nil {
				return ignoreErr
			}
			continue
		}
		references, err := getResultSet(ctx, background)
		if err != nil {
			return err
		}
		if execResultArrayHasData(references) {
			return moerr.NewNotSupportedf(
				ctx,
				"Stage %s is referenced by TAE object Lifecycle; "+
					"UNSET the Binding and PURGE its Datasets first",
				stageName,
			)
		}
	}
	return nil
}
