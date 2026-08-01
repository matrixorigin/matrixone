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
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

const maxPendingViewMetadataRetries = 1024

var retryPendingViewMetadataFunc = retryPendingViewMetadata

func retryPendingViewMetadata(ctx context.Context, ses *Session, bh BackgroundExec) error {
	bh.ClearExecResultSet()
	query := fmt.Sprintf(
		"select account_id, rel_id, rel_version, reldatabase, relname, viewdef "+
			"from %s.%s where relkind = '%s' and "+
			"json_unquote(json_extract(viewdef, '$.metadata_refresh_pending')) = 'true' "+
			"order by account_id, rel_id limit %d",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		catalog.SystemViewRel,
		maxPendingViewMetadataRetries+1,
	)
	if err := bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), query); err != nil {
		return err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(results) {
		return nil
	}
	if results[0].GetRowCount() > maxPendingViewMetadataRetries {
		return moerr.NewInvalidInputf(ctx, "dependency recovery affects more than %d pending views", maxPendingViewMetadataRetries)
	}
	lower := int64(1)
	if value, err := ses.GetSessionSysVar("lower_case_table_names"); err == nil {
		lower, _ = value.(int64)
	}
	for row := uint64(0); row < results[0].GetRowCount(); row++ {
		accountID, err := results[0].GetUint64(ctx, row, 0)
		if err != nil {
			return err
		}
		viewID, err := results[0].GetUint64(ctx, row, 1)
		if err != nil {
			return err
		}
		version, err := results[0].GetUint64(ctx, row, 2)
		if err != nil {
			return err
		}
		database, err := results[0].GetString(ctx, row, 3)
		if err != nil {
			return err
		}
		name, err := results[0].GetString(ctx, row, 4)
		if err != nil {
			return err
		}
		definition, err := results[0].GetString(ctx, row, 5)
		if err != nil {
			return err
		}
		var viewData plan2.ViewData
		if err = json.Unmarshal([]byte(definition), &viewData); err != nil {
			continue
		}
		sql, err := compile.BuildViewMetadataRefreshSQL(ctx, lower, database, name, viewData)
		if err != nil {
			continue
		}
		retryCtx := defines.AttachAccountId(ctx, uint32(accountID))
		retryCtx = context.WithValue(retryCtx, defines.ViewMetadataRetryKey{}, defines.ViewMetadataRetry{
			TargetViewID:         viewID,
			TargetViewVersion:    uint32(version),
			TargetViewDefinition: definition,
		})
		if viewData.DefaultDatabase != "" {
			bh.ClearExecResultSet()
			if err = bh.Exec(retryCtx, "use "+sqlquote.Ident(viewData.DefaultDatabase)); err != nil {
				return err
			}
		}
		sqlMode := plan2.LegacyViewParserSQLMode()
		if viewData.SQLMode != nil {
			sqlMode = *viewData.SQLMode
		}
		bh.ClearExecResultSet()
		if err = bh.ExecWithSQLMode(retryCtx, sql, sqlMode); err != nil {
			if canSkipPendingViewMetadataRetry(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func canSkipPendingViewMetadataRetry(err error) bool {
	code, ok := moerr.GetMoErrCode(err)
	if !ok {
		return false
	}
	switch code {
	case moerr.ErrInvalidInput,
		moerr.ErrConstraintViolation,
		moerr.ErrParseError,
		moerr.ErrBadFieldError,
		moerr.ErrOperandColumns,
		moerr.ErrViewWrongList,
		moerr.ErrBadDB,
		moerr.ErrNoSuchTable,
		moerr.ErrNoDB,
		moerr.ErrBadView:
		return true
	default:
		return false
	}
}
