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

package db_holder

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

// StatementInfoTextLimit is the decoded byte capacity of statement_info's TEXT fields.
const StatementInfoTextLimit = types.MaxStringSize
const StatementInfoTruncationMarker = "...[truncated]"

// CapStatementInfoText renders valid UTF-8 diagnostics within the storage budget.
// Valid retained text is not copied. Callers reserve enough budget for the marker.
func CapStatementInfoText(s string, budget int) string {
	if len(s) <= budget {
		return commonutil.Abbreviate(s, -1)
	}
	prefix := s[:commonutil.UTF8PrefixLen(s, budget-len(StatementInfoTruncationMarker))]
	return commonutil.Abbreviate(prefix, -1) + StatementInfoTruncationMarker
}

// StatementInfoPlanSummary replaces an oversized plan with valid JSON rather
// than an unusable truncated document. Status and err_code describe execution;
// code 200 follows the existing convention for an omitted recorded plan.
func StatementInfoPlanSummary(originalBytes int) []byte {
	result := []byte(`{"code":200,"message":"exec_plan omitted: TEXT capacity exceeded ...[truncated]","truncated":true,"original_bytes":`)
	result = strconv.AppendInt(result, int64(originalBytes), 10)
	return append(result, `,"limit_bytes":65535}`...)
}

// Resolve once per batch; historical CSV must use the descriptor's named layout.
// A malformed descriptor is an upload failure, not a reason to discard its file.
func statementInfoDiagnosticColumns(tbl *table.Table) ([3]int, error) {
	indices := [3]int{-1, -1, -1}
	if tbl.Database != "system" || tbl.Table != "statement_info" {
		return indices, nil
	}
	for i, col := range tbl.Columns {
		slot := -1
		switch col.Name {
		case "statement":
			slot = 0
		case "error":
			slot = 1
		case "exec_plan":
			slot = 2
		}
		if slot >= 0 {
			if indices[slot] >= 0 {
				return indices, moerr.NewInternalErrorNoCtx("ambiguous statement_info diagnostic columns")
			}
			indices[slot] = i
		}
	}
	if indices[0] < 0 || indices[1] < 0 || indices[2] < 0 {
		return indices, moerr.NewInternalErrorNoCtx("missing statement_info diagnostic columns")
	}
	return indices, nil
}
