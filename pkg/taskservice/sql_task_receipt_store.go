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

package taskservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

func sqlTaskReceiptAsyncResult(receipt *SQLTaskReceiptRequest) task.ExecuteResult {
	result := task.ExecuteResult{Code: task.ResultCode_Success}
	if receipt.Kind == SQLTaskReceiptUnknown || receipt.Run.Status == SQLTaskStatusFailed || receipt.Run.Status == SQLTaskStatusTimeout {
		result.Code = task.ResultCode_Failed
		result.Error = util.Abbreviate(receipt.Run.ErrorMessage, 997)
	}
	return result
}

func applySQLTaskRunOutcome(existing, outcome SQLTaskRun) SQLTaskRun {
	existing.FinishedAt, existing.DurationSeconds, existing.Status = outcome.FinishedAt, outcome.DurationSeconds, outcome.Status
	existing.RowsAffected, existing.ErrorCode, existing.ErrorMessage, existing.GateResult = outcome.RowsAffected, outcome.ErrorCode, outcome.ErrorMessage, outcome.GateResult
	return existing
}

func sameSQLTaskReceiptResult(existing, frozen SQLTaskRun) bool {
	return existing.RunID == frozen.RunID && existing.TaskID == frozen.TaskID &&
		existing.AccountID == frozen.AccountID && existing.TaskName == frozen.TaskName &&
		existing.Status == frozen.Status && existing.RunnerCN == frozen.RunnerCN &&
		existing.AttemptNumber == frozen.AttemptNumber && existing.RowsAffected == frozen.RowsAffected &&
		existing.ErrorCode == frozen.ErrorCode && existing.ErrorMessage == frozen.ErrorMessage &&
		existing.GateResult == frozen.GateResult &&
		existing.FinishedAt.Unix() == frozen.FinishedAt.Unix()
}

var errSQLTaskReceiptConflict = moerr.NewInvalidStateNoCtx("SQL task receipt conflicts with persisted execution state")
