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

package predefine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

func TestGenInitCronTaskSQLIncludesDataBranchLineageGC(t *testing.T) {
	sql, err := GenInitCronTaskSQL(int32(task.TaskCode_DataBranchLineageGC))
	require.NoError(t, err)
	require.Contains(t, sql, databranchutils.LineageGCTaskID)
	require.Contains(t, sql, databranchutils.LineageGCTaskCronExpr)
}
