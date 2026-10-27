//go:build !mo_m0

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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDefaultAlterAddColumnUsesCopy(t *testing.T) {
	logicPlan, err := buildSingleStmt(
		NewMockOptimizer(false, newPlanTestProcess(t)),
		t,
		"ALTER TABLE t1 ADD COLUMN default_copy_col TIMESTAMP",
	)
	require.NoError(t, err)
	alter := logicPlan.GetDdl().GetAlterTable()
	require.NotNil(t, alter)
	require.Equal(t, plan.AlterTable_COPY, alter.AlgorithmType)
	require.NotNil(t, alter.CopyTableDef)
	require.NotNil(t, FindColumn(alter.CopyTableDef.Cols, "default_copy_col"))
}

func TestDefaultM0PlanBaseRejectsAdmission(t *testing.T) {
	_, err := m0PlanBase(&TableDef{})
	require.ErrorContains(t, err, "not enabled")
	require.Nil(t, activeM0PlanHooks.selectAdd)
	require.Nil(t, activeM0PlanHooks.preserveClone)
	require.False(t, activeM0PlanHooks.disableCountNotNull)
}
