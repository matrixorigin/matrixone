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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestBuildPerformUsesSelectPlan(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "perform select 1", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)
	require.Equal(t, planpb.Query_SELECT, queryPlan.GetQuery().GetStmtType())
}

func TestBuildPerformRejectsSelectIntoOutfile(t *testing.T) {
	stmt := &tree.Select{
		IsPerform: true,
		Ep:        &tree.ExportParam{},
	}
	_, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.EqualError(t, err, "not supported: PERFORM SELECT INTO OUTFILE")
}
