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
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedAnalyzeProtocolContract(t *testing.T) {
	require.NotZero(t, DefaultCapability&CLIENT_PS_MULTI_RESULTS)

	stmt, err := mysql.ParseOne(t.Context(), "prepare p from analyze table t(a)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	prepareStmt := stmt.(*tree.PrepareStmt)
	require.IsType(t, &tree.AnalyzeStmt{}, prepareStmt.Stmt)

	columns := getPreparedResultColumnsFor(prepareStmt.Stmt, &planpb.Plan{}, false)
	require.Empty(t, columns)
}
