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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestCTASSoundexRetainsDeclaredCapacity(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, `create table t_soundex_ctas as select
		soundex(cast(concat(repeat('BC', 32767), 'B') as varchar(65535))) as code_65535,
		soundex(cast(repeat('BC', 32768) as mediumtext)) as code_65536
		from nation limit 1`, 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 2)
	require.Equal(t, int32(types.T_varchar), visible[0].Typ.Id)
	require.Equal(t, int32(types.MaxVarcharLen), visible[0].Typ.Width)
	require.Equal(t, int32(types.T_text), visible[1].Typ.Id)
	require.Equal(t, int32(types.MaxMediumTextLen), visible[1].Typ.Width)
}
