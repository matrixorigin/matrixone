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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestInRangeAcceptsPromotedDecimalConstants(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		select in_range(c, 9999999999999999999999999999999999999.9,
			99999999999999999999999999999999999999, 0)
		from (select cast(1 as decimal(38,0)) as c) t`)
	require.NoError(t, err)
	found := false
	for _, node := range logicPlan.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			fn := expr.GetF()
			if fn == nil || fn.Func.ObjName != "in_range" {
				continue
			}
			found = true
			for _, arg := range fn.Args[:3] {
				require.Equal(t, int32(types.T_decimal256), arg.Typ.Id)
				require.Equal(t, int32(39), arg.Typ.Width)
				require.Equal(t, int32(1), arg.Typ.Scale)
			}
		}
	}
	require.True(t, found, "the column range must retain its promoted comparison domain")
}

func TestInRangeStillRejectsNonconstantBounds(t *testing.T) {
	for _, sql := range []string{
		"select in_range(c, c, 15.0, 0) from (select cast(1 as decimal(38,0)) c) t",
		"select in_range(c, 0.0, c, 0) from (select cast(1 as decimal(38,0)) c) t",
		"select in_range(c, cast(rand() as decimal(38,0)), 15.0, 0) from (select cast(1 as decimal(38,0)) c) t",
		"select in_range(c, 0.0, cast(rand() as decimal(38,0)), 0) from (select cast(1 as decimal(38,0)) c) t",
	} {
		_, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.ErrorContains(t, err, "argument of in_range must be constant", sql)
	}
}
