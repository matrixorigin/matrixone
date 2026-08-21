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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestFormatAsofJoinTolerance(t *testing.T) {
	intervalName := NewUnresolvedColName("interval")
	tolerance := &FuncExpr{
		Func:     FuncName2ResolvableFunctionReference(intervalName),
		FuncName: NewCStr("interval", 1),
		Exprs: Exprs{
			NewNumVal[int64](2, "2", false, P_int64),
			NewTimeUnitExpr("minute"),
		},
	}
	join := &JoinTableExpr{
		Left:      NewTableName("l", ObjectNamePrefix{}, nil),
		JoinType:  JOIN_TYPE_ASOF_LEFT,
		Right:     NewTableName("r", ObjectNamePrefix{}, nil),
		Tolerance: tolerance,
	}
	require.Equal(t, "l asof left join r tolerance INTERVAL 2 minute", String(join, dialect.MYSQL))
}
