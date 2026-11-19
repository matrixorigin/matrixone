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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestLocalOrderedCutoffDirectionAndTies(t *testing.T) {
	point := func(value int64) index.ZM {
		zm := objectio.NewZM(types.T_int64, 0)
		index.UpdateZM(zm, types.EncodeInt64(&value))
		return zm
	}
	for _, desc := range []bool{false, true} {
		flag := plan.OrderBySpec_ASC
		if desc {
			flag = plan.OrderBySpec_DESC
		}
		source := &LocalDisttaeDataSource{}
		source.SetOrderBy([]*plan.OrderBySpec{{Flag: flag, Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}}})
		source.SetFilterZM(point(8))
		weaker := int64(10)
		if desc {
			weaker = 6
		}
		source.SetFilterZM(point(weaker)) // can arrive before handleOrderBy
		source.blockZMS = []index.ZM{point(7), point(8), point(9), nil}
		if desc {
			require.False(t, source.needReadBlkByZM(0))
			require.True(t, source.needReadBlkByZM(1))
			require.True(t, source.needReadBlkByZM(2))
		} else {
			require.True(t, source.needReadBlkByZM(0))
			require.True(t, source.needReadBlkByZM(1))
			require.False(t, source.needReadBlkByZM(2))
		}
		require.True(t, source.needReadBlkByZM(3), "unknown blocks must be read")
	}
}
