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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExplicitCastUsesDedicatedOverload(t *testing.T) {
	ctx := context.Background()
	source := makePlan2StringConstExprWithType("1")
	targetType := types.T_int64.ToType()
	target := makePlan2TypeValue(&targetType)

	ordinary, err := appendCastBeforeExpr(ctx, DeepCopyExpr(source), target)
	require.NoError(t, err)
	explicit, err := appendExplicitCastBeforeExpr(ctx, DeepCopyExpr(source), target)
	require.NoError(t, err)

	ordinaryFunction := ordinary.GetF().GetFunc()
	explicitFunction := explicit.GetF().GetFunc()
	require.Equal(t, "cast", ordinaryFunction.GetObjName())
	require.Equal(t, "cast", explicitFunction.GetObjName())
	_, ordinaryOverload := function.DecodeOverloadID(ordinaryFunction.GetObj())
	_, explicitOverload := function.DecodeOverloadID(explicitFunction.GetObj())
	require.Equal(t, int32(0), ordinaryOverload)
	require.Equal(t, int32(1), explicitOverload)
}
