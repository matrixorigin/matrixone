// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func Test_rewriteDecimalTypeIfNecessary(t *testing.T) {
	t1 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id: int32(types.T_decimal64),
	})
	require.Equal(t, []int32{t1.Scale, t1.Width}, []int32{2, 6})
	t2 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id: int32(types.T_decimal128),
	})
	require.Equal(t, []int32{t2.Scale, t2.Width}, []int32{10, 38})
	t3 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id:    int32(types.T_decimal64),
		Scale: 10,
	})
	require.Equal(t, []int32{t3.Scale, t3.Width}, []int32{10, 0})
	t4 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id:    int32(types.T_decimal128),
		Width: 18,
	})
	require.Equal(t, []int32{t4.Scale, t4.Width}, []int32{0, 18})
}

func Test_MakePlan2Vecf32ConstExprWithType(t *testing.T) {
	t1 := MakePlan2Vecf32ConstExprWithType("[1,2,3]", 3)
	actual := t1.Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_Sval).Sval
	require.Equal(t, "[1,2,3]", actual)
}
