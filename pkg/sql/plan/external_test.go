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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestJudgeContainColnameAllowsNestedZeroArgumentFunction(t *testing.T) {
	filter := &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: ">"},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_F{
							F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "rand"}},
						},
					},
					{
						Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{}},
					},
				},
			},
		},
	}

	require.False(t, judgeContainColname(filter))
}
