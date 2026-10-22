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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSelectIntoKindLayoutValidationAndVisibleColumns(t *testing.T) {
	query := &pb.Query{
		StmtType:                    pb.Query_SELECT,
		Steps:                       []int32{0},
		Headings:                    []string{"v"},
		Nodes:                       []*pb.Node{{ProjectList: []*pb.Expr{{Typ: pb.Type{Id: int32(types.T_varchar)}}, {Typ: pb.Type{Id: int32(types.T_bool)}}}}},
		IntoUserVariableKindColumns: []int32{1},
	}
	require.NoError(t, ValidateSelectIntoKindLayout(query))
	columns := GetResultColumnsFromPlan(&Plan{Plan: &pb.Plan_Query{Query: query}})
	require.Len(t, columns, 1)
	require.Equal(t, "v", columns[0].Name)
	require.Equal(t, int32(types.T_varchar), columns[0].Typ.Id)

	query.IntoUserVariableKindColumns = []int32{-1}
	require.ErrorContains(t, ValidateSelectIntoKindLayout(query), "unaccounted")
	require.Nil(t, GetResultColumnsFromPlan(&Plan{Plan: &pb.Plan_Query{Query: query}}),
		"result metadata extraction must fail closed on an invalid passenger layout")

	query.IntoUserVariableKindColumns = []int32{1}
	query.Nodes[0].ProjectList[1].Typ.Id = int32(types.T_int8)
	require.ErrorContains(t, ValidateSelectIntoKindLayout(query), "non-BOOL")
}
