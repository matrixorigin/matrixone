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

package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBatchDataStringWidthCheck(t *testing.T) {
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{
				Name: "vc",
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 3},
			},
		},
	}

	require.NoError(t, BatchDataStringWidthCheck(
		[]*vector.Vector{testutil.MakeVarcharVector([]string{"abc", "你好世"}, nil)},
		[]string{"vc"},
		tableDef,
		context.Background(),
	))

	err := BatchDataStringWidthCheck(
		[]*vector.Vector{testutil.MakeVarcharVector([]string{"abcd"}, nil)},
		[]string{"vc"},
		tableDef,
		context.Background(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Data too long for column 'vc' at row 1")
}
