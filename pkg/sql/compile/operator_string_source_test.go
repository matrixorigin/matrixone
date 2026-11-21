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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestConstructValueScanRejectsInvalidLiteralStringSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, rawSource := range []uint32{257, ^uint32(0)} {
		node := &plan.Node{
			NodeType: plan.Node_VALUE_SCAN,
			TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
				Typ: plan.Type{Id: int32(types.T_varchar)},
			}}},
			RowsetData: &plan.RowsetData{Cols: []*plan.ColData{{Data: []*plan.RowsetExpr{{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value:        &plan.Literal_Sval{Sval: "invalid"},
						StringSource: rawSource,
					}},
				},
			}}}}},
		}
		op, err := constructValueScan(proc, node)
		require.Nil(t, op)
		require.ErrorContains(t, err, "invalid literal string source")
	}
}
