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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDescribeViewColumnsUsesCurrentDefinition(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	const definition = `{"Stmt":"create view v as select n_name as label, n_nationkey as k from nation","DefaultDatabase":"tpch"}`
	before, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	ctx.tables["nation"].Cols[1].Typ.Width = 60
	source := ctx.tables["nation"].Cols[0]
	source.Typ.Id = int32(types.T_int64)
	source.Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: "7"}
	after, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, "label", after[0].Name)
	require.Equal(t, int32(60), after[0].Typ.Width)
	require.NotEqual(t, before[0].Typ.Width, after[0].Typ.Width)
	require.Equal(t, int32(types.T_int64), after[1].Typ.Id)
	require.Equal(t, "7", after[1].Default.OriginString)
	regenerated, err := RegenerateViewDefinition(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, regenerated.TableDef.Cols, after)
	after[1].Default.OriginString = "changed"
	require.Equal(t, "7", source.Default.OriginString, "caller owns description, not source catalog metadata")
	again, err := DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, "7", again[1].Default.OriginString)
}

func TestDescribeViewColumnsFailureDoesNotCache(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	const definition = `{"Stmt":"create view v as select n_name from nation","DefaultDatabase":"tpch"}`
	source := ctx.tables["nation"]
	delete(ctx.tables, "nation")
	cols, err := DescribeViewColumns(ctx, definition)
	require.Error(t, err)
	require.Nil(t, cols)
	ctx.tables["nation"] = source
	cols, err = DescribeViewColumns(ctx, definition)
	require.NoError(t, err)
	require.Len(t, cols, 1)
}

func TestDescribeViewColumnsRejectsInvalidInputAndCancellation(t *testing.T) {
	for _, definition := range []string{`{`, `{"Stmt":"select 1"}`, `{"Stmt":"create view v as select 1; select 2"}`} {
		ctx := NewMockCompilerContext(false)
		cols, err := DescribeViewColumns(ctx, definition)
		require.Error(t, err)
		require.Nil(t, cols)
	}
	ctx := NewMockCompilerContext(false)
	canceled, cancel := context.WithCancel(ctx.GetContext())
	cancel()
	ctx.SetContext(canceled)
	cols, err := DescribeViewColumns(ctx, `{"Stmt":"create view v as select n_name from nation","DefaultDatabase":"tpch"}`)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, cols)
}
