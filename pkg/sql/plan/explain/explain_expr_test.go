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

package explain

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDescribeExprEscapesNonTextBytes(t *testing.T) {
	options := NewExplainDefaultOptions()
	for _, tc := range []struct{ value, want string }{
		{"hello 世界", "'hello 世界'"},
		{string([]byte{'a', 0, '\n', 0xff}), `'a\x00\n\xff'`},
	} {
		var out bytes.Buffer
		expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tc.value}}}}
		require.NoError(t, describeExpr(context.Background(), expr, options, &out))
		require.Equal(t, tc.want, out.String())
	}

	mp := mpool.MustNew(t.Name())
	values := vector.NewVec(types.T_varchar.ToType())
	defer func() { values.Free(mp); require.Zero(t, mp.CurrNB()) }()
	require.NoError(t, vector.AppendBytes(values, []byte{'u', 0, 0xff}, false, mp))
	require.NoError(t, vector.AppendBytes(values, []byte("plain"), false, mp))
	data, err := values.MarshalBinary()
	require.NoError(t, err)
	expr := &plan.Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 2, Data: data}}}
	var out bytes.Buffer
	require.NoError(t, describeExpr(context.Background(), expr, options, &out))
	require.Equal(t, `[u\x00\xff plain]`, out.String())
}
