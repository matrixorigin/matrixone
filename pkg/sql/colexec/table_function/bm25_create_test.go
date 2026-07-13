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

package table_function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// bm25CreateArg builds a TableFunction with three varchar Args (cfg, word, doc_id),
// the postings-mode shape bm25_create expects.
func bm25CreateArg() *TableFunction {
	col := func() *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{}},
			Typ:  plan.Type{Id: int32(types.T_varchar)},
		}
	}
	return &TableFunction{Args: []*plan.Expr{col(), col(), col()}}
}

func TestBm25CreateStart_Errors(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.Ctx = context.Background()

	constCfg := func(s string) *vector.Vector {
		v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(s), 1, m)
		require.NoError(t, err)
		return v
	}
	constVarchar := func(s string) *vector.Vector { return constCfg(s) }
	constInt := func() *vector.Vector {
		v, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, m)
		require.NoError(t, err)
		return v
	}
	nonConstVarchar := func() *vector.Vector {
		v := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(v, []byte(`{}`), false, m))
		return v
	}

	cases := []struct {
		name    string
		cfg     *vector.Vector
		word    *vector.Vector
		wantErr string
	}{
		{"cfg not string", constInt(), constVarchar("w"), "must be a string"},
		{"cfg not const", nonConstVarchar(), constVarchar("w"), "must be a string constant"},
		{"cfg empty", constCfg(""), constVarchar("w"), "config is empty"},
		{"cfg bad json", constCfg(`{`), constVarchar("w"), ""},
		{"word not string", constCfg(`{}`), constInt(), "must be a string"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			arg := bm25CreateArg()
			st, err := bm25CreatePrepare(proc, arg)
			require.NoError(t, err)

			arg.ctr.argVecs = make([]*vector.Vector, 3)
			arg.ctr.argVecs[0] = tt.cfg
			arg.ctr.argVecs[1] = tt.word
			arg.ctr.argVecs[2] = constVarchar("doc1")

			err = st.start(arg, proc, 0, nil)
			require.Error(t, err)
			if tt.wantErr != "" {
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
