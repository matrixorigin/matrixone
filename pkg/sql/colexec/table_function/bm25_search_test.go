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

// bm25TestConstVarchar / …ConstInt / …NonConstVarchar are shared vector builders for
// the bm25 TVF start()-error tests.
func bm25TestConstVarchar(m *mpool.MPool, s string) *vector.Vector {
	v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(s), 1, m)
	if err != nil {
		panic(err)
	}
	return v
}

func bm25TestConstInt(m *mpool.MPool) *vector.Vector {
	v, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, m)
	if err != nil {
		panic(err)
	}
	return v
}

func bm25TestNonConstVarchar(m *mpool.MPool) *vector.Vector {
	v := vector.NewVec(types.T_varchar.ToType())
	if err := vector.AppendBytes(v, []byte(`{}`), false, m); err != nil {
		panic(err)
	}
	return v
}

func bm25VarcharCol() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{}}, Typ: plan.Type{Id: int32(types.T_varchar)}}
}

func TestBm25SearchStart_Errors(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.Ctx = context.Background()

	cases := []struct {
		name    string
		cfg     *vector.Vector
		pat     *vector.Vector
		wantErr string
	}{
		{"cfg not string", bm25TestConstInt(m), bm25TestConstVarchar(m, "apple"), "must be a string"},
		{"cfg not const", bm25TestNonConstVarchar(m), bm25TestConstVarchar(m, "apple"), "must be a string constant"},
		{"cfg empty", bm25TestConstVarchar(m, ""), bm25TestConstVarchar(m, "apple"), "config is empty"},
		{"cfg bad json", bm25TestConstVarchar(m, `{`), bm25TestConstVarchar(m, "apple"), ""},
		{"pattern not string", bm25TestConstVarchar(m, `{}`), bm25TestConstInt(m), "must be a string"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			arg := &TableFunction{Args: []*plan.Expr{bm25VarcharCol(), bm25VarcharCol()}}
			st, err := bm25SearchPrepare(proc, arg)
			require.NoError(t, err)

			arg.ctr.argVecs = make([]*vector.Vector, 2)
			arg.ctr.argVecs[0] = tt.cfg
			arg.ctr.argVecs[1] = tt.pat

			err = st.start(arg, proc, 0, nil)
			require.Error(t, err)
			if tt.wantErr != "" {
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
