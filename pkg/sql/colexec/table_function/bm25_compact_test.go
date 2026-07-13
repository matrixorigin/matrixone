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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestBm25CompactPrepare_WrongArgCount(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.Ctx = context.Background()

	arg := &TableFunction{Args: []*plan.Expr{bm25VarcharCol(), bm25VarcharCol()}} // only 2
	_, err := bm25CompactPrepare(proc, arg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expects 4 args")
}

func TestBm25CompactStart_Errors(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.Ctx = context.Background()

	// argVecs: [0]=db, [1]=store, [2]=meta, [3]=capacity — all const strings on the
	// happy path. Each case corrupts one.
	cases := []struct {
		name    string
		vecs    func() []*vector.Vector
		wantErr string
	}{
		{
			name: "arg not string",
			vecs: func() []*vector.Vector {
				return []*vector.Vector{bm25TestConstInt(m), bm25TestConstVarchar(m, "s"), bm25TestConstVarchar(m, "meta"), bm25TestConstVarchar(m, "1000")}
			},
			wantErr: "must be strings",
		},
		{
			name: "arg not const",
			vecs: func() []*vector.Vector {
				return []*vector.Vector{bm25TestNonConstVarchar(m), bm25TestConstVarchar(m, "s"), bm25TestConstVarchar(m, "meta"), bm25TestConstVarchar(m, "1000")}
			},
			wantErr: "",
		},
		{
			name: "capacity not integer",
			vecs: func() []*vector.Vector {
				return []*vector.Vector{bm25TestConstVarchar(m, "db"), bm25TestConstVarchar(m, "s"), bm25TestConstVarchar(m, "meta"), bm25TestConstVarchar(m, "abc")}
			},
			wantErr: "capacity must be an integer",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			arg := &TableFunction{Args: []*plan.Expr{bm25VarcharCol(), bm25VarcharCol(), bm25VarcharCol(), bm25VarcharCol()}}
			st, err := bm25CompactPrepare(proc, arg)
			require.NoError(t, err)

			arg.ctr.argVecs = tt.vecs()
			err = st.start(arg, proc, 0, nil)
			require.Error(t, err)
			if tt.wantErr != "" {
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
