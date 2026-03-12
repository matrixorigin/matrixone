// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestIvfCreateStart_Error(t *testing.T) {
	proc := testutil.NewProc(t)
	defaultTblCfg, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`{"db_name":"db", "table_name":"t", "index_table_name":"__mo_index_centroid_556f61743332", "key_part":"v", "datasize": 100000, "kmeans_train_percent": 20}`), 1, proc.Mp())
	require.NoError(t, err)

	emptySqlFn := func(p *sqlexec.SqlProcess, sql string) (executor.Result, error) { return executor.Result{}, nil }

	testCases := []struct {
		name    string
		params  string
		tblCfg  *vector.Vector
		sqlFn   func(p *sqlexec.SqlProcess, sql string) (executor.Result, error)
		wantErr string
	}{
		{
			name:    "Invalid params json",
			params:  `{"lists":"1"`,
			wantErr: "Syntax error",
		},
		{
			name:    "Missing lists",
			params:  `{"optype":"l2"}`,
			tblCfg:  defaultTblCfg,
			sqlFn:   emptySqlFn,
			wantErr: "invalid lists must be > 0",
		},
		{
			name:    "Invalid lists",
			params:  `{"lists":"abc", "op_type":"vector_l2_ops"}`,
			tblCfg:  defaultTblCfg,
			sqlFn:   emptySqlFn,
			wantErr: "invalid syntax",
		},
		{
			name:    "Invalid optype",
			params:  `{"lists":"1", "op_type":"invalid"}`,
			tblCfg:  defaultTblCfg,
			sqlFn:   emptySqlFn,
			wantErr: "invalid optype",
		},
		{
			name:   "Invalid tblcfg type",
			params: `{"lists":"1", "op_type":"vector_l2_ops"}`,
			tblCfg: func() *vector.Vector {
				v, err := vector.NewConstFixed(types.T_int64.ToType(), int64(123), 1, proc.Mp())
				require.NoError(t, err)
				return v
			}(),
			sqlFn:   emptySqlFn,
			wantErr: "First argument (IndexTableConfig must be a string",
		},
		{
			name:   "Empty tblcfg",
			params: `{"lists":"1", "op_type":"vector_l2_ops"}`,
			tblCfg: func() *vector.Vector {
				v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(``), 1, proc.Mp())
				require.NoError(t, err)
				return v
			}(),
			sqlFn:   emptySqlFn,
			wantErr: "IndexTableConfig is empty",
		},
		{
			name:   "SQL error",
			params: `{"lists":"1", "op_type":"vector_l2_ops"}`,
			tblCfg: defaultTblCfg,
			sqlFn: func(p *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				return executor.Result{}, fmt.Errorf("sql error")
			},
			wantErr: "sql error",
		},
		{
			name:   "Unsupported vector type",
			params: `{"lists":"1", "op_type":"vector_l2_ops"}`,
			tblCfg: defaultTblCfg,
			sqlFn: func(p *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				bat := batch.New([]string{"v"})
				vec := vector.NewVec(types.T_int32.ToType())
				vector.AppendFixed(vec, int32(1), false, p.Proc.Mp())
				bat.SetVector(0, vec)
				bat.SetRowCount(1)
				return executor.Result{Mp: p.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
			},
			wantErr: "vector must be a vecf32 or vecf64 type",
		},
		{
			name:   "Dimension mismatch",
			params: `{"lists":"1", "op_type":"vector_l2_ops"}`,
			tblCfg: defaultTblCfg,
			sqlFn: func(p *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				bat := batch.New([]string{"v"})
				typ := types.T_array_float32.ToType()
				typ.Width = 2
				vec := vector.NewVec(typ)
				vector.AppendBytes(vec, types.ArrayToBytes([]float32{1.0, 2.0}), false, p.Proc.Mp())
				vector.AppendBytes(vec, types.ArrayToBytes([]float32{3.0}), false, p.Proc.Mp())
				bat.SetVector(0, vec)
				bat.SetRowCount(2)
				return executor.Result{Mp: p.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
			},
			wantErr: "vector dimension mismatch",
		},
	} // THIS CLOSING BRACE WAS MISSING

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			oldRunSql := ivf_runSql
			defer func() {
				ivf_runSql = oldRunSql
			}()
			ivf_runSql = tt.sqlFn

			m := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", m)
			proc.Ctx = context.Background()

			arg := &TableFunction{
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{},
						},
						Typ: plan.Type{
							Id: int32(types.T_varchar),
						},
					},
				},
				Params: []byte(tt.params),
			}
			st, err := ivfCreatePrepare(proc, arg)
			require.NoError(t, err)

			// Manually set the arg vectors as if they were evaluated
			arg.ctr.argVecs = make([]*vector.Vector, 1) // Ensure argVecs is initialized
			arg.ctr.argVecs[0] = tt.tblCfg

			err = st.start(arg, proc, 0, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
