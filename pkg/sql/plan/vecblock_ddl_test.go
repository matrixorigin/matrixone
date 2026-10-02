// Copyright 2021 - 2024 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVecBlockDDL(t *testing.T) {
	mock := NewMockOptimizer(false)
	for _, typ := range []string{"vecf8", "vecf4"} {
		runTestShouldPass(mock, t, []string{
			"create table vb_ok (id int primary key, v " + typ + "(4), w " + typ + "(1024))",
			"create table vb_ok2 (id int primary key, v " + typ + "(4) not null)",
		}, false, false)
		for sql, reason := range map[string]string{
			"create table vb_pk (v " + typ + "(4) primary key)":                                                                   "cannot be in primary key",
			"create table vb_pk2 (id int, v " + typ + "(4), primary key (id, v))":                                                 "VECTOR column 'v' cannot be in",
			"create table vb_idx (id int primary key, v " + typ + "(4), index i (v))":                                             "cannot be in index",
			"create table vb_uk (id int primary key, v " + typ + "(4), unique key u (v))":                                         "cannot be in index",
			"create table vb_ivf (id int primary key, v " + typ + "(4), key i using ivfflat (v) lists=2 op_type 'vector_l2_ops')": "cannot be in index",
			"create table vb_hnsw (id bigint primary key, v " + typ + "(4), key i using hnsw (v) op_type 'vector_l2_ops')":        "cannot be in index",
			"create table vb_dim (id int primary key, v " + typ + "(65536))":                                                      "MaxVectorLen",
			"create table vb_dim0 (id int primary key, v " + typ + "(0))":                                                         "cannot be less than 1",
		} {
			_, err := runOneStmt(mock, t, sql)
			require.Error(t, err, sql)
			require.Contains(t, err.Error(), reason, sql)
		}
	}
}

func TestVecBlockAssignmentCastChecksDimension(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		require.True(t, needsSameTypeAssignmentCast(plan.Type{Id: int32(oid), Width: 4}))
		require.False(t, needsSameTypeAssignmentCast(plan.Type{Id: int32(oid), Width: types.MaxArrayDimension}))
	}
}
