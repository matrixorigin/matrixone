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

package test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

// TestLowPrecisionFloatGetUpdateByFilter reads bf16, float16, float8 and float4 values by
// filter in the appending transaction and after commit, and updates one of them while the
// others are read back.
func TestLowPrecisionFloatGetUpdateByFilter(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()
	db := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer db.Close()

	attrs := []string{"id", "a", "b", "c", "d"}
	typs := []types.Type{types.T_int32.ToType(), types.T_bf16.ToType(), types.T_float16.ToType(), types.T_float8.ToType(), types.T_float4.ToType()}
	schema := catalog.NewEmptySchema("lowprec")
	require.NoError(t, schema.AppendPKCol(attrs[0], typs[0], 0))
	for i := 1; i < len(attrs); i++ {
		require.NoError(t, schema.AppendCol(attrs[i], typs[i]))
	}
	require.NoError(t, schema.Finalize(false))

	row := func(f float32) []any {
		return []any{types.BF16FromFloat32(f), types.Float16FromFloat32(f), types.Float8FromFloat32(f), types.Float4FromFloat32(f)}
	}
	bat := containers.BuildBatch(attrs, typs, containers.Options{})
	defer bat.Close()
	for id, f := range []float32{1.5, -2} {
		bat.Vecs[0].Append(int32(id+1), false)
		for i, v := range row(f) {
			bat.Vecs[i+1].Append(v, false)
		}
	}
	read := func(rel handle.Relation, id int32, want []any) {
		for i, w := range want {
			v, isNull, err := rel.GetValueByFilter(ctx, handle.NewEQFilter(id), i+1)
			require.NoError(t, err, attrs[i+1])
			require.False(t, isNull, attrs[i+1])
			require.Equal(t, w, v, attrs[i+1])
		}
	}

	txn, err := db.StartTxn(nil)
	require.NoError(t, err)
	database, err := txn.CreateDatabase("db", "", "")
	require.NoError(t, err)
	rel, err := database.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, rel.Append(ctx, bat))
	read(rel, 1, row(1.5))
	read(rel, 2, row(-2))
	require.NoError(t, txn.Commit(ctx))

	txn, err = db.StartTxn(nil)
	require.NoError(t, err)
	database, err = txn.GetDatabase("db")
	require.NoError(t, err)
	rel, err = database.GetRelationByName(schema.Name)
	require.NoError(t, err)
	read(rel, 1, row(1.5))
	require.NoError(t, rel.UpdateByFilter(ctx, handle.NewEQFilter(int32(1)), 1, types.BF16FromFloat32(3), false))
	want := row(1.5)
	want[0] = types.BF16FromFloat32(3)
	read(rel, 1, want)
	require.NoError(t, txn.Commit(ctx))
}
