// Copyright 2021 Matrix Origin
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

package tables

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/stretchr/testify/require"
)

func evolutionSchemas(t *testing.T) (*catalog.Schema, *catalog.Schema) {
	t.Helper()
	old, current := catalog.NewEmptySchema("oracle"), catalog.NewEmptySchema("oracle")
	for _, schema := range []*catalog.Schema{old, current} {
		require.NoError(t, schema.AppendCol("id", types.T_int64.ToType()))
	}
	require.NoError(t, old.AppendCol("dropped", types.T_int64.ToType()))
	require.NoError(t, current.AppendColWithAttribute(engine.Attribute{Name: "flag", Seqnum: 2, Type: types.T_bool.ToType(),
		Default: &plan.Default{NullAbility: true, HasOriginDefault: true,
			OriginDefault: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}},
			Expr:          &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}}}))
	current.Extra.FeatureFlag = features.SchemaEvolved
	current.Extra.NextColSeqnum = 3
	require.NoError(t, old.Finalize(false))
	require.NoError(t, current.Finalize(false))
	return old, current
}

func TestSchemaEvolutionMemoryNodeOracle(t *testing.T) {
	old, current := evolutionSchemas(t)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	rt := dbutils.NewRuntime(dbutils.WithRuntimeTransientPool(containers.NewVectorPool("oracle", 0, containers.WithMPool(mp))))
	makeInput := func(t *testing.T, id int64, isNull bool, schema *catalog.Schema) *containers.Batch {
		bat := containers.BuildBatch(schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: mp})
		t.Cleanup(bat.Close)
		bat.GetVectorByName("id").Append(id, false)
		bat.GetVectorByName(catalog.PhyAddrColumnName).Append(nil, true)
		if schema == current {
			bat.GetVectorByName("flag").Append(true, isNull)
		} else {
			bat.GetVectorByName("dropped").Append(int64(99), false)
		}
		return bat
	}
	t.Run("old node scan in both branches", func(t *testing.T) {
		node := &memoryNode{object: &baseObject{rt: rt}, writeSchema: old}
		defer node.mustData().Close()
		_, err := node.ApplyAppendLocked(makeInput(t, 2, false, old))
		require.NoError(t, err)
		var out *containers.Batch
		require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 0, 1, mp))
		defer out.Close()
		require.Equal(t, int64(2), out.Vecs[0].Get(0))
		require.Equal(t, false, out.Vecs[1].Get(0))
		require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 0, 1, mp))
		require.Equal(t, 2, out.Length())
		require.Equal(t, int64(2), out.Vecs[0].Get(1))
		require.Equal(t, false, out.Vecs[1].Get(1))
	})
	for _, mode := range []string{"append", "out-of-order WAL payload"} {
		t.Run(mode, func(t *testing.T) {
			node := &memoryNode{object: &baseObject{rt: rt}, writeSchema: current}
			defer node.mustData().Close()
			oldRow := makeInput(t, 2, false, old)
			require.Equal(t, len(current.ColDefs), len(oldRow.Attrs), "DROP and ADD leave the WAL width unchanged")
			newRow := makeInput(t, 3, false, current)
			nullRow := makeInput(t, 4, true, current)
			if mode == "append" {
				for _, row := range []*containers.Batch{oldRow, newRow, nullRow} {
					_, err := node.ApplyAppendLocked(row)
					require.NoError(t, err)
				}
			} else {
				require.NoError(t, node.ApplyAppendAtLocked(newRow, 1))
				require.NoError(t, node.ApplyAppendAtLocked(oldRow, 0))
				require.NoError(t, node.ApplyAppendAtLocked(nullRow, 2))
			}
			var out *containers.Batch
			require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 0, 1, mp))
			defer out.Close()
			require.NoError(t, node.getDataWindowLocked(&out, current, []int{0, 1}, 1, 3, mp))
			require.Equal(t, 3, out.Length())
			require.Equal(t, []int64{2, 3, 4}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0].GetDownstreamVector()))
			require.Equal(t, false, out.Vecs[1].Get(0))
			require.Equal(t, true, out.Vecs[1].Get(1))
			require.True(t, out.Vecs[1].IsNull(2))
		})
	}
}

func TestSchemaEvolutionWALFillFailurePreservesRows(t *testing.T) {
	schema := catalog.NewEmptySchema("positional_replay")
	idType, textType := types.T_int64.ToType(), types.T_text.ToType()
	require.NoError(t, schema.AppendCol("id", idType))
	require.NoError(t, schema.AppendColWithAttribute(engine.Attribute{Name: "flag", Type: types.T_bool.ToType(),
		Default: &plan.Default{HasOriginDefault: true, OriginDefault: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}))
	require.NoError(t, schema.AppendColWithAttribute(engine.Attribute{Name: "memo", Type: textType,
		Default: &plan.Default{HasOriginDefault: true, OriginDefault: &plan.Literal{Value: &plan.Literal_Sval{Sval: strings.Repeat("x", 64*1024)}}}}))
	schema.Extra.FeatureFlag = features.SchemaEvolved
	require.NoError(t, schema.Finalize(false))

	mp, err := mpool.NewMPool("positional-wal-test", 1024*1024, mpool.NoFixed)
	require.NoError(t, err)
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	data := containers.BuildBatch(schema.AllNames(), schema.AllTypes(), containers.Options{Allocator: mp})
	defer data.Close()
	node := &memoryNode{writeSchema: schema, data: data}
	data.GetVectorByName("id").Append(int64(7), false)
	data.GetVectorByName("flag").Append(true, false)
	data.GetVectorByName("memo").Append([]byte("unchanged"), false)
	data.GetVectorByName(catalog.PhyAddrColumnName).Append(nil, true)
	old := containers.BuildBatch([]string{"id"}, []types.Type{idType}, containers.Options{Allocator: mp})
	defer old.Close()
	for range 32 {
		old.Vecs[0].Append(int64(99), false)
	}
	before := mp.CurrNB()
	err = node.ApplyAppendAtLocked(old, 0)
	require.True(t, mpool.IsMPoolCapacityFailure(err), "expected the repeated varlena default copies to exceed the pool: %v", err)
	require.Equal(t, before, mp.CurrNB())
	for _, vec := range data.Vecs {
		require.Equal(t, 1, vec.Length())
	}
	require.Equal(t, int64(7), data.GetVectorByName("id").Get(0))
	require.Equal(t, true, data.GetVectorByName("flag").Get(0))
	require.Equal(t, []byte("unchanged"), data.GetVectorByName("memo").Get(0))
}

func TestSchemaEvolutionPersistedNodeOracle(t *testing.T) {
	_, schema := evolutionSchemas(t)
	ctx := t.Context()
	fs, err := fileservice.NewLocalFS(ctx, "oracle", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	rt := dbutils.NewRuntime(dbutils.WithRuntimeObjectFS(fs), dbutils.WithRuntimeTransientPool(
		containers.NewVectorPool("oracle", 0, containers.WithMPool(mp))))
	cat := catalog.MockCatalog(nil)
	defer cat.Close()
	db, err := cat.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	tbl, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, mp))
	input.SetRowCount(1)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	obj, err := tbl.CreateObject(nil, &objectio.CreateObjOpt{Stats: &stats}, nil)
	require.NoError(t, err)
	base := &baseObject{rt: rt}
	base.meta.Store(obj)
	node := newPersistedNode(base)
	for _, mode := range []struct {
		name     string
		borrowed bool
	}{{"scan", false}, {"merge scan", true}} {
		t.Run(mode.name, func(t *testing.T) {
			readCtx := ctx
			if mode.borrowed {
				readCtx = WithScanNoCopy(ctx)
			}
			var out *containers.Batch
			require.NoError(t, node.Scan(readCtx, &out, nil, schema, 0, []int{0, 1}, mp))
			defer out.Close()
			require.Equal(t, 1, out.Length())
			require.Equal(t, int64(1), out.Vecs[0].Get(0))
			require.Equal(t, false, out.Vecs[1].Get(0))
		})
	}
}
