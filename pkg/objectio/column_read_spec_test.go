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

package objectio_test

import (
	"context"
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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func specTestPool(t *testing.T) *mpool.MPool {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	return mp
}

func TestColumnReadSpecEmptyMetadata(t *testing.T) {
	typ := types.T_bool.ToType()
	spec := objectio.NewColumnReadSpecFromTableDef(&plan.TableDef{FeatureFlag: features.SchemaEvolved, Cols: []*plan.ColDef{
		{Seqnum: 0, Typ: typ.PlanType()},
		{Seqnum: 2, Typ: typ.PlanType()},
	}})
	checkEmpty := func(col objectio.ColumnMeta, err error) {
		t.Helper()
		require.NoError(t, err)
		require.False(t, col.ZoneMap().IsInited())
		require.Equal(t, types.T_bool, col.ZoneMap().GetType())
		require.Zero(t, col.Ndv())
		require.Zero(t, col.NullCnt())
	}
	meta := objectio.BuildObjectMeta(1)
	meta.BlockHeader().SetMetaColumnCount(1)
	require.Zero(t, meta.BlockCount())
	for _, seq := range []uint16{0, 2} {
		checkEmpty(spec.VirtualColumnMeta(seq, 0))
		checkEmpty(spec.ResolveColumnMeta(meta, seq))
		checkEmpty(spec.ResolveColumnMeta(objectio.ObjectDataMeta(nil), seq))
	}
}

func TestColumnReadSpecResolution(t *testing.T) {
	typ := types.T_bool.ToType()
	def := &plan.TableDef{Cols: []*plan.ColDef{
		{Seqnum: 2, Typ: typ.PlanType(), Default: &plan.Default{NullAbility: true, Expr: &plan.Expr{
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}},
		}}},
		{Seqnum: 3, Typ: typ.PlanType()},
	}}
	require.Nil(t, objectio.NewColumnReadSpecFromTableDef(def))
	def.FeatureFlag = features.SchemaEvolved
	spec := objectio.NewColumnReadSpecFromTableDef(def)
	require.Equal(t, objectio.ColPhysical, spec.Classify(2, 2, true))
	require.Equal(t, objectio.ColVirtualDefault, spec.Classify(2, 1, false))
	require.Equal(t, objectio.ColCorruptedMissing, spec.Classify(2, 2, false))
	block := objectio.BuildBlockMeta(3)
	header := block.BlockHeader()
	header.SetMaxSeqnum(1)
	header.SetMetaColumnCount(3)
	header.SetRows(3)
	mp := specTestPool(t)
	for _, seq := range []uint16{2, 3} {
		col, err := spec.ResolveColumnMeta(block, seq)
		require.NoError(t, err)
		require.Equal(t, uint32(3), col.NullCnt())
		require.Zero(t, col.Ndv())
		require.False(t, col.ZoneMap().IsInited())
		vec, err := spec.MaterializeMissingVector(seq, typ, 3, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		require.True(t, vec.IsConstNull())
	}
	_, err := spec.ResolveColumnMeta(block, 5)
	require.ErrorContains(t, err, "absent from the read schema")
	header.SetMaxSeqnum(2)
	_, err = spec.ResolveColumnMeta(block, 2)
	require.ErrorContains(t, err, "missing within block layout")
	fetcher := spec.WrapColumnMetaFetcher(block)
	_ = fetcher.MustGetColumn(2)
	require.ErrorContains(t, fetcher.Err(), "missing within block layout")
}

func TestColumnReadSpecLocalFS(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewLocalFS(ctx, "column-read-spec", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := specTestPool(t)
	intType, strType := types.T_int64.ToType(), types.T_varchar.ToType()
	arrayType := types.New(types.T_array_float32, 2, 0)
	bat := batch.NewWithSize(2)
	bat.Vecs[0], bat.Vecs[1] = vector.NewVec(intType, mp), vector.NewVec(intType, mp)
	defer bat.Clean(mp)
	for row := range 4 {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(row+10), false, mp))
	}
	bat.SetRowCount(4)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0, 1}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	loc := objectio.BuildLocation(name, blocks[0].GetExtent(), 4, 0)
	meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
	require.NoError(t, err)
	dataMeta := meta.MustDataMeta()

	pb := &plan.TableDef{FeatureFlag: features.SchemaEvolved, Cols: []*plan.ColDef{
		{Seqnum: 0, Typ: intType.PlanType()}, {Seqnum: 1, Typ: intType.PlanType()},
		{Seqnum: 2, Typ: intType.PlanType()},
		{Seqnum: 3, Typ: strType.PlanType()},
		{Seqnum: 4, Typ: intType.PlanType()},
		{Seqnum: 5, Typ: arrayType.PlanType()},
	}}
	spec := objectio.NewColumnReadSpecFromTableDef(pb)
	ctx = objectio.WithColumnReadSpec(ctx, spec)
	cols := []uint16{0, 2, 3, 4}
	typs := []types.Type{intType, intType, strType, intType}
	assertRows := func(t *testing.T, vecs []*vector.Vector, ids []int64) {
		t.Helper()
		for i, id := range ids {
			require.Equal(t, id, vector.GetFixedAtNoTypeCheck[int64](vecs[0], i))
			require.True(t, vecs[1].IsNull(uint64(i)))
			require.True(t, vecs[2].IsNull(uint64(i)))
			require.True(t, vecs[3].IsNull(uint64(i)))
		}
		for _, vec := range vecs {
			require.Equal(t, len(ids), vec.Length())
		}
	}
	t.Run("ReadOneBlock", func(t *testing.T) {
		before := mp.CurrNB()
		io, err := objectio.ReadOneBlock(ctx, &dataMeta, name.String(), 0, cols, typs, mp, fs, 0)
		require.NoError(t, err)
		defer io.Release()
		require.Equal(t, before, mp.CurrNB(), "serialization keeps no query-pool bytes")
		vecs := make([]*vector.Vector, len(cols))
		for i := range vecs {
			vecs[i] = vector.NewVec(typs[i], mp)
			defer vecs[i].Free(mp)
			require.NoError(t, objectio.MustVectorToCached(vecs[i], io.Entries[i].CachedData))
		}
		require.True(t, vecs[1].IsConst())
		require.True(t, vecs[2].IsConst())
		assertRows(t, vecs, []int64{1, 2, 3, 4})
	})
	t.Run("LoadColumns", func(t *testing.T) {
		cache := containers.NewVectors(len(cols))
		release, _, err := ioutil.LoadColumns(ctx, cols, typs, fs, loc, cache, mp, 0)
		require.NoError(t, err)
		defer release()
		vecs := make([]*vector.Vector, len(cols))
		for i := range vecs {
			vecs[i] = &cache[i]
		}
		assertRows(t, vecs, []int64{1, 2, 3, 4})
	})
	t.Run("LoadColumnsData2 without vector pool", func(t *testing.T) {
		wrapped, release, err := ioutil.LoadColumnsData2(ctx, cols, typs, fs, loc, 0, false, nil)
		require.NoError(t, err)
		defer release()
		vecs := make([]*vector.Vector, len(wrapped))
		for i, vec := range wrapped {
			vecs[i] = vec
		}
		assertRows(t, vecs, []int64{1, 2, 3, 4})
	})
	for _, selected := range [][]int64{nil, {3, 1, 1}} {
		t.Run("LoadColumnsDataInto", func(t *testing.T) {
			vecs := make([]*vector.Vector, len(cols))
			for i, typ := range typs {
				vecs[i] = vector.NewVec(typ, mp)
				defer vecs[i].Free(mp)
			}
			mask, _, err := ioutil.LoadColumnsDataInto(ctx, cols, typs, fs, loc, vecs, selected, nil, mp, 0)
			require.NoError(t, err)
			defer mask.Release()
			ids := []int64{1, 2, 3, 4}
			if selected != nil {
				ids = []int64{4, 2, 2}
			}
			assertRows(t, vecs, ids)
		})
	}
	t.Run("TopN", func(t *testing.T) {
		dst := vector.NewVec(strType, mp)
		defer dst.Free(mp)
		top := &objectio.IndexReaderTopOp{Typ: types.T_array_float32, MetricType: metric.Metric_L2sqDistance,
			NumVec: types.ArrayToBytes([]float32{0, 0}), Limit: 2}
		rows, distances, _, err := objectio.ReadBlockBySearchAndTopN(ctx, []uint16{2}, []types.Type{intType}, []uint16{3}, []types.Type{strType}, []*vector.Vector{dst},
			5, arrayType, func(filters []vector.Vector) ([]int64, error) {
				require.Equal(t, 4, filters[0].Length())
				for i := range 4 {
					require.True(t, filters[0].IsNull(uint64(i)))
				}
				return []int64{1, 2, 3}, nil
			}, top, fs, loc, mp, 0)
		require.NoError(t, err)
		require.Empty(t, rows)
		require.Empty(t, distances)
		require.Zero(t, dst.Length())
		rows, _, _, err = objectio.ReadColumnTopN(ctx, 5, arrayType, fs, loc, nil, &objectio.IndexReaderTopOp{
			Typ: types.T_array_float32, MetricType: metric.Metric_L2sqDistance, NumVec: top.NumVec, Limit: 2}, mp, 0)
		require.NoError(t, err)
		require.Empty(t, rows)
	})
	t.Run("without-spec", func(t *testing.T) {
		ctx := objectio.WithoutColumnReadSpec(ctx)
		require.Nil(t, objectio.ColumnReadSpecFromContext(ctx))
		cache := containers.NewVectors(1)
		_, release, err := ioutil.LoadTombstoneColumns(ctx, []uint16{2}, []types.Type{intType}, fs, loc, cache, mp, 0)
		require.NoError(t, err)
		defer release()
		require.True(t, cache[0].IsConstNull())
	})
	t.Run("ReadDeletes strips the data spec", func(t *testing.T) {
		tombstone := batch.NewWithSize(2)
		tombstone.Vecs[0], tombstone.Vecs[1] = vector.NewVec(objectio.RowidType, mp), vector.NewVec(intType, mp)
		defer tombstone.Clean(mp)
		rowID := types.RandomRowid()
		require.NoError(t, vector.AppendFixed(tombstone.Vecs[0], rowID, false, mp))
		require.NoError(t, vector.AppendFixed(tombstone.Vecs[1], int64(2), false, mp))
		tombstone.SetRowCount(1)
		writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
		_, err := writer.WriteBatch(tombstone)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		stats := writer.GetObjectStats()
		cache := containers.NewVectors(2)
		assertFS := &specAssertingFS{FileService: fs, t: t}
		_, release, err := ioutil.ReadDeletes(ctx, stats.ObjectLocation(), assertFS, true, cache, &intType)
		require.NoError(t, err)
		defer release()
		require.True(t, assertFS.read)
		require.Equal(t, rowID, vector.GetFixedAtNoTypeCheck[types.Rowid](&cache[0], 0))
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](&cache[1], 0))
	})
}

type specAssertingFS struct {
	fileservice.FileService
	t    *testing.T
	read bool
}

func (fs *specAssertingFS) Read(ctx context.Context, io *fileservice.IOVector) error {
	fs.read = true
	require.Nil(fs.t, objectio.ColumnReadSpecFromContext(ctx))
	return fs.FileService.Read(ctx, io)
}
