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

package tables

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func TestAppendTNBatchVectorsValidationMatrix(t *testing.T) {
	newVector := func(t *testing.T, mp *mpool.MPool, typ types.Type, values ...int64) containers.Vector {
		t.Helper()
		vec := containers.MakeVector(typ, mp)
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(vec.GetDownstreamVector(), value, false, mp))
		}
		return vec
	}
	newPair := func(t *testing.T, dstValues, srcValues []int64) (*mpool.MPool, *containers.Batch, *containers.Batch) {
		t.Helper()
		mp := mpool.MustNewZero()
		dst := containers.NewBatch()
		dst.AddVector("id", newVector(t, mp, types.T_int64.ToType(), dstValues...))
		src := containers.NewBatch()
		src.AddVector("id", newVector(t, mp, types.T_int64.ToType(), srcValues...))
		return mp, dst, src
	}

	t.Run("top-level shape", func(t *testing.T) {
		mp, dst, src := newPair(t, nil, nil)
		defer mpool.DeleteMPool(mp)
		defer dst.Close()
		defer src.Close()
		_, err := appendTNBatchVectorsAtomic(nil, src.Attrs, src.Vecs, mp)
		require.ErrorContains(t, err, "schema is inconsistent")
		_, err = appendTNBatchVectorsAtomic(dst, nil, src.Vecs, mp)
		require.ErrorContains(t, err, "schema is inconsistent")
	})

	t.Run("nil leading source", func(t *testing.T) {
		mp, dst, src := newPair(t, nil, nil)
		defer mpool.DeleteMPool(mp)
		defer dst.Close()
		src.Vecs[0].Close()
		src.Vecs[0] = nil
		_, err := appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, mp)
		require.ErrorContains(t, err, "nil leading")
	})

	t.Run("source length mismatch", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		dst := containers.NewBatch()
		defer dst.Close()
		dst.AddVector("id", newVector(t, mp, types.T_int64.ToType()))
		dst.AddVector("value", newVector(t, mp, types.T_int64.ToType()))
		src := containers.NewBatch()
		defer src.Close()
		src.AddVector("id", newVector(t, mp, types.T_int64.ToType(), 1))
		src.AddVector("value", newVector(t, mp, types.T_int64.ToType(), 1, 2))
		_, err := appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, mp)
		require.ErrorContains(t, err, "inconsistent length")
	})

	t.Run("missing destination mapping", func(t *testing.T) {
		mp, dst, src := newPair(t, nil, []int64{1})
		defer mpool.DeleteMPool(mp)
		defer dst.Close()
		defer src.Close()
		_, err := appendTNBatchVectorsAtomic(dst, []string{"missing"}, src.Vecs, mp)
		require.ErrorContains(t, err, "missing column")
	})

	t.Run("incompatible type", func(t *testing.T) {
		mp, dst, src := newPair(t, nil, nil)
		defer mpool.DeleteMPool(mp)
		defer dst.Close()
		src.Close()
		src = containers.NewBatch()
		defer src.Close()
		src.AddVector("id", newVector(t, mp, types.T_uint64.ToType()))
		_, err := appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, mp)
		require.ErrorContains(t, err, "incompatible source")
	})

	t.Run("aliased source", func(t *testing.T) {
		mp, dst, src := newPair(t, nil, nil)
		defer mpool.DeleteMPool(mp)
		defer dst.Close()
		src.Close()
		_, err := appendTNBatchVectorsAtomic(dst, dst.Attrs, dst.Vecs, mp)
		require.ErrorContains(t, err, "incompatible source")
	})

	t.Run("empty append", func(t *testing.T) {
		mp, dst, src := newPair(t, nil, nil)
		defer mpool.DeleteMPool(mp)
		defer dst.Close()
		defer src.Close()
		offset, err := appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, mp)
		require.NoError(t, err)
		require.Zero(t, offset)
		require.Zero(t, dst.Length())
	})

	t.Run("duplicate destination mapping", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		dst := containers.NewBatch()
		defer dst.Close()
		dst.AddVector("a", newVector(t, mp, types.T_int64.ToType()))
		dst.AddVector("b", newVector(t, mp, types.T_int64.ToType()))
		dst.Attrs[1] = "a"
		sources := []containers.Vector{
			newVector(t, mp, types.T_int64.ToType()),
			newVector(t, mp, types.T_int64.ToType()),
		}
		defer sources[0].Close()
		defer sources[1].Close()
		_, err := appendTNBatchVectorsAtomic(dst, []string{"a", "a"}, sources, mp)
		require.ErrorContains(t, err, "multiple sources")
	})

	t.Run("large empty schema", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		dst := containers.NewBatch()
		defer dst.Close()
		src := containers.NewBatch()
		defer src.Close()
		for i := 0; i < 65; i++ {
			attr := fmt.Sprintf("c%d", i)
			dst.AddVector(attr, newVector(t, mp, types.T_int64.ToType()))
			src.AddVector(attr, newVector(t, mp, types.T_int64.ToType()))
		}
		_, err := appendTNBatchVectorsAtomic(dst, src.Attrs, src.Vecs, mp)
		require.NoError(t, err)
	})
}

func TestPersistedNodeGuardContracts(t *testing.T) {
	var node *persistedNode
	_, err := node.Rows()
	require.Error(t, err)
	node = newPersistedNode(nil)
	_, err = node.Rows()
	require.Error(t, err)
	require.True(t, node.IsPersisted())
	require.Error(t, node.Contains(nil, nil, index.ZM(nil), nil, nil))
	require.Error(t, node.GetDuplicatedRows(nil, nil, nil, nil, index.ZM(nil), nil, nil))
	_, err = node.GetDataWindow(nil, nil, 0, 0, nil)
	require.Error(t, err)
}

func TestPersistedScanGuardMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	schema := catalog.MockSchema(1, 0)
	var output *containers.Batch
	var node *persistedNode
	require.Error(t, node.Scan(context.Background(), &output, nil, schema, 0, []int{0}, mp))

	node = &persistedNode{object: &baseObject{}}
	require.Error(t, node.Scan(nil, &output, nil, schema, 0, []int{0}, mp))
	require.Error(t, node.Scan(context.Background(), &output, nil, schema, 0, nil, mp))
	require.Error(t, node.Scan(
		context.Background(), &output, nil, schema, 0,
		[]int{objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, mp,
	))

	emptyNameSchema := catalog.MockSchema(1, 0)
	emptyNameSchema.ColDefs[0].Name = ""
	require.ErrorContains(t, node.Scan(
		context.Background(), &output, nil, emptyNameSchema, 0, []int{0}, mp,
	), "empty attribute")
	require.ErrorContains(t, node.Scan(
		context.Background(), &output, nil, schema, 0, []int{0}, mp,
	), "no object metadata")
}
