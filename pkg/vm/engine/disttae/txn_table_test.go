// Copyright 2023 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
)

func newTxnTableForTest(
	mp *mpool.MPool,
) *txnTable {
	engine := &Engine{
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker(mp)
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.FreeMem()
			},
		),
	}
	var dnStore DNStore
	txn := &Transaction{
		engine:   engine,
		dnStores: []DNStore{dnStore},
	}
	db := &txnDatabase{
		txn: txn,
	}
	table := &txnTable{
		db:         db,
		primaryIdx: 0,
	}
	return table
}

func makeBatchForTest(
	mp *mpool.MPool,
	ints ...int64,
) *batch.Batch {
	bat := batch.New(false, []string{"a"})
	vec := vector.NewVec(types.T_int64.ToType())
	for _, n := range ints {
		vector.AppendFixed(vec, n, false, mp)
	}
	bat.SetVector(0, vec)
	bat.SetZs(len(ints), mp)
	return bat
}

func TestPrimaryKeyCheck(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	getRowIDsBatch := func(table *txnTable) *batch.Batch {
		bat := batch.New(false, []string{catalog.Row_ID})
		vec := vector.NewVec(types.T_Rowid.ToType())
		iter := table.localState.NewRowsIter(
			types.TimestampToTS(table.nextLocalTS()),
			nil,
			false,
		)
		l := 0
		for iter.Next() {
			entry := iter.Entry()
			vector.AppendFixed(vec, entry.RowID, false, mp)
			l++
		}
		iter.Close()
		bat.SetVector(0, vec)
		bat.SetZs(l, mp)
		return bat
	}

	table := newTxnTableForTest(mp)

	// insert
	err := table.Write(
		ctx,
		makeBatchForTest(mp, 1),
	)
	assert.Nil(t, err)

	// insert duplicated
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 1),
	)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

	// insert no duplicated
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 2, 3),
	)
	assert.Nil(t, err)

	// duplicated in same batch
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 4, 4),
	)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

	table = newTxnTableForTest(mp)

	// insert, delete then insert
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 1),
	)
	assert.Nil(t, err)
	err = table.Delete(
		ctx,
		getRowIDsBatch(table),
		catalog.Row_ID,
	)
	assert.Nil(t, err)
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 5),
	)
	assert.Nil(t, err)

}

func BenchmarkTxnTableInsert(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	table := newTxnTableForTest(mp)
	for i, max := int64(0), int64(b.N); i < max; i++ {
		err := table.Write(
			ctx,
			makeBatchForTest(mp, i),
		)
		assert.Nil(b, err)
	}
}

func TestBuildColumnVector(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)
	minv := int32(-10)
	maxv := int32(10)
	vector.AppendFixed[int32](vec, 1, false, mp)
	vector.AppendFixed[int32](vec, 2, false, mp)
	vector.AppendFixed[int32](vec, -2, false, mp)
	vector.AppendFixed[int32](vec, 3, false, mp)
	vector.AppendFixed(vec, minv, false, mp)
	vector.AppendFixed[int32](vec, 9, false, mp)
	vector.AppendFixed(vec, maxv, false, mp)

	zm := objectio.NewZM(vec.GetType().Oid, vec.GetType().Scale)
	zm.Update(minv)
	zm.Update(maxv)
	t.Log(zm.String())

	vec2, err := buildColumnZMVector(*zm, mp)
	assert.NoError(t, err)
	defer vec2.Free(mp)
	assert.Equal(t, minv, vector.GetFixedAt[int32](vec2, 0))
	assert.Equal(t, maxv, vector.GetFixedAt[int32](vec2, 1))
}

func BenchmarkBuildColumnZMVector(b *testing.B) {
	mp := mpool.MustNewZero()
	zm := objectio.NewZM(types.T_int64, 0)
	zm.Update(int64(-100))
	zm.Update(int64(99))

	b.Run("build-column-zm-vec", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec, _ := buildColumnZMVector(*zm, mp)
			vec.Free(mp)
		}
	})
}
