// Copyright 2025 Matrix Origin
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

package native

import (
	"context"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func TestObjectIndexerBuildAndReadSidecar(t *testing.T) {
	schema := catalog.NewEmptySchema("fts_native_test")
	require.NoError(t, schema.AppendPKCol("id", types.T_int64.ToType(), 0))
	require.NoError(t, schema.AppendCol("body", types.T_varchar.ToType()))

	cstrDef := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: "id",
					Names:       []string{"id"},
				},
			},
			&engine.IndexDef{
				Indexes: []*pbplan.IndexDef{{
					IndexName:       "idx_body",
					IndexTableName:  "__idx_body",
					IndexAlgo:       pkgcatalog.MOIndexFullTextAlgo.ToString(),
					Parts:           []string{"body"},
					IndexAlgoParams: `{"parser":"default"}`,
				}},
			},
		},
	}
	var err error
	schema.Constraint, err = cstrDef.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, schema.Finalize(false))

	mp := mpool.MustNewZero()

	idVec := vector.NewVec(types.T_int64.ToType())
	bodyVec := vector.NewVec(types.T_varchar.ToType())
	defer idVec.Free(mp)
	defer bodyVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int64](idVec, 2, false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("Matrix Origin native fulltext"), false, mp))
	require.NoError(t, vector.AppendBytes(bodyVec, []byte("native search sidecar"), false, mp))

	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"id", "body"}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = bodyVec
	bat.SetRowCount(2)

	indexer, err := NewObjectIndexer(schema)
	require.NoError(t, err)
	require.False(t, indexer.Empty())
	require.NoError(t, indexer.AddBatch(bat, []uint32{2}))

	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	objID := objectio.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(&objID)
	require.NoError(t, indexer.Write(context.Background(), fs, objName))

	seg, ok, err := ReadSidecar(context.Background(), fs, objName, "__idx_body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(2), seg.DocCount)
	require.Equal(t, int64(7), seg.TokenSum)
	require.Len(t, seg.Lookup("native"), 2)
	require.Len(t, seg.Lookup("matrix"), 1)
}
