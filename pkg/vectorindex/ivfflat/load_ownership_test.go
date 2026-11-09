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

package ivfflat

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type ownedCentroidFixture struct {
	cache.VectorIndexSearchIf
	loadErr      error
	loadPanic    any
	destroyPanic any
	destroys     int
}

func (f *ownedCentroidFixture) Load(*sqlexec.SqlProcess) error {
	if f.loadPanic != nil {
		panic(f.loadPanic)
	}
	return f.loadErr
}

func (f *ownedCentroidFixture) Destroy() {
	f.destroys++
	if f.destroyPanic != nil {
		panic(f.destroyPanic)
	}
}

func TestIvfflatLoadTransfersCentroidOwnership(t *testing.T) {
	loadErr := moerr.NewInvalidStateNoCtx("centroid load failed")
	metadataErr := moerr.NewInvalidStateNoCtx("metadata failed")
	cases := []struct {
		name          string
		loadErr       error
		loadPanic     any
		metadataErr   error
		metadataPanic any
		destroyPanic  any
		wantErr       error
		wantPanic     any
	}{
		{name: "centroid error", loadErr: loadErr, destroyPanic: "cleanup panic", wantErr: loadErr},
		{name: "centroid panic", loadPanic: "load panic", destroyPanic: "cleanup panic", wantPanic: "load panic"},
		{name: "metadata error", metadataErr: metadataErr, destroyPanic: "cleanup panic", wantErr: metadataErr},
		{name: "metadata panic", metadataPanic: "metadata panic", destroyPanic: "cleanup panic", wantPanic: "metadata panic"},
		{name: "success"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			previous := runSql
			t.Cleanup(func() { runSql = previous })
			m := mpool.MustNewZero()
			calls := 0
			runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
				calls++
				if calls == 1 {
					bat := batch.NewWithSize(2)
					t.Cleanup(func() { bat.Clean(m) })
					bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					bat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(0), false, m))
					require.NoError(t, vector.AppendBytes(bat.Vecs[1], types.ArrayToBytes([]float32{1, 2}), false, m))
					bat.SetRowCount(1)
					return executor.Result{Mp: m, Batches: []*batch.Batch{bat}}, nil
				}
				if tc.metadataPanic != nil {
					panic(tc.metadataPanic)
				}
				return executor.Result{}, tc.metadataErr
			}
			fake := &ownedCentroidFixture{loadErr: tc.loadErr, loadPanic: tc.loadPanic, destroyPanic: tc.destroyPanic}
			factory := func(vectors [][]float32, _ uint, _ metric.MetricType, _ uint, _ uint, _ bool) (cache.VectorIndexSearchIf, error) {
				require.Equal(t, [][]float32{{1, 2}}, vectors)
				return fake, nil
			}
			cfg := vectorindex.IndexConfig{}
			cfg.Ivfflat.Lists = 1
			cfg.Ivfflat.Dimensions = 2
			cfg.Ivfflat.VectorType = int32(types.T_array_int8)
			search := NewIvfflatSearch[float32](cfg, vectorindex.IndexTableConfig{})
			proc := &sqlexec.SqlProcess{}
			if tc.wantPanic != nil {
				require.PanicsWithValue(t, tc.wantPanic, func() { _ = search.load(proc, factory) })
			} else {
				err := search.load(proc, factory)
				if tc.wantErr != nil {
					require.Same(t, tc.wantErr, err)
				} else {
					require.NoError(t, err)
				}
			}
			require.Zero(t, m.CurrNB(), "SQL result must be closed")
			if tc.wantErr != nil || tc.wantPanic != nil {
				require.Nil(t, search.Index)
				require.Equal(t, 1, fake.destroys)
				search.Destroy()
				require.Equal(t, 1, fake.destroys, "cache cleanup must not destroy the candidate twice")
				return
			}
			require.NotNil(t, search.Index)
			require.Same(t, fake, search.Index.Centroids)
			require.Zero(t, fake.destroys)
			search.Destroy()
			search.Destroy()
			require.Equal(t, 1, fake.destroys)
		})
	}
}
