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

package productl2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type ownedBruteForceFixture struct {
	cache.VectorIndexSearchIf
	loadErr      error
	loadPanic    any
	destroyPanic any
	loads        int
	destroys     int
}

func (f *ownedBruteForceFixture) Load(*sqlexec.SqlProcess) error {
	f.loads++
	if f.loadPanic != nil {
		panic(f.loadPanic)
	}
	return f.loadErr
}

func (f *ownedBruteForceFixture) Destroy() {
	f.destroys++
	if f.destroyPanic != nil {
		panic(f.destroyPanic)
	}
}

func TestGetIndexTransfersOwnership(t *testing.T) {
	createErr := moerr.NewInvalidStateNoCtx("constructor failed")
	loadErr := moerr.NewInvalidStateNoCtx("load failed")
	for _, tc := range []struct {
		name         string
		createErr    error
		loadErr      error
		loadPanic    any
		destroyPanic any
	}{
		{name: "constructor error", createErr: createErr},
		{name: "load error", loadErr: loadErr},
		{name: "load error and cleanup panic", loadErr: loadErr, destroyPanic: "cleanup panic"},
		{name: "load panic", loadPanic: "load panic"},
		{name: "load panic and cleanup panic", loadPanic: "load panic", destroyPanic: "cleanup panic"},
		{name: "success"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			bat := batch.NewWithSize(1)
			t.Cleanup(func() { bat.Clean(mp) })
			typ := types.T_array_float32.ToType()
			typ.Width = 2
			bat.Vecs[0] = vector.NewVec(typ)
			require.NoError(t, vector.AppendArrayList(bat.Vecs[0], [][]float32{{1, 2}}, nil, mp))
			bat.SetRowCount(1)

			ap := &Productl2{OnExpr: onExprWithProbeCol(0)}
			ap.ctr.bat = bat
			ap.ctr.metrictype = metric.Metric_L2sqDistance
			ap.ctr.sqlproc = &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{
				ResolveVariableFunc: func(string, bool, bool) (any, error) { return int8(1), nil },
			}}
			fake := &ownedBruteForceFixture{loadErr: tc.loadErr, loadPanic: tc.loadPanic, destroyPanic: tc.destroyPanic}
			factory := func(centers [][]float32, dim uint, m metric.MetricType, elemSize, nthread uint, gpu bool) (cache.VectorIndexSearchIf, error) {
				require.Equal(t, [][]float32{{1, 2}}, centers)
				require.EqualValues(t, 2, dim)
				require.Equal(t, metric.Metric_L2sqDistance, m)
				require.EqualValues(t, 4, elemSize)
				require.EqualValues(t, 1, nthread)
				require.True(t, gpu)
				if tc.createErr != nil {
					return nil, tc.createErr
				}
				return fake, nil
			}

			var err error
			load := func() {
				ap.ctr.brute_force, err = getIndex(ap, make([][]float32, 1), make([]float32, 2), factory)
			}
			if tc.loadPanic != nil {
				require.PanicsWithValue(t, tc.loadPanic, load)
			} else {
				load()
				if tc.createErr != nil {
					require.Same(t, tc.createErr, err)
				} else if tc.loadErr != nil {
					require.Same(t, tc.loadErr, err)
				} else {
					require.NoError(t, err)
				}
			}

			if tc.createErr != nil {
				require.Nil(t, ap.ctr.brute_force)
				require.Zero(t, fake.loads)
				require.Zero(t, fake.destroys)
			} else {
				require.Equal(t, 1, fake.loads)
				if tc.loadErr != nil || tc.loadPanic != nil {
					require.Nil(t, ap.ctr.brute_force)
					require.Equal(t, 1, fake.destroys)
				} else {
					require.Same(t, fake, ap.ctr.brute_force)
					require.Zero(t, fake.destroys)
				}
			}
			proc := testutil.NewProcessWithMPool(t, "", mp)
			ap.Reset(proc, false, nil)
			ap.Free(proc, false, nil)
			if tc.createErr != nil {
				require.Zero(t, fake.destroys)
			} else {
				require.Equal(t, 1, fake.destroys)
			}
		})
	}
}
