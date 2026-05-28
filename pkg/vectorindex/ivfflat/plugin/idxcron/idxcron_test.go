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

// IVF-FLAT Updatable hook tests. Lifted from the old executor-side
// TestCheckIndexUpdatable (pkg/vectorindex/idxcron/executor_test.go)
// after the lists/nsample heuristic + kmeans_train_percent mutation
// moved here from (*IndexUpdateTaskInfo).checkIndexUpdatable.

package idxcron

import (
	"testing"
	"time"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const oneWeek = 24 * 7 * time.Hour

type updatableCase struct {
	name      string
	jstr      string
	dsize     int64
	nlists    int64
	ts        types.Timestamp
	createdAt types.Timestamp
	expected  bool
}

// updatableCases mirrors the table that drove the old
// TestCheckIndexUpdatable. Cases that previously exercised the
// createdAt+interval gate (now lives in the executor, not the hook)
// are excluded; the hook never sees ticks that haven't passed that
// universal cadence.
func updatableCases() []updatableCase {
	return []updatableCase{
		{
			name:      "dsize < nlist → skip",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1}}}`,
			dsize:     100,
			nlists:    1000,
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			expected:  false,
		},
		{
			name:      "nsample < lower → always reindex",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1}}}`,
			dsize:     1000000,
			nlists:    1000,
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			expected:  true,
		},
		{
			name:      "nsample in middle, no lastUpdateAt → reindex",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}}}`,
			dsize:     1000000,
			nlists:    1000,
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			expected:  true,
		},
		{
			name:      "nsample in middle, lastUpdate 2 weeks ago → reindex",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}}}`,
			dsize:     1000000,
			nlists:    1000,
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			ts:        types.UnixToTimestamp(time.Now().Add(-2 * oneWeek).Unix()),
			expected:  true,
		},
		{
			name:      "nsample in middle, lastUpdate 1h ago → skip",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}}}`,
			dsize:     1000000,
			nlists:    1000,
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			ts:        types.UnixToTimestamp(time.Now().Add(-time.Hour).Unix()),
			expected:  false,
		},
		{
			name:      "nsample upper, lastUpdate 1h ago → skip",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}}}`,
			dsize:     10000000,
			nlists:    1000,
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			ts:        types.UnixToTimestamp(time.Now().Add(-time.Hour).Unix()),
			expected:  false,
		},
		{
			name:      "nsample upper, lastUpdate 2 weeks ago → reindex + mutate",
			jstr:      `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10}}}`,
			dsize:     10000000,
			nlists:    1000,
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			ts:        types.UnixToTimestamp(time.Now().Add(-2 * oneWeek).Unix()),
			expected:  true,
		},
		{
			name:      "empty metadata, lastUpdate 2 weeks ago → reindex",
			jstr:      "",
			dsize:     10000000,
			nlists:    1000,
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * oneWeek).Unix()),
			ts:        types.UnixToTimestamp(time.Now().Add(-2 * oneWeek).Unix()),
			expected:  true,
		},
	}
}

func ivfflatTestTableDef(nlist int64) *plan.TableDef {
	algoParams := `{"lists":"` + intStr(nlist) + `"}`
	return &plan.TableDef{
		DbName: "db",
		Name:   "tbl",
		Indexes: []*plan.IndexDef{
			{
				IndexName:       "ivf_idx",
				IndexAlgoParams: algoParams,
			},
		},
	}
}

func intStr(v int64) string {
	// Faster than fmt.Sprint for fixed positive integers in tests.
	if v == 0 {
		return "0"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestUpdatable(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, ta := range updatableCases() {
		t.Run(ta.name, func(t *testing.T) {
			var m *sqlexec.Metadata
			if len(ta.jstr) > 0 {
				var err error
				m, err = sqlexec.NewMetadataFromJson(ta.jstr)
				require.NoError(t, err)
			}

			stub := gostub.Stub(&RunGetCountSql, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
				require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], ta.dsize, false, mp))
				bat.SetRowCount(1)
				return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
			})
			defer stub.Reset()

			lastUpdate := ta.ts
			ok, _, err := Hooks{}.Updatable(idxcronplugin.UpdatableInput{
				TableDef:     ivfflatTestTableDef(ta.nlists),
				IndexName:    "ivf_idx",
				Metadata:     m,
				CreatedAt:    ta.createdAt,
				LastUpdateAt: &lastUpdate,
				Interval:     oneWeek,
			})
			require.NoError(t, err)
			require.Equal(t, ta.expected, ok)
		})
	}
}

func TestUpdatable_MissingNlist(t *testing.T) {
	// algoParams without the "lists" key → executor's historical
	// behaviour was to surface an error.
	tableDef := &plan.TableDef{
		DbName: "db",
		Name:   "tbl",
		Indexes: []*plan.IndexDef{
			{IndexName: "ivf_idx", IndexAlgoParams: `{}`},
		},
	}

	_, _, err := Hooks{}.Updatable(idxcronplugin.UpdatableInput{
		TableDef:  tableDef,
		IndexName: "ivf_idx",
		Interval:  oneWeek,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "IVFFLAT index parameter LISTS not found")
}
