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

package hnsw

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// metaBatch builds a metadata batch with the four original columns, optionally followed by the
// appended nrow / build_ts.
func metaBatch(t *testing.T, mp *mpool.MPool, withProvenance bool, nrow, buildTS int64) *batch.Batch {
	t.Helper()
	n := 4
	if withProvenance {
		n = 6
	}
	bat := batch.NewWithSize(n)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("m0"), false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("chk"), false, mp))
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[2], 111, false, mp))
	bat.Vecs[3] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[3], 2222, false, mp))
	if withProvenance {
		bat.Vecs[4] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](bat.Vecs[4], nrow, false, mp))
		bat.Vecs[5] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed[int64](bat.Vecs[5], buildTS, false, mp))
	}
	bat.SetRowCount(1)
	return bat
}

// loadMetaFrom drives the REAL LoadMetadata over a stubbed catalog read. Decoding a private
// copy of the loop instead would leave search.go's own Vecs[4]/Vecs[5] reads executed by no
// test: every other hnsw mock builds a four-column batch, so swapping the two indices there
// would keep the suite green while Nrow silently became a build timestamp -- and GetIndexSize
// estimates the pre-load cost as Nrow*8, so the governor would reclaim against ~1.4e16 bytes
// and evict the whole host cache on every hnsw miss. cagra and ivfpq feed 6-, 5- and 4-column
// batches through their real LoadMetadata; this is the hnsw equivalent.
func loadMetaFrom(t *testing.T, bat *batch.Batch, mp *mpool.MPool) []*HnswModel[float32] {
	t.Helper()
	old := runSql
	t.Cleanup(func() { runSql = old })
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	idxs, err := LoadMetadata[float32](nil, "db", "meta")
	require.NoError(t, err)
	return idxs
}

// An index created before nrow/build_ts existed still has a four-column metadata table -- the
// table is created per index at CREATE INDEX and REINDEX rewrites its rows, not the table -- so
// the reader must not index past the end. Absent means UNKNOWN, never "empty" or "the epoch".
func TestLoadMetadataToleratesLegacyFourColumnShape(t *testing.T) {
	mp := mpool.MustNewZero()

	legacy := loadMetaFrom(t, metaBatch(t, mp, false, 0, 0), mp)
	require.Len(t, legacy, 1)
	require.EqualValues(t, 0, legacy[0].Nrow, "unknown, not zero rows")
	require.EqualValues(t, 0, legacy[0].BuildTS, "unknown, not the epoch")
	require.Equal(t, "m0", legacy[0].Id, "the original columns still decode")
	require.EqualValues(t, 2222, legacy[0].FileSize, "and so does the rest of the legacy shape")

	current := loadMetaFrom(t, metaBatch(t, mp, true, 4321, 999888777), mp)
	require.Len(t, current, 1)
	require.EqualValues(t, 4321, current[0].Nrow, "nrow comes from Vecs[4], not Vecs[5]")
	require.EqualValues(t, 999888777, current[0].BuildTS, "build_ts from Vecs[5], not Vecs[4]")
}

// The sync records the version supplied by the ISCP consumer -- the applied change range's upper
// bound -- rather than its own transaction timestamp, which is later and would overstate what the
// generation covers.
func TestSyncBuildTSIsSuppliedByTheConsumer(t *testing.T) {
	s := &HnswSync[float32]{}
	require.EqualValues(t, 0, s.buildTS, "unknown until a consumer supplies one")
	s.SetBuildTS(1234567890)
	require.EqualValues(t, 1234567890, s.buildTS)
}
