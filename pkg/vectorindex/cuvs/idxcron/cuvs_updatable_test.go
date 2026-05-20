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

package idxcron

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const (
	testIndexName  = "test_idx"
	testTableName  = "tab"
	testDbName     = "db"
	testVecColName = "v"
	testDim        = 4
)

// buildTestTableDef constructs a TableDef with one vector column and a
// single storage IndexDef matching tblType + carrying the given
// algoParams. dim is the declared vector width.
func buildTestTableDef(tblType, algoParams string, dim int32) *plan.TableDef {
	return &plan.TableDef{
		DbName: testDbName,
		Name:   testTableName,
		Cols: []*plan.ColDef{
			{Name: testVecColName, Typ: plan.Type{Id: int32(types.T_array_float32), Width: dim}},
		},
		Name2ColIndex: map[string]int32{testVecColName: 0},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          testIndexName,
				IndexTableName:     "__mo_storage",
				IndexAlgoTableType: tblType,
				IndexAlgoParams:    algoParams,
				Parts:              []string{testVecColName},
			},
		},
	}
}

// chunkBytesWithRecords builds a single tag=1 chunk frame carrying n
// DELETE records (the smallest, 9 bytes each — sufficient to test
// counting without committing to a vec/include encoding).
func chunkBytesWithRecords(t *testing.T, n int) []byte {
	t.Helper()
	var records []byte
	for i := 0; i < n; i++ {
		rec, err := vectorindex.EncodeEventRecord(nil, vectorindex.CdcOpDelete, int64(i+1), nil, nil, testDim, 0)
		require.NoError(t, err)
		records = append(records, rec...)
	}
	return vectorindex.FrameCdcChunk(records)
}

// stubSelect returns a runSelectChunkSql replacement that yields a
// single-row batch whose `data` column holds the supplied framed
// chunk bytes. mp is the test mpool the caller owns and frees.
func stubSelect(t *testing.T, mp *mpool.MPool, framed [][]byte) func(*sqlexec.SqlProcess, string) (executor.Result, error) {
	return func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 0, 0))
		for _, b := range framed {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], b, false, mp))
		}
		bat.SetRowCount(len(framed))
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
}

func TestCuvsUpdatable_EmptySpec(t *testing.T) {
	tableDef := buildTestTableDef(catalog.Cagra_TblType_Storage, `{}`, testDim)
	_, _, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must set StorageTableType and ThresholdParam")
}

func TestCuvsUpdatable_IndexDefMissing(t *testing.T) {
	tableDef := buildTestTableDef("__some_other_type", `{}`, testDim)
	_, _, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no IndexDef found")
}

func TestCuvsUpdatable_ThresholdMissing(t *testing.T) {
	// algoParams has no IntermediateGraphDegree key → threshold reads as 0.
	tableDef := buildTestTableDef(catalog.Cagra_TblType_Storage, `{}`, testDim)
	ok, reason, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.Contains(t, reason, "missing or non-positive")
}

func TestCuvsUpdatable_ThresholdNonInt(t *testing.T) {
	// Threshold is the wrong type — surfaces as an error.
	algoParams := fmt.Sprintf(`{"%s":"not-an-int"}`, catalog.IntermediateGraphDegree)
	tableDef := buildTestTableDef(catalog.Cagra_TblType_Storage, algoParams, testDim)
	_, _, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not int64")
}

func TestCuvsUpdatable_BelowThreshold(t *testing.T) {
	const threshold = 128
	mp := mpool.MustNewZero()

	algoParams := fmt.Sprintf(`{"%s":%d}`, catalog.IntermediateGraphDegree, threshold)
	tableDef := buildTestTableDef(catalog.Cagra_TblType_Storage, algoParams, testDim)

	// 5 records, threshold 128 → not enough delta to rebuild.
	stub := gostub.Stub(&runSelectChunkSql, stubSelect(t, mp, [][]byte{chunkBytesWithRecords(t, 5)}))
	defer stub.Reset()

	ok, reason, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.Contains(t, reason, "CDC delta records 5 < threshold 128")
}

func TestCuvsUpdatable_AtOrAboveThreshold(t *testing.T) {
	const threshold = 4
	mp := mpool.MustNewZero()

	algoParams := fmt.Sprintf(`{"%s":%d}`, catalog.IntermediateGraphDegree, threshold)
	tableDef := buildTestTableDef(catalog.Cagra_TblType_Storage, algoParams, testDim)

	// 6 records across 2 chunks → comfortably above threshold 4.
	stub := gostub.Stub(&runSelectChunkSql, stubSelect(t, mp, [][]byte{
		chunkBytesWithRecords(t, 4),
		chunkBytesWithRecords(t, 2),
	}))
	defer stub.Reset()

	ok, reason, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, reason)
}

func TestCuvsUpdatable_EmptyTag1(t *testing.T) {
	const threshold = 1
	mp := mpool.MustNewZero()

	algoParams := fmt.Sprintf(`{"%s":%d}`, catalog.IntermediateGraphDegree, threshold)
	tableDef := buildTestTableDef(catalog.Cagra_TblType_Storage, algoParams, testDim)

	// Zero rows returned from SELECT — count is 0, threshold is 1.
	stub := gostub.Stub(&runSelectChunkSql, stubSelect(t, mp, nil))
	defer stub.Reset()

	ok, reason, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Cagra_TblType_Storage,
		ThresholdParam:   catalog.IntermediateGraphDegree,
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, strings.HasPrefix(reason, "CDC delta records 0 < threshold 1"))
}

func TestCuvsUpdatable_IvfpqShape(t *testing.T) {
	// Mirror IVF-PQ wiring: lists key + IVF-PQ storage type.
	const threshold = 2
	mp := mpool.MustNewZero()

	algoParams := fmt.Sprintf(`{"%s":%d}`, catalog.IndexAlgoParamLists, threshold)
	tableDef := buildTestTableDef(catalog.Ivfpq_TblType_Storage, algoParams, testDim)

	stub := gostub.Stub(&runSelectChunkSql, stubSelect(t, mp, [][]byte{chunkBytesWithRecords(t, 3)}))
	defer stub.Reset()

	ok, _, err := CuvsUpdatable(nil, tableDef, testIndexName, CuvsUpdatableSpec{
		StorageTableType: catalog.Ivfpq_TblType_Storage,
		ThresholdParam:   catalog.IndexAlgoParamLists,
	})
	require.NoError(t, err)
	require.True(t, ok)
}
