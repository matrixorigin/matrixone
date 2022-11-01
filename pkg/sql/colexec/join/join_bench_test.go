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

package join

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/index"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const (
	benchFlag        = false
	benchCardinality = 1024

	benchBuildTargetRows = 2_500
	benchProbeTargetRows = 5_000
)

var (
	benchBuildData  []string
	benchProbeData  []string
	benchBuildIndex *index.LowCardinalityIndex
	benchProbeIndex *index.LowCardinalityIndex
)

func init() {
	if !benchFlag {
		return
	}

	// init benchBuildData
	benchBuildData = testutil.MakeRandomStrings(benchCardinality, benchBuildTargetRows)

	// init benchProbeData
	for len(benchProbeData) < benchProbeTargetRows {
		// there is 1/3 probability picking a not existed string
		if rand.Intn(3) == 0 {
			benchProbeData = append(benchProbeData, "hello_world") // '_' is not existed
		} else {
			src := benchBuildData[rand.Intn(len(benchBuildData))]
			benchProbeData = append(benchProbeData, string(append([]byte{}, src...)))
		}
	}

	// init benchBuildIndex and benchProbeIndex
	var err error
	if benchBuildIndex, err = initBenchIndex(benchBuildData); err != nil {
		panic(err)
	}
	if benchProbeIndex, err = initBenchIndex(benchProbeData); err != nil {
		panic(err)
	}
}

func initBenchIndex(values []string) (*index.LowCardinalityIndex, error) {
	var err error
	m := mpool.MustNewZero()
	v := testutil.NewVector(len(values), types.T_varchar.ToType(), m, false, values)
	idx, err := index.New(v.Typ, m)
	if err != nil {
		return nil, err
	}
	if err = idx.InsertBatch(v); err != nil {
		return nil, err
	}
	return idx, nil
}

func TestJoinPerf(t *testing.T) {
	if !benchFlag {
		t.Log("benchmark flag is turned off...")
		return
	}

	const testCnt = 10

	metricMp := map[string][]int64{
		"totalCost":              make([]int64, testCnt),
		"constructBuildDataCost": make([]int64, testCnt),
		"hashbuildCost":          make([]int64, testCnt),
		"constructProbeDataCost": make([]int64, testCnt),
		"probeCost":              make([]int64, testCnt),
	}
	avgMp := map[string]int64{
		"totalCost":              0,
		"constructBuildDataCost": 0,
		"hashbuildCost":          0,
		"constructProbeDataCost": 0,
		"probeCost":              0,
	}

	//------------
	// common join
	//------------
	{
		t.Log("---------- test common join performance ----------")
		for i := 0; i < testCnt; i++ {
			mockTimingCase(t, metricMp, i, nil, nil)
		}

		metric, err := json.Marshal(metricMp)
		require.NoError(t, err)
		t.Log(string(metric))

		avg := calcAvg(t, testCnt, metricMp, avgMp)
		t.Logf("common join performance average: \n%s\n", avg)
	}

	t.Logf("\n")

	//---------------------
	// low cardinality join
	//---------------------
	{
		t.Log("---------- test low cardinality join performance ----------")
		for i := 0; i < testCnt; i++ {
			mockTimingCase(t, metricMp, i, nil, benchBuildIndex.Dup())
		}

		metric, err := json.Marshal(metricMp)
		require.NoError(t, err)
		t.Log(string(metric))

		avg := calcAvg(t, testCnt, metricMp, avgMp)
		t.Logf("low cardinality join performance average: \n%v\n", avg)
	}

	t.Logf("\n")

	//-----------------------------
	// low cardinality indexes join
	//-----------------------------
	{
		t.Log("---------- test low cardinality indexes join performance ----------")
		for i := 0; i < testCnt; i++ {
			mockTimingCase(t, metricMp, i, benchProbeIndex.Dup(), benchBuildIndex.Dup())
		}

		metric, err := json.Marshal(metricMp)
		require.NoError(t, err)
		t.Log(string(metric))

		avg := calcAvg(t, testCnt, metricMp, avgMp)
		t.Logf("low cardinality indexes join performance average: \n%v\n", avg)
	}
}

func mockTimingCase(t *testing.T, metricMp map[string][]int64, pos int, probeIdx, buildIdx *index.LowCardinalityIndex) {
	totalStart := time.Now().UnixNano()
	tc := newTestCase([]bool{false}, []types.Type{types.T_varchar.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{newExpr(0, types.T_varchar.ToType())},
			{newExpr(0, types.T_varchar.ToType())},
		})
	tc.arg.Cond = nil

	// construct build data part
	constructBuildDataStart := time.Now().UnixNano()
	v0 := testutil.NewVector(len(benchBuildData), types.T_varchar.ToType(), tc.proc.Mp(), false, benchBuildData)
	if buildIdx != nil {
		v0.SetIndex(buildIdx)
	}
	constructBuildDataEnd := time.Now().UnixNano()
	metricMp["constructBuildDataCost"][pos] = constructBuildDataEnd - constructBuildDataStart

	// hashbuild part
	hashbuildStart := time.Now().UnixNano()
	bat := hashBuildWithBatch(t, tc, testutil.NewBatchWithVectors([]*vector.Vector{v0}, nil))
	hashbuildEnd := time.Now().UnixNano()
	metricMp["hashbuildCost"][pos] = hashbuildEnd - hashbuildStart

	// construct probe data part
	constructProbeDataStart := time.Now().UnixNano()
	v1 := testutil.NewVector(len(benchProbeData), types.T_varchar.ToType(), tc.proc.Mp(), false, benchProbeData)
	if probeIdx != nil {
		v1.SetIndex(probeIdx)
	}
	constructProbeDataEnd := time.Now().UnixNano()
	metricMp["constructProbeDataCost"][pos] = constructProbeDataEnd - constructProbeDataStart

	// probe part
	probeStart := time.Now().UnixNano()
	rbat := probeWithBatches(t, tc, testutil.NewBatchWithVectors([]*vector.Vector{v1}, nil), bat)
	require.NotNil(t, rbat)
	probeEnd := time.Now().UnixNano()
	metricMp["probeCost"][pos] = probeEnd - probeStart

	totalEnd := time.Now().UnixNano()
	metricMp["totalCost"][pos] = totalEnd - totalStart
}

func calcAvg(t *testing.T, cnt int64, perf map[string][]int64, avg map[string]int64) string {
	for k, data := range perf {
		sum := int64(0)
		for _, v := range data {
			sum += v
		}
		avg[k] = sum / cnt
	}

	res, err := json.Marshal(avg)
	require.NoError(t, err)
	return string(res)
}
