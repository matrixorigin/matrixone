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

package group

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/index"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const (
	benchFlag        = false
	benchCardinality = 1024

	benchTargetRows = 5_000
)

var (
	benchGroupData  []string
	benchGroupIndex *index.LowCardinalityIndex
)

func init() {
	if !benchFlag {
		return
	}

	benchGroupData = testutil.MakeRandomStrings(benchCardinality, benchTargetRows)

	var err error
	if benchGroupIndex, err = initBenchIndex(benchGroupData); err != nil {
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

func TestGroupPerf(t *testing.T) {
	if !benchFlag {
		t.Log("benchmark flag is turned off...")
		return
	}

	const testCnt = 10

	metricMp := map[string][]int64{
		"totalCost":              make([]int64, testCnt),
		"constructGroupDataCost": make([]int64, testCnt),
		"groupbyCost":            make([]int64, testCnt),
	}
	avgMp := map[string]int64{
		"totalCost":              0,
		"constructGroupDataCost": 0,
		"groupbyCost":            0,
	}

	//----------------
	// common group by
	//----------------
	{
		t.Log("---------- test common group by performance ----------")
		for i := 0; i < testCnt; i++ {
			mockTimingCase(t, metricMp, i, nil)
		}

		metric, err := json.Marshal(metricMp)
		require.NoError(t, err)
		t.Logf("common group by performance metric: \n%s\n", string(metric))

		avg := calcAvg(t, testCnt, metricMp, avgMp)
		t.Logf("common group by performance average: \n%s\n", avg)
	}

	t.Logf("\n")

	//-------------------------
	// low cardinality group by
	//-------------------------
	{
		t.Log("---------- test low cardinality group by performance ----------")
		for i := 0; i < testCnt; i++ {
			mockTimingCase(t, metricMp, i, benchGroupIndex.Dup())
		}

		metric, err := json.Marshal(metricMp)
		require.NoError(t, err)
		t.Logf("low cardinality group by performance metric: \n%s\n", string(metric))

		avg := calcAvg(t, testCnt, metricMp, avgMp)
		t.Logf("low cardinality group by performance average: \n%s\n", avg)
	}
}

func mockTimingCase(t *testing.T, metricMp map[string][]int64, pos int, idx *index.LowCardinalityIndex) {
	totalStart := time.Now().UnixNano()
	// SELECT COUNT(*) FROM t GROUP BY t.col
	tc := newTestCase([]bool{false}, []types.Type{{Oid: types.T_varchar}},
		[]*plan.Expr{newExpression(0)}, []agg.Aggregate{{Op: 5, E: newExpression(0)}})

	// construct group data part
	constructGroupDataStart := time.Now().UnixNano()
	v := testutil.NewVector(len(benchGroupData), types.T_varchar.ToType(), tc.proc.Mp(), false, benchGroupData)
	if idx != nil {
		v.SetIndex(idx)
	}
	constructGroupDataEnd := time.Now().UnixNano()
	metricMp["constructGroupDataCost"][pos] = constructGroupDataEnd - constructGroupDataStart

	// group by part
	groupbyStart := time.Now().UnixNano()
	err := Prepare(tc.proc, tc.arg)
	require.NoError(t, err)
	tc.proc.Reg.InputBatch = testutil.NewBatchWithVectors([]*vector.Vector{v}, nil)
	_, err = Call(0, tc.proc, tc.arg)
	require.NoError(t, err)
	groupbyEnd := time.Now().UnixNano()
	metricMp["groupbyCost"][pos] = groupbyEnd - groupbyStart

	if tc.proc.Reg.InputBatch != nil {
		tc.proc.Reg.InputBatch.Clean(tc.proc.Mp())
	}

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
