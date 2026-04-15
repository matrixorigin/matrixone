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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFillDeleteBlockInfoKeepsObjectsSeparated(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	defer proc.Free()

	writer := &s3WriterDelegate{
		deleteBlockInfo: make([]map[string]*deleteBlockInfo, 1),
	}

	stats1 := objectio.NewObjectStatsWithObjectID(new(objectio.ObjectId), true, true, true)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 10))
	obj2 := types.Objectid{1}
	stats2 := objectio.NewObjectStatsWithObjectID(&obj2, true, true, true)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats2, 20))

	bat := newDeleteBlockInfoTestBatch(t, proc, stats1, stats2)
	defer bat.Clean(proc.Mp())

	require.NoError(t, writer.fillDeleteBlockInfo(proc, 0, bat, 30))

	stats1Val := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(0))
	stats2Val := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(1))

	require.Len(t, writer.deleteBlockInfo[0], 2)
	require.Equal(t, uint64(10), writer.deleteBlockInfo[0][(&stats1Val).ObjectLocation().String()].rawRowCount)
	require.Equal(t, uint64(20), writer.deleteBlockInfo[0][(&stats2Val).ObjectLocation().String()].rawRowCount)
}

func newDeleteBlockInfoTestBatch(t *testing.T, proc *process.Process, stats ...*objectio.ObjectStats) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})
	bat.SetVector(0, vector.NewVec(types.T_binary.ToType()))
	for _, stat := range stats {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], stat.Marshal(), false, proc.Mp()))
	}
	bat.SetRowCount(len(stats))
	return bat
}
