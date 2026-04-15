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

	bat1 := newDeleteBlockInfoTestBatch(t, proc, objectio.NewObjectStatsWithObjectID(new(objectio.ObjectId), true, true, true))
	defer bat1.Clean(proc.Mp())

	obj2 := types.Objectid{1}
	bat2 := newDeleteBlockInfoTestBatch(t, proc, objectio.NewObjectStatsWithObjectID(&obj2, true, true, true))
	defer bat2.Clean(proc.Mp())

	require.NoError(t, writer.fillDeleteBlockInfo(proc, 0, bat1, 10))
	require.NoError(t, writer.fillDeleteBlockInfo(proc, 0, bat2, 20))

	stats1 := objectio.ObjectStats(bat1.Vecs[0].GetBytesAt(0))
	stats2 := objectio.ObjectStats(bat2.Vecs[0].GetBytesAt(0))

	require.Len(t, writer.deleteBlockInfo[0], 2)
	require.Equal(t, uint64(10), writer.deleteBlockInfo[0][(&stats1).ObjectLocation().String()].rawRowCount)
	require.Equal(t, uint64(20), writer.deleteBlockInfo[0][(&stats2).ObjectLocation().String()].rawRowCount)
}

func newDeleteBlockInfoTestBatch(t *testing.T, proc *process.Process, stats *objectio.ObjectStats) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})
	bat.SetVector(0, vector.NewVec(types.T_binary.ToType()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], stats.Marshal(), false, proc.Mp()))
	bat.SetRowCount(1)
	return bat
}
