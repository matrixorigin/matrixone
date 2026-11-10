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

package test

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Repeated MergeObjects tasks on one engine must not park their large
// per-column serialization buffer in the shared WriteArena pool.
func TestMergeObjectsDoesNotRetainLargeSerialBuffer(t *testing.T) {
	ctx := context.Background()
	baselinePooled := promtestutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	defer tae.Close()

	schema := catalog.NewEmptySchema("merge_serial_retention")
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	payloadType := types.T_varchar.ToType()
	payloadType.Width = 4096
	require.NoError(t, schema.AppendCol("payload", payloadType))
	schema.Extra.BlockMaxRows = 8192
	schema.Extra.ObjectMaxBlocks = 2
	require.NoError(t, schema.Finalize(false))
	tae.BindSchema(schema)

	const rows = 8192
	payload := bytes.Repeat([]byte("x"), 2304) // >16 MiB when serialized as one column
	bat := containers.NewBatch()
	bat.AddVector("id", containers.NewVector(types.T_int32.ToType()))
	bat.AddVector("payload", containers.NewVector(payloadType))
	defer bat.Close()
	for i := 0; i < rows; i++ {
		bat.Vecs[0].Append(int32(i), false)
		bat.Vecs[1].Append(payload, false)
	}

	id := objectio.NewObjectid()
	writer, err := ioutil.NewBlockWriterNew(tae.Runtime.Fs, objectio.BuildObjectNameWithObjectID(&id), 0, nil, false)
	require.NoError(t, err)
	writer.SetPrimaryKey(0)
	_, err = writer.WriteBatch(containers.ToCNBatch(bat))
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	statsVec := containers.MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	statsVec.Append(stats[:], false)
	defer statsVec.Close()

	txn, err := tae.StartTxn(nil)
	require.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	require.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, rel.AddDataFiles(ctx, statsVec))
	require.NoError(t, txn.Commit(ctx))

	for round := 0; round < 2; round++ {
		txn, rel := tae.GetRelation()
		meta := testutil.GetOneBlockMeta(rel)
		task, err := jobs.NewMergeObjectsTask(nil, txn, []*catalog.ObjectEntry{meta}, tae.Runtime, 128<<20, false)
		require.NoError(t, err)
		require.NoError(t, task.OnExec(ctx))
		require.NoError(t, txn.Commit(ctx))
		pooled := promtestutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge)
		require.LessOrEqual(t, pooled-baselinePooled, float64(16<<20), "round %d retained %g extra pooled bytes", round, pooled-baselinePooled)
	}
	tae.CheckRowsByScan(rows, true)
}
