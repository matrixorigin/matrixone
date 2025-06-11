// Copyright 2024 Matrix Origin
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

package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

func Test_storageUsageDetails(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()

	storage := &storageUsageHistoryArg{
		ctx: &inspectContext{
			db:   tae,
			resp: &cmd_util.InspectResp{},
		},
	}
	err := storageUsageDetails(storage)
	require.NoError(t, err)
}

func TestMergeCommand(t *testing.T) {
	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	asyncTxn, err := handle.db.StartTxn(nil)
	require.NoError(t, err)

	ctx := context.Background()
	database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
	require.NoError(t, err)
	schema := catalog.MockSchema(2, 1)
	schema.Name = "test1"
	table, err := testutil.CreateRelation2(ctx, asyncTxn, database, schema)
	require.NoError(t, err)
	vector := containers.NewVector(types.T_varchar.ToType())
	{
		id := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&id, true, true, false)
		vector.Append(stats.Marshal(), false)

		id = objectio.NewObjectid()
		stats = objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		zm := index.NewZM(types.T_int32, 0)
		v1 := int32(1)
		v2 := int32(2)
		index.UpdateZM(zm, types.EncodeInt32(&v1))
		index.UpdateZM(zm, types.EncodeInt32(&v2))
		objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
		vector.Append(stats.Marshal(), false)

		id = objectio.NewObjectid()
		stats = objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		zm = index.NewZM(types.T_int32, 0)
		v1 = int32(2)
		v2 = int32(3)
		index.UpdateZM(zm, types.EncodeInt32(&v1))
		index.UpdateZM(zm, types.EncodeInt32(&v2))
		objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
		vector.Append(stats.Marshal(), false)
	}
	require.NoError(t, table.AddDataFiles(ctx, vector))
	require.NoError(t, asyncTxn.Commit(context.Background()))

	resp, err := handle.runInspectCmd("merge show")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: true")

	handle.runInspectCmd("merge switch off")
	resp, err = handle.runInspectCmd("merge show")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")

	handle.runInspectCmd("merge switch off -t db1.test1")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")
	require.Contains(t, string(resp.Payload), "auto merge: false")
	require.Contains(t, string(resp.Payload), "Obj(s,c,u): 2-0-0")

	handle.runInspectCmd("merge switch on -t db1.test1")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")
	require.Contains(t, string(resp.Payload), "auto merge: true")

	handle.runInspectCmd("merge switch on")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: true")
	require.Contains(t, string(resp.Payload), "auto merge: true")

	resp, err = handle.runInspectCmd("merge switch notOnOrOff")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge switch on off")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge trigger -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "trigger nothing")

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind l0")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind ln")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind vacuum")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind tombstone")
	require.NoError(t, err)

	resp, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind xx")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind tombstone --tombstone-oneshot --patch-expire 1h")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind l0 --l0-oneshot --patch-expire 1h")
	require.NoError(t, err)

	resp, err = handle.runInspectCmd("merge trace -t 1.2")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge trace on")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "specified")

	resp, err = handle.runInspectCmd("merge trace xx -t 1.2")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	_, err = handle.runInspectCmd("merge trace on -t 1.2")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trace off -t 1.2")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

}
