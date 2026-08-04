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

package lifecycle

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/stretchr/testify/require"
)

func TestBuildLifecycleSyncProtectionFilterAndResponse(t *testing.T) {
	stats := objectio.NewObjectStats()
	name := objectio.BuildObjectName(&types.Uuid{1, 2, 3}, 7)
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	filter, err := buildLifecycleSyncProtectionFilter([]objectio.ObjectStats{*stats})
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(filter)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(decoded), 24)

	manager := gc.NewSyncProtectionManager()
	require.NoError(t, manager.RegisterSyncProtection(
		"lifecycle-round-trip",
		filter,
		1,
		"lifecycle-test",
	))
	require.True(t, manager.IsProtected(name.String()))

	require.NoError(t, validateLifecycleMoCtlResponse(
		`{"result":[{"ReturnStr":"{\"status\":\"ok\"}"}]}`,
	))
	require.Error(t, validateLifecycleMoCtlResponse(
		`{"result":[{"ReturnStr":"{\"status\":\"error\",\"code\":\"busy\"}"}]}`,
	))
}

func TestSQLSyncProtectionClientUsesExistingMOControlPlane(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fake := &syncProtectionSQLExecutor{t: t, mp: mp}
	client := SQLSyncProtectionClient{Executor: fake, TaskID: "lifecycle-task"}
	stats := objectio.NewObjectStats()
	name := objectio.BuildObjectName(&types.Uuid{1, 2, 3}, 7)
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	deadline := time.Now().Add(time.Minute)

	require.NoError(t, client.Register(
		context.Background(),
		"lifecycle-attempt",
		[]objectio.ObjectStats{*stats},
		deadline,
	))
	require.NoError(t, client.Renew(
		context.Background(),
		"lifecycle-attempt",
		deadline,
	))
	require.NoError(t, client.Release(
		context.Background(),
		"lifecycle-attempt",
	))
	require.Len(t, fake.sqls, 3)
	require.Contains(t, fake.sqls[0], "register_sync_protection")
	require.Contains(t, fake.sqls[0], "lifecycle-task")
	require.Contains(t, fake.sqls[1], "renew_sync_protection")
	require.Contains(t, fake.sqls[2], "unregister_sync_protection")

	require.ErrorContains(t, (SQLSyncProtectionClient{}).Renew(
		context.Background(),
		"lifecycle-attempt",
		deadline,
	), "executor is nil")
}

func TestSQLSyncProtectionClientStatsExactObjectIdentity(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(
		"SHARED",
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	defer fs.Close(ctx)
	name := objectio.BuildObjectName(&types.Uuid{4, 5, 6}, 9)
	data := []byte("exact-object")
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: name.String(),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   data,
		}},
	}))
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsLocation(
		stats,
		objectio.BuildLocation(
			name,
			objectio.NewExtent(0, 0, uint32(len(data)), uint32(len(data))),
			1,
			0,
		),
	))
	require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(len(data))))
	client := SQLSyncProtectionClient{FileService: fs}
	require.NoError(t, client.StatExact(ctx, []objectio.ObjectStats{*stats}))

	require.ErrorContains(t, (SQLSyncProtectionClient{}).StatExact(
		ctx,
		[]objectio.ObjectStats{*stats},
	), "FileService is nil")
	empty := objectio.NewObjectStats()
	require.ErrorContains(t, client.StatExact(
		ctx,
		[]objectio.ObjectStats{*empty},
	), "no exact location")
	require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(len(data)+1)))
	require.ErrorContains(t, client.StatExact(
		ctx,
		[]objectio.ObjectStats{*stats},
	), "size changed")
}

type syncProtectionSQLExecutor struct {
	t    *testing.T
	mp   *mpool.MPool
	sqls []string
}

func (fake *syncProtectionSQLExecutor) Exec(
	_ context.Context,
	sql string,
	options executor.Options,
) (executor.Result, error) {
	require.Equal(fake.t, uint32(catalog.System_Account), options.AccountID())
	fake.sqls = append(fake.sqls, sql)
	value := batch.NewWithSize(1)
	value.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	response := `{"result":[{"ReturnStr":"{\"status\":\"ok\"}"}]}`
	require.NoError(fake.t, vector.AppendBytes(
		value.Vecs[0], []byte(response), false, fake.mp,
	))
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: fake.mp}, nil
}

func (*syncProtectionSQLExecutor) ExecTxn(
	context.Context,
	func(executor.TxnExecutor) error,
	executor.Options,
) error {
	panic("unexpected transaction")
}

var _ executor.SQLExecutor = (*syncProtectionSQLExecutor)(nil)

func TestSQLSyncProtectionClientRejectsMissingTNResponse(t *testing.T) {
	client := SQLSyncProtectionClient{Executor: executor.NewMemExecutor(
		func(sql string) (executor.Result, error) {
			require.True(t, strings.Contains(sql, "renew_sync_protection"))
			return executor.Result{}, nil
		},
	)}
	require.ErrorContains(t, client.Renew(
		context.Background(),
		"lifecycle-attempt",
		time.Now().Add(time.Minute),
	), "no TN response")
}
