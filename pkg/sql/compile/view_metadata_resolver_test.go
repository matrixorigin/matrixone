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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestViewMetadataRefreshResolverWithoutSQLHelper(t *testing.T) {
	ctrl := gomock.NewController(t)
	mp := mpool.MustNewZero()
	snapshotSQL := "select sname, ts, level, account_name, obj_id from " +
		"mo_catalog.mo_snapshots where sname = 'sn' and coalesce(kind, '') != 'branch' " +
		"order by snapshot_id"
	udfSQL := "select cast(args as char), body, language, rettype, db, cast(modified_time as char), sql_mode " +
		"from mo_catalog.mo_user_defined_function where name = 'f' and db = 'db'"

	snapshotBatch := batch.NewWithSize(5)
	snapshotBatch.SetRowCount(1)
	snapshotBatch.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	snapshotBatch.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	snapshotBatch.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	snapshotBatch.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	snapshotBatch.Vecs[4] = vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendBytes(snapshotBatch.Vecs[0], []byte("sn"), false, mp))
	require.NoError(t, vector.AppendFixed(snapshotBatch.Vecs[1], int64(123), false, mp))
	require.NoError(t, vector.AppendBytes(snapshotBatch.Vecs[2], []byte("account"), false, mp))
	require.NoError(t, vector.AppendBytes(snapshotBatch.Vecs[3], []byte("acc"), false, mp))
	require.NoError(t, vector.AppendFixed(snapshotBatch.Vecs[4], uint64(9), false, mp))

	udfBatch := batch.NewWithSize(7)
	udfBatch.SetRowCount(1)
	for i := range udfBatch.Vecs {
		udfBatch.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
	}
	for i, value := range []string{"[]", "1", "sql", "int", "db", "2026-07-30 12:00:00", ""} {
		require.NoError(t, vector.AppendBytes(udfBatch.Vecs[i], []byte(value), false, mp))
	}

	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		snapshotSQL: {Mp: mp, Batches: []*batch.Batch{snapshotBatch}},
		udfSQL:      {Mp: mp, Batches: []*batch.Batch{udfBatch}},
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	require.Nil(t, c.proc.GetSessionInfo().SqlHelper)
	resolver := viewMetadataRefreshResolver{
		compile:         c,
		accountID:       7,
		defaultDatabase: "db",
	}

	snapshot, err := resolver.ResolveSnapshot(context.Background(), "sn")
	require.NoError(t, err)
	require.Equal(t, int64(123), snapshot.GetTS().GetPhysicalTime())
	require.Equal(t, uint32(7), snapshot.GetTenant().GetTenantID())

	udf, err := resolver.ResolveUdf(context.Background(), "f", nil)
	require.NoError(t, err)
	require.Equal(t, "1", udf.Body)
	require.Equal(t, "2026-07-30_12-00-00", udf.ModifiedTime)
	require.Equal(t, []string{snapshotSQL, udfSQL}, spyExec.executedSQLs)
	require.Zero(t, mp.CurrNB())
}

func TestViewMetadataRefreshResolverSnapshotNotFoundIsTyped(t *testing.T) {
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	resolver := viewMetadataRefreshResolver{compile: c, accountID: 7}

	_, err := resolver.ResolveSnapshot(context.Background(), "deleted")

	require.Error(t, err)
	var notFound *viewMetadataSnapshotNotFoundError
	require.True(t, errors.As(err, &notFound))
	require.Equal(t, "deleted", notFound.name)
}
