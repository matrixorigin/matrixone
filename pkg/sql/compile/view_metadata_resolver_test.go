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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	require.Equal(t, uint32(9), snapshot.GetTenant().GetTenantID())

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

func TestSnapshotTenantID(t *testing.T) {
	require.Equal(t, uint32(0), snapshotTenantID("cluster", 9, 7))
	require.Equal(t, uint32(9), snapshotTenantID("account", 9, 7))
	require.Equal(t, uint32(7), snapshotTenantID("database", 9, 7))
	require.Equal(t, uint32(7), snapshotTenantID("table", 9, 7))
}

func TestViewMetadataRefreshResolverSnapshotErrors(t *testing.T) {
	ctx := context.Background()
	const snapshotSQL = "select sname, ts, level, account_name, obj_id from " +
		"mo_catalog.mo_snapshots where sname = 'sn' and coalesce(kind, '') != 'branch' " +
		"order by snapshot_id"

	t.Run("executor error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		want := errors.New("snapshot query failed")
		spyExec := &alterCopyInsertSpyExecutor{errs: map[string]error{snapshotSQL: want}}
		resolver := viewMetadataRefreshResolver{
			compile:   newAlterCopyPrecheckCompile(t, ctrl, spyExec),
			accountID: 7,
		}

		_, err := resolver.ResolveSnapshot(ctx, "sn")
		require.ErrorIs(t, err, want)
	})

	t.Run("duplicate snapshot records", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mp := mpool.MustNewZero()
		result := newSnapshotResolverResult(t, mp, 2)
		spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{snapshotSQL: result}}
		resolver := viewMetadataRefreshResolver{
			compile:   newAlterCopyPrecheckCompile(t, ctrl, spyExec),
			accountID: 7,
		}

		_, err := resolver.ResolveSnapshot(ctx, "sn")
		require.ErrorContains(t, err, "find 2 snapshot records")
		require.Zero(t, mp.CurrNB())
	})
}

func TestViewMetadataRefreshResolverUDFErrors(t *testing.T) {
	ctx := context.Background()
	const udfSQL = "select cast(args as char), body, language, rettype, db, cast(modified_time as char), sql_mode " +
		"from mo_catalog.mo_user_defined_function where name = 'f' and db = 'db'"
	arg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}

	tests := []struct {
		name      string
		rows      [][]string
		execError error
		contains  string
		typed     bool
	}{
		{name: "executor error", execError: errors.New("udf query failed"), contains: "udf query failed"},
		{name: "missing udf", typed: true, contains: "function or operator 'f'"},
		{name: "invalid args json", rows: [][]string{{"{", "1", "sql", "int", "db", "2026-01-01", ""}}, contains: "unexpected end"},
		{name: "arity mismatch", rows: [][]string{{"[]", "1", "sql", "int", "db", "2026-01-01", ""}}, contains: "No matching function"},
		{name: "type mismatch", rows: [][]string{{`[{"name":"x","type":"json"}]`, "1", "sql", "int", "db", "2026-01-01", ""}}, contains: "No matching function"},
		{name: "ambiguous overload", rows: [][]string{
			{`[{"name":"x","type":"bigint"}]`, "1", "sql", "int", "db", "2026-01-01", ""},
			{`[{"name":"x","type":"bigint"}]`, "2", "sql", "int", "db", "2026-01-02", ""},
		}, contains: "ambiguous"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mp := mpool.MustNewZero()
			spyExec := &alterCopyInsertSpyExecutor{}
			if test.execError != nil {
				spyExec.errs = map[string]error{udfSQL: test.execError}
			} else if test.rows != nil {
				spyExec.results = map[string]executor.Result{udfSQL: newUDFResolverResult(t, mp, test.rows)}
			}
			resolver := viewMetadataRefreshResolver{
				compile:         newAlterCopyPrecheckCompile(t, ctrl, spyExec),
				accountID:       7,
				defaultDatabase: "db",
			}

			_, err := resolver.ResolveUdf(ctx, "f", []*plan.Expr{arg})
			require.ErrorContains(t, err, test.contains)
			if test.typed {
				var notFound *viewMetadataUDFNotFoundError
				require.ErrorAs(t, err, &notFound)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func newSnapshotResolverResult(t *testing.T, mp *mpool.MPool, rows int) executor.Result {
	t.Helper()
	bat := batch.NewWithSize(5)
	bat.SetRowCount(rows)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_uint64.ToType())
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("sn"), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(100+i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("account"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte("acc"), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], uint64(7), false, mp))
	}
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func newUDFResolverResult(t *testing.T, mp *mpool.MPool, rows [][]string) executor.Result {
	t.Helper()
	bat := batch.NewWithSize(7)
	bat.SetRowCount(len(rows))
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
	}
	for _, row := range rows {
		for i, value := range row {
			require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte(value), false, mp))
		}
	}
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}
