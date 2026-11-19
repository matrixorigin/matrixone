// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package changedtables

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type watchCatalogTestExecutor struct {
	executor.SQLExecutor
	execTxn func(context.Context, func(executor.TxnExecutor) error, executor.Options) error
}

func (e watchCatalogTestExecutor) ExecTxn(
	ctx context.Context, f func(executor.TxnExecutor) error, o executor.Options,
) error {
	return e.execTxn(ctx, f, o)
}

type watchCatalogTestTxn struct {
	executor.TxnExecutor
	op   client.TxnOperator
	exec func(string, executor.StatementOption) (executor.Result, error)
}

func (te watchCatalogTestTxn) Txn() client.TxnOperator { return te.op }

func (te watchCatalogTestTxn) Exec(sql string, o executor.StatementOption) (executor.Result, error) {
	return te.exec(sql, o)
}

type watchCatalogTestOp struct {
	client.TxnOperator
	ts timestamp.Timestamp
}

func (op watchCatalogTestOp) SnapshotTS() timestamp.Timestamp { return op.ts }

func TestReadWatchCatalogAtExactCut(t *testing.T) {
	ctx := t.Context()
	cut := timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 17}
	requestedName := "ob'server"
	var queries int
	op := watchCatalogTestOp{ts: cut.Next()}
	te := watchCatalogTestTxn{op: op}
	te.exec = func(sql string, o executor.StatementOption) (executor.Result, error) {
		queries++
		stmt, err := mysql.ParseOne(ctx, sql, 1)
		require.NoError(t, err, "%s", sql)
		stmt.Free()
		require.NotContains(t, sql, requestedName)
		require.NotContains(t, sql, cut.Next().DebugString())
		switch queries {
		case 1:
			require.Equal(t, 3, strings.Count(sql, "{MO_TS = '42-17'}"))
			require.Equal(t, []string{"0", "moadmin", "0"}, watchCatalogTestParams(t, o))
			return changedTableTestResult(t, []any{int32(0)}), nil
		case 2:
			require.Equal(t, 1, strings.Count(sql, "{MO_TS = '42-17'}"))
			return changedTableTestResult(t, []any{types.Uuid{1}, true}), nil
		case 3:
			require.Equal(t, 1, strings.Count(sql, "{MO_TS = '42-17'}"))
			require.Equal(t, []string{requestedName}, watchCatalogTestParams(t, o))
			return changedTableTestResult(t, []any{int32(17), types.Timestamp(12)}), nil
		case 4:
			require.Equal(t, 4, strings.Count(sql, "{MO_TS = '42-17'}"))
			require.Equal(t, []string{"17"}, watchCatalogTestParams(t, o))
			require.NotZero(t, o.MaxResultBytes())
			return changedTableTestResult(t, []any{uint32(3), "source", "db", "t", types.Uuid{2}, uint64(88), nil, nil, nil}), nil
		default:
			t.Fatalf("unexpected catalog query %s", sql)
			return executor.Result{}, nil
		}
	}
	e := watchCatalogTestExecutor{execTxn: func(c context.Context, f func(executor.TxnExecutor) error, o executor.Options) error {
		accountID, err := defines.GetAccountId(c)
		require.NoError(t, err)
		require.Zero(t, accountID)
		require.False(t, o.HasExistsTxn())
		require.Equal(t, cut, o.MinCommittedTS())
		require.True(t, o.HasTxnIsolation())
		require.Equal(t, txn.TxnIsolation_SI, o.TxnIsolation())
		return f(te)
	}}
	catalogResult, err := readCatalog(ctx, e, callerIdentity{}, &requestedName, cut)
	require.NoError(t, err)
	require.Equal(t, 4, queries)
	require.Equal(t, Binding{Epoch: [16]byte{1}, Observer: Observer{AccountID: 17, Incarnation: 12}}, catalogResult.binding)
	require.Equal(t, map[uint32]string{3: "source"}, catalogResult.sourceNames)
	require.Equal(t, []Grant{{AccountID: 3, DatabaseName: "db", TableName: "t", GrantID: [16]byte{2}, DatabaseID: 88}}, catalogResult.grants)
}

func TestWatchCatalogRejectsInvalidRead(t *testing.T) {
	ctx := t.Context()
	cut := timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 17}
	name := "observer"
	e := watchCatalogTestExecutor{execTxn: func(c context.Context, f func(executor.TxnExecutor) error, o executor.Options) error {
		return f(watchCatalogTestTxn{op: watchCatalogTestOp{ts: cut}})
	}}
	_, err := readCatalog(ctx, e, callerIdentity{}, &name, cut)
	require.ErrorContains(t, err, "did not start after")
	_, err = readCatalog(ctx, e, callerIdentity{accountID: 7, roleID: 2}, &name, cut)
	require.ErrorContains(t, err, "permission denied")

	res := changedTableTestResult(t, []any{uint32(3), "source", "db", "t", types.Uuid{2}, uint64(10), uint64(11), int8(2), nil})
	defer res.Close()
	grants, names, err := decodeWatchGrants(ctx, res)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
	require.ErrorContains(t, err, `source 3 granted "db"."t"`)
	require.Nil(t, grants)
	require.Nil(t, names)
}

func TestDecodeWatchPartitionIdentity(t *testing.T) {
	encode := func(flag, parentID uint64) []byte {
		raw, err := (&api.SchemaExtra{FeatureFlag: flag, ParentTableID: parentID}).Marshal()
		require.NoError(t, err)
		return raw
	}
	for _, tc := range []struct {
		name, table string
		legacy      int8
		extra       any
		wantID      uint64
	}{
		{name: "old null extra", table: "t", wantID: 11},
		{name: "old empty extra", table: "t", extra: []byte{}, wantID: 11},
		{name: "ambiguous name awaits catalog proof", table: "%!%p0%!%t", wantID: 11},
		{name: "flagged leaf", table: "%!%p0%!%t", extra: encode(features.Partition, 12)},
		{name: "flagged index", table: "index_other_name", extra: encode(features.IndexTable, 12)},
		{name: "partition index", table: "%!%p0%!%t", extra: encode(features.Partition|features.IndexTable, 12)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := changedTableTestResult(t, []any{uint32(3), "source", "db", tc.table, types.Uuid{2}, uint64(10), uint64(11), tc.legacy, tc.extra})
			defer res.Close()
			grants, names, err := decodeWatchGrants(t.Context(), res)
			require.NoError(t, err)
			require.Equal(t, map[uint32]string{3: "source"}, names)
			require.Equal(t, []Grant{{AccountID: 3, DatabaseName: "db", TableName: tc.table, GrantID: [16]byte{2},
				DatabaseID: 10, TableID: tc.wantID}}, grants)
		})
	}
	for name, raw := range map[string][]byte{
		"malformed":    {0x80},
		"conflicting":  encode(features.Partition|features.Partitioned, 12),
		"unknown link": encode(0, 12),
	} {
		t.Run(name, func(t *testing.T) {
			res := changedTableTestResult(t, []any{uint32(3), "source", "db", "t", types.Uuid{2}, uint64(10), uint64(11), int8(0), raw})
			defer res.Close()
			grants, names, err := decodeWatchGrants(t.Context(), res)
			require.ErrorContains(t, err, `source 3 granted "db"."t"`)
			require.Nil(t, grants)
			require.Nil(t, names)
		})
	}
	for _, tc := range []struct {
		name, table string
		legacy      int8
		extra       any
		errorText   string
	}{
		{name: "modern parent", table: "t", legacy: 1, extra: encode(features.Partitioned, 0), errorText: `partitioned source 3 granted "db"."t"`},
		{name: "flag only parent", table: "t", extra: encode(features.Partitioned, 0), errorText: `partitioned source 3 granted "db"."t"`},
		{name: "legacy parent", table: "t", legacy: 1, errorText: `partitioned source 3 granted "db"."t"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := changedTableTestResult(t, []any{uint32(3), "source", "db", tc.table, types.Uuid{2}, uint64(10), uint64(11), tc.legacy, tc.extra})
			defer res.Close()
			grants, names, err := decodeWatchGrants(t.Context(), res)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.ErrorContains(t, err, tc.errorText)
			require.Nil(t, grants)
			require.Nil(t, names)
		})
	}
}

func TestReadWatchGrantsChecksAmbiguousNamesAtSourceCut(t *testing.T) {
	const (
		hint  = "{MO_TS = '42-17'}"
		table = "%!%p0%!%t"
		id    = uint64(9_007_199_254_740_993)
	)
	for _, tc := range []struct {
		name, table  string
		relationID   any
		match        bool
		missingTable bool
		wantError    string
	}{
		{name: "ordinary name", table: "t", relationID: id},
		{name: "missing relation", table: table},
		{name: "name collision", table: table, relationID: id},
		{name: "legacy catalog leaf", table: table, relationID: id, match: true},
		{name: "partition catalog unavailable", table: table, relationID: id, missingTable: true,
			wantError: "mo_catalog.mo_partition_tables is unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queries int
			te := watchCatalogTestTxn{exec: func(sql string, opts executor.StatementOption) (executor.Result, error) {
				queries++
				if queries == 1 {
					var partitioned any
					if tc.relationID != nil {
						partitioned = int8(0)
					}
					return changedTableTestResult(t, []any{uint32(3), "source", "db", tc.table, types.Uuid{2}, uint64(10), tc.relationID, partitioned, nil}), nil
				}
				require.Equal(t, 2, queries)
				require.Contains(t, sql, "mo_catalog.mo_partition_tables "+hint)
				require.Contains(t, sql, "where partition_id in (cast(? as unsigned)) limit 2")
				require.True(t, opts.HasAccountID())
				require.Equal(t, uint32(3), opts.AccountID())
				require.Equal(t, []string{"9007199254740993"}, watchCatalogTestParams(t, opts))
				require.Equal(t, uint64(watchPartitionLookupResultSize), opts.MaxResultBytes())
				if tc.missingTable {
					return executor.Result{}, moerr.NewNoSuchTable(t.Context(), "mo_catalog", "mo_partition_tables")
				}
				if tc.match {
					return changedTableTestResult(t, []any{id}), nil
				}
				return executor.Result{}, nil
			}}
			grants, names, err := readWatchGrants(t.Context(), te, 17, hint)
			wantQueries := 1
			if tc.table == table && tc.relationID != nil {
				wantQueries = 2
			}
			require.Equal(t, wantQueries, queries)
			if tc.wantError != "" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
				require.ErrorContains(t, err, tc.wantError)
				require.Empty(t, grants)
				require.Empty(t, names)
				return
			}
			require.NoError(t, err)
			require.Len(t, grants, 1)
			if tc.relationID == nil || tc.match {
				require.Zero(t, grants[0].TableID)
			} else {
				require.Equal(t, id, grants[0].TableID)
			}
		})
	}
}

func changedTableTestResult(t *testing.T, row []any) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	b := batch.NewWithSize(len(row))
	handedOff := false
	defer func() {
		if !handedOff {
			b.Clean(mp)
		}
	}()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	for i, val := range row {
		switch v := val.(type) {
		case string:
			b.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(b.Vecs[i], []byte(v), false, mp))
		case []byte:
			b.Vecs[i] = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(b.Vecs[i], v, false, mp))
		case int8:
			b.Vecs[i] = vector.NewVec(types.T_int8.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case int32:
			b.Vecs[i] = vector.NewVec(types.T_int32.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case uint32:
			b.Vecs[i] = vector.NewVec(types.T_uint32.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case uint64:
			b.Vecs[i] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case bool:
			b.Vecs[i] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case types.Uuid:
			b.Vecs[i] = vector.NewVec(types.T_uuid.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case types.Timestamp:
			b.Vecs[i] = vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixed(b.Vecs[i], v, false, mp))
		case nil:
			oid := types.T_uint64
			if i == 7 {
				oid = types.T_int8
			}
			if i == 8 {
				oid = types.T_varchar
			}
			b.Vecs[i] = vector.NewVec(oid.ToType())
			require.NoError(t, vector.AppendNull(b.Vecs[i], mp))
		default:
			t.Fatalf("unsupported catalog fixture value %T", v)
		}
	}
	b.SetRowCount(1)
	handedOff = true
	return executor.Result{Batches: []*batch.Batch{b}, Mp: mp}
}

func watchCatalogTestParams(t *testing.T, opts executor.StatementOption) []string {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	vec, err := opts.Params(mp)
	require.NoError(t, err)
	defer vec.Free(mp)
	params := make([]string, vec.Length())
	for i := range params {
		params[i] = vec.GetStringAt(i)
	}
	return params
}
