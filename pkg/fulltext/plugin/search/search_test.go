// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func classicSpec(t *testing.T, opts ftplan.ScanOptions) *plan.IndexSearchScan {
	data, err := ftplan.EncodeScanOptions(opts)
	require.NoError(t, err)
	return &plan.IndexSearchScan{
		Index:       &plan.IndexDef{IndexAlgo: "fulltext", IndexAlgoParams: "{}"},
		AlgoOptions: data,
		SourceTable: &plan.ObjectRef{SchemaName: "db", ObjName: "src"},
		HiddenTables: []*plan.IndexHiddenTableRef{
			{Object: &plan.ObjectRef{SchemaName: "db", ObjName: "idx"}},
		},
	}
}

func boolGuard(v bool) searchplugin.AlgoValue {
	return searchplugin.AlgoValue{Name: fulltext.ZeroRelevanceGuardExpr, Value: &plan.Literal{Value: &plan.Literal_Bval{Bval: v}}}
}

func stubClassicSql(t *testing.T) *[]string {
	prevSQL, prevStreaming := RunSql, RunStreamingSql
	t.Cleanup(func() { RunSql, RunStreamingSql = prevSQL, prevStreaming })
	var sqls []string
	RunSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		return fake_runSql(sqlproc, sql)
	}
	RunStreamingSql = func(ctx context.Context, sqlproc *sqlexec.SqlProcess, sql string, ch chan executor.Result, errCh chan error) (executor.Result, error) {
		sqls = append(sqls, sql)
		return fake_runSql_streaming(ctx, sqlproc, sql, ch, errCh)
	}
	return &sqls
}

func docScoreBatch() *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	return bat
}

func TestClassicNewReaderValidatesItsInput(t *testing.T) {
	proc := newFTTestProcess(t, mpool.MustNewZero(), fulltext.ALGO_TFIDF)
	_, err := Hooks{}.NewReader(nil, classicSpec(t, ftplan.ScanOptions{}), searchplugin.Request{})
	require.ErrorContains(t, err, "requires a process")
	_, err = Hooks{}.NewReader(proc, &plan.IndexSearchScan{}, searchplugin.Request{})
	require.ErrorContains(t, err, "missing its index metadata")
	_, err = Hooks{}.NewReader(proc, &plan.IndexSearchScan{Index: &plan.IndexDef{}}, searchplugin.Request{})
	require.ErrorContains(t, err, "no scan options")
	_, err = Hooks{}.NewReader(proc, &plan.IndexSearchScan{Index: &plan.IndexDef{}, AlgoOptions: []byte("{")}, searchplugin.Request{})
	require.ErrorContains(t, err, "invalid fulltext scan options")
	spec := classicSpec(t, ftplan.ScanOptions{})
	spec.Index.IndexAlgoParams = "{bad"
	_, err = Hooks{}.NewReader(proc, spec, searchplugin.Request{})
	require.Error(t, err)
}

// EmptyScan checks the guard before the pattern, as a running search does.
func TestClassicEmptyScanOrder(t *testing.T) {
	proc := newFTTestProcess(t, mpool.MustNewZero(), fulltext.ALGO_TFIDF)
	nullReq := searchplugin.Request{QueryIsNull: true, AlgoValues: []searchplugin.AlgoValue{boolGuard(true)}}
	require.True(t, moerr.IsMoErrCode(Hooks{}.EmptyScan(proc, nil, nullReq), moerr.ErrNotSupported))
	nullReq.AlgoValues = []searchplugin.AlgoValue{boolGuard(false)}
	require.ErrorContains(t, Hooks{}.EmptyScan(proc, nil, nullReq), "must not be NULL")
	require.ErrorContains(t, Hooks{}.EmptyScan(proc, nil, searchplugin.Request{}), "must not be empty")
	require.NoError(t, Hooks{}.EmptyScan(proc, nil, searchplugin.Request{QueryPayload: []byte("apple")}))
}

func TestClassicReadReturnsTheSearchResult(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := newFTTestProcess(t, mp, fulltext.ALGO_TFIDF)
	sqls := stubClassicSql(t)

	spec := classicSpec(t, ftplan.ScanOptions{SourceTable: "`db`.`src`", IndexTable: "`db`.`idx`", Mode: int64(tree.FULLTEXT_NL)})
	r, err := Hooks{}.NewReader(proc, spec, searchplugin.Request{QueryPayload: []byte("apple")})
	require.NoError(t, err)
	out := docScoreBatch()
	rows := 0
	for {
		end, err := r.Read(context.Background(), nil, nil, mp, out)
		require.NoError(t, err)
		if end {
			break
		}
		rows += out.RowCount()
	}
	require.Equal(t, 8192*3+1, rows)
	require.Len(t, *sqls, 2)
	require.Contains(t, (*sqls)[0], "`db`.`idx`")
	_ = r.(*reader).TakeExplainDiagnostics()
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	end, err := r.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)
}

func TestClassicReadRejectsBeforeSearching(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := newFTTestProcess(t, mp, fulltext.ALGO_TFIDF)
	sqls := stubClassicSql(t)
	spec := classicSpec(t, ftplan.ScanOptions{SourceTable: "src", IndexTable: "idx"})

	for _, req := range []searchplugin.Request{
		{QueryPayload: []byte("apple"), AlgoValues: []searchplugin.AlgoValue{boolGuard(true)}},
		{},
		{QueryPayload: []byte("apple"), HasMembershipFilter: true, MembershipFilter: []byte{1}},
	} {
		r, err := Hooks{}.NewReader(proc, spec, req)
		require.NoError(t, err)
		_, err = r.Read(context.Background(), nil, nil, mp, docScoreBatch())
		require.Error(t, err)
		require.NoError(t, r.Close())
	}
	require.Empty(t, *sqls, "a rejected search runs no SQL")

	// A subscribed source with an inconsistent index reference is rejected.
	pub := classicSpec(t, ftplan.ScanOptions{SourceTable: "src", IndexTable: "idx"})
	pub.SourceTable.PubInfo = &plan.PubInfo{TenantId: 42}
	r, err := Hooks{}.NewReader(proc, pub, searchplugin.Request{QueryPayload: []byte("apple")})
	require.NoError(t, err)
	_, err = r.Read(context.Background(), nil, nil, mp, docScoreBatch())
	require.ErrorContains(t, err, "trusted fulltext table references")
}

// A subscribed source runs the internal SQL against the publisher's tables, and an exact
// membership filter reaches the index reads.
func TestClassicReadPublisherAndMembership(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := newFTTestProcess(t, mp, fulltext.ALGO_TFIDF)
	sqls := stubClassicSql(t)

	keys := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed[int32](keys, 5, false, mp))
	data, err := keys.MarshalBinary()
	require.NoError(t, err)

	spec := classicSpec(t, ftplan.ScanOptions{SourceTable: "ignored", IndexTable: "ignored", Mode: int64(tree.FULLTEXT_NL)})
	pub := &plan.PubInfo{TenantId: 42}
	spec.SourceTable = &plan.ObjectRef{SchemaName: "pub", ObjName: "src", SubscriptionName: "sub", PubInfo: pub}
	spec.HiddenTables[0].Object = &plan.ObjectRef{SchemaName: "pub", ObjName: "idx", SubscriptionName: "sub", PubInfo: pub}
	r, err := Hooks{}.NewReader(proc, spec, searchplugin.Request{
		QueryPayload: []byte("apple"), HasMembershipFilter: true, MembershipFilter: data,
	})
	require.NoError(t, err)
	_, err = r.Read(context.Background(), nil, nil, mp, docScoreBatch())
	require.NoError(t, err)
	require.Contains(t, (*sqls)[0], "`pub`.`idx`")
	require.NotNil(t, r.(*reader).scan.MembershipFilter())
	require.NoError(t, r.Close())
}

func TestClassicReaderNoOpSettersAndOptions(t *testing.T) {
	r := &reader{}
	r.SetOrderBy(nil)
	require.Nil(t, r.GetOrderBy())
	r.SetIndexParam(nil)
	r.SetFilterZM(nil)

	opts := ftplan.ScanOptions{SourceTable: "s", IndexTable: "i", Mode: 1}
	data, err := ftplan.EncodeScanOptions(opts)
	require.NoError(t, err)
	got, err := ftplan.DecodeScanOptions(data)
	require.NoError(t, err)
	require.Equal(t, opts, got)
}
