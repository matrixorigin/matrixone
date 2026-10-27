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

package fulltext2

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type topKWorkspace struct {
	client.Workspace
	readonly bool
}

func (w *topKWorkspace) Readonly() bool { return w.readonly }

type topKTxn struct {
	client.TxnOperator
	meta txn.TxnMeta
	snap bool
	ws   *topKWorkspace
}

func (op *topKTxn) Txn() txn.TxnMeta { return op.meta }
func (op *topKTxn) IsSnapOp() bool   { return op.snap }
func (op *topKTxn) GetWorkspace() client.Workspace {
	if op.ws == nil {
		return nil
	}
	return op.ws
}

func topKReadProcess() (*sqlexec.SqlProcess, *topKTxn) {
	op := &topKTxn{meta: txn.TxnMeta{ID: []byte("read"), SnapshotTS: timestamp.Timestamp{PhysicalTime: 30, LogicalTime: 1}}, ws: &topKWorkspace{readonly: true}}
	ctx := defines.AttachAccountId(context.Background(), 7)
	return sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(ctx, "", op, 7, nil)), op
}

func TestConditionalTopKVisibilityFallsBackToUnlimited(t *testing.T) {
	b := NewBuilder("visibility", int32(types.T_varchar))
	for _, pk := range []string{"c", "a", "b"} {
		feed(t, b, pk, "alpha")
	}
	feed(t, b, "padding", "noise")
	seg := loadedSeg(t, b)
	t.Cleanup(func() { require.NoError(t, seg.dict.Close()) })
	idx := NewIndex([]*Segment{seg}, nil)
	for _, tc := range []struct {
		name    string
		mutate  func(*Fulltext2Search, *sqlexec.SqlProcess, *topKTxn, *Fulltext2Query)
		want    int
		wantErr error
	}{
		{name: "proved", want: 1},
		{name: "unknown load", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) {
			s.loadSnapshotKnown = false
		}, want: 3},
		{name: "unknown generation", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) { s.genValid = false }, want: 3},
		{name: "delete-only tail", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) { s.loadedTail = 0 }, want: 3},
		{name: "unknown build", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) { s.buildTS = 0 }, want: 3},
		{name: "future build", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) { s.buildTS = 30 }, want: 3},
		{name: "newer physical load", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) {
			s.loadSnapshot.PhysicalTime = 31
		}, want: 3},
		{name: "newer logical load", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) {
			s.loadSnapshot = timestamp.Timestamp{PhysicalTime: 30, LogicalTime: 2}
		}, want: 3},
		{name: "other account", mutate: func(s *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) { s.accountID = 8 }, want: 3},
		{name: "mirror", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, _ *Fulltext2Query) { op.meta.Mirror = true }, want: 3},
		{name: "snapshot operator", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, _ *Fulltext2Query) { op.snap = true }, want: 3},
		{name: "named snapshot", mutate: func(_ *Fulltext2Search, sp *sqlexec.SqlProcess, _ *topKTxn, _ *Fulltext2Query) {
			sp.SnapshotTS = &timestamp.Timestamp{PhysicalTime: 20}
		}, want: 3},
		{name: "missing workspace", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, _ *Fulltext2Query) { op.ws = nil }, want: 3},
		{name: "local writes", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, _ *Fulltext2Query) {
			op.ws.readonly = false
		}, want: 3},
		{name: "missing source proof", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = nil
		}, want: 3},
		{name: "provider error", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = func(context.Context, int64) (bool, error) { return false, errors.New("coverage unavailable") }
		}, want: 3},
		{name: "source changed", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = func(context.Context, int64) (bool, error) { return false, nil }
		}, want: 3},
		{name: "workspace changed during proof", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = func(context.Context, int64) (bool, error) { op.ws.readonly = false; return true, nil }
		}, want: 3},
		{name: "identity changed during proof", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = func(context.Context, int64) (bool, error) { op.meta.ID = []byte("other"); return true, nil }
		}, want: 3},
		{name: "snapshot changed during proof", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, op *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = func(context.Context, int64) (bool, error) { op.meta.SnapshotTS.LogicalTime++; return true, nil }
		}, want: 3},
		{name: "cancelled proof", mutate: func(_ *Fulltext2Search, _ *sqlexec.SqlProcess, _ *topKTxn, q *Fulltext2Query) {
			q.SourceUnchanged = func(context.Context, int64) (bool, error) { return false, context.Canceled }
		}, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sp, op := topKReadProcess()
			s := &Fulltext2Search{idx: idx, loaded: true, genValid: true, accountID: 7, loadedTail: -1, buildTS: 20, loadSnapshotKnown: true, loadSnapshot: timestamp.Timestamp{PhysicalTime: 25}}
			q := Fulltext2Query{Pattern: []byte("alpha"), BagOfWords: true, Algo: BM25, ConditionalTopK: 1, Context: sp.GetContext(), SourceUnchanged: func(context.Context, int64) (bool, error) { return true, nil }}
			if tc.mutate != nil {
				tc.mutate(s, sp, op, &q)
			}
			emitted := 0
			_, _, err := s.Search(sp, q, vectorindex.RuntimeConfig{Emit: func(o *vectorindex.SearchOutput) error { emitted += o.Keys.N; PutColumnBuffer(o.Keys); return nil }})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, emitted)
		})
	}
}

func TestConditionalTopKFailedLoadInvalidatesProvenance(t *testing.T) {
	sp, _ := topKReadProcess()
	s := &Fulltext2Search{genValid: true, loadSnapshotKnown: true, buildTS: 20, loadSnapshot: timestamp.Timestamp{PhysicalTime: 25}}
	swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		return executor.Result{}, errors.New("load failed")
	})
	require.Error(t, s.Load(sp))
	require.False(t, s.loadSnapshotKnown)
	require.False(t, s.genValid)
	require.Zero(t, s.buildTS)
	s.Destroy()
	require.True(t, s.loadSnapshot.IsEmpty())
}

func TestConditionalTopKLoadProvenance(t *testing.T) {
	for _, change := range []string{"none", "writer", "snapshot", "identity", "mirror"} {
		t.Run(change, func(t *testing.T) {
			sp, mp := mockSqlProc(t)
			_, op := topKReadProcess()
			sp.Proc.Base.TxnOperator = op
			swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				// Mutate only after Load's initial readonly capture, during its reads.
				switch change {
				case "writer":
					op.ws.readonly = false
				case "snapshot":
					op.meta.SnapshotTS.LogicalTime++
				case "identity":
					op.meta.ID = append(op.meta.ID, 'x')
				case "mirror":
					op.meta.Mirror = true
				}
				if strings.Contains(sql, "SUM(") || strings.Contains(sql, "COUNT(*)") {
					return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
				}
				return executor.Result{Mp: mp}, nil
			})
			s := NewFulltext2Search(testStorageCfg())
			require.NoError(t, s.Load(sp))
			defer s.Destroy()
			require.Equal(t, change == "none", s.loadSnapshotKnown)
			if s.loadSnapshotKnown {
				require.Equal(t, op.meta.SnapshotTS, s.loadSnapshot)
			}
		})
	}
}

func TestConditionalTopKConcurrentCollectors(t *testing.T) {
	b := NewBuilder("shared", int32(types.T_varchar))
	for _, key := range []string{"c", "a", "b"} {
		feed(t, b, key, "alpha")
	}
	feed(t, b, "padding", "noise")
	seg := loadedSeg(t, b)
	t.Cleanup(func() { require.NoError(t, seg.dict.Close()) })
	s := &Fulltext2Search{idx: NewIndex([]*Segment{seg}, nil), loaded: true, genValid: true, accountID: 7, loadedTail: -1, buildTS: 20, loadSnapshotKnown: true, loadSnapshot: timestamp.Timestamp{PhysicalTime: 25}}
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			sp, _ := topKReadProcess()
			q := Fulltext2Query{Pattern: []byte("alpha"), BagOfWords: true, Algo: BM25, ConditionalTopK: 2, Context: sp.GetContext(), SourceUnchanged: func(context.Context, int64) (bool, error) { return true, nil }}
			var rows []rankedStreamRow
			_, _, err := s.Search(sp, q, vectorindex.RuntimeConfig{Emit: func(o *vectorindex.SearchOutput) error {
				defer PutColumnBuffer(o.Keys)
				rows = append(rows, streamRows(o)...)
				return nil
			}})
			require.NoError(t, err)
			require.Len(t, rows, 2)
			require.Equal(t, "a", rows[0].key)
			require.Equal(t, "b", rows[1].key)
		})
	}
}

func TestConditionalTopKUnknownProcess(t *testing.T) {
	s := &Fulltext2Search{}
	ok, err := s.conditionalTopKEligible(nil, Fulltext2Query{Context: context.Background()})
	require.NoError(t, err)
	require.False(t, ok)
}
