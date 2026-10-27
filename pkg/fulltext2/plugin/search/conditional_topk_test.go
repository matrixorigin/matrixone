// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package search

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type proofWorkspace struct{ client.Workspace }

func (*proofWorkspace) Readonly() bool { return true }

type proofTxn struct{ client.TxnOperator }

func (*proofTxn) IsSnapOp() bool                 { return false }
func (*proofTxn) Txn() txn.TxnMeta               { return txn.TxnMeta{ID: []byte("query")} }
func (*proofTxn) GetWorkspace() client.Workspace { return &proofWorkspace{} }

type proofRelation struct {
	engine.Relation
	bar   types.TS
	err   error
	bound types.TS
}

func (*proofRelation) GetTableID(context.Context) uint64 { return 42 }
func (*proofRelation) GetTableName() string              { return "src" }
func (r *proofRelation) SourceCommitTS(_ context.Context, bound types.TS) (types.TS, error) {
	r.bound = bound
	return r.bar, r.err
}

type proofEngine struct {
	engine.Engine
	rel engine.Relation
	err error
}

func (e *proofEngine) GetRelationById(_ context.Context, _ client.TxnOperator, id uint64) (string, string, engine.Relation, error) {
	if id != 42 {
		return "", "", nil, errors.New("wrong identity")
	}
	return "db", "src", e.rel, e.err
}

func TestConditionalTopKSourceProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bar    types.TS
		err    error
		mutate func(*reader, *proofEngine)
		want   bool
	}{
		{name: "covered source", bar: types.BuildTS(19, 9), want: true},
		{name: "physical boundary", bar: types.BuildTS(20, 0), want: true},
		{name: "logical beyond build", bar: types.BuildTS(20, 1)},
		{name: "newer source", bar: types.BuildTS(21, 0)},
		{name: "missing source timestamp"},
		{name: "coverage error", err: errors.New("retention unavailable")},
		{name: "provider cancellation", err: context.Canceled},
		{name: "lookup error", mutate: func(_ *reader, e *proofEngine) { e.err = errors.New("lookup failed") }},
		{name: "missing provider", mutate: func(_ *reader, e *proofEngine) { e.rel = nil }},
		{name: "name mismatch", mutate: func(u *reader, _ *proofEngine) { u.spec.SourceTable.ObjName = "other" }},
		{name: "id mismatch", mutate: func(u *reader, _ *proofEngine) { u.spec.SourceTableDef.TblId = 43 }},
		{name: "publication", mutate: func(u *reader, _ *proofEngine) { u.spec.SourceTable.PubInfo = &plan.PubInfo{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
			proc.Base.TxnOperator = &proofTxn{}
			rel := &proofRelation{bar: tc.bar, err: tc.err}
			eng := &proofEngine{rel: rel}
			proc.Base.SessionInfo.StorageEngine = eng
			u := &reader{proc: proc, tblcfg: fulltext2.TableConfig{DbName: "db"}, spec: &plan.IndexSearchScan{SourceTable: &plan.ObjectRef{Obj: 42, SchemaName: "db", ObjName: "src"}, SourceTableDef: &plan.TableDef{TblId: 42, Name: "src"}}}
			if tc.mutate != nil {
				tc.mutate(u, eng)
			}
			ok, err := u.sourceUnchanged(proc.Ctx, 20)
			require.Equal(t, tc.want, ok)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			if tc.want {
				require.Equal(t, types.BuildTS(20, 0), rel.bound)
			}
		})
	}
}

func TestConditionalTopKOptionalOptionsCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		desc     *ft2plan.ConditionalStreamTopK
		snapshot bool
		want     int
	}{
		{name: "old plan"},
		{name: "version one", desc: &ft2plan.ConditionalStreamTopK{Semantics: ft2plan.ConditionalStreamTopKVersion, K: 3}, want: 3},
		{name: "unknown version", desc: &ft2plan.ConditionalStreamTopK{Semantics: "future", K: 3}},
		{name: "zero", desc: &ft2plan.ConditionalStreamTopK{Semantics: ft2plan.ConditionalStreamTopKVersion}},
		{name: "unbounded", desc: &ft2plan.ConditionalStreamTopK{Semantics: ft2plan.ConditionalStreamTopKVersion, K: 8193}},
		{name: "snapshot", desc: &ft2plan.ConditionalStreamTopK{Semantics: ft2plan.ConditionalStreamTopKVersion, K: 3}, snapshot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := ft2plan.ScanOptions{Config: "{}", Mode: 4, ConditionalStreamTopK: tc.desc}
			data, err := ft2plan.EncodeScanOptions(opts)
			require.NoError(t, err)
			// A reader compiled before the descriptor existed ignores the optional field.
			var old struct {
				Config string `json:"config"`
				Mode   int64  `json:"mode"`
			}
			require.NoError(t, json.Unmarshal(data, &old))
			require.Equal(t, opts.Config, old.Config)
			require.Equal(t, opts.Mode, old.Mode)
			decoded, err := ft2plan.DecodeScanOptions(data)
			require.NoError(t, err)
			u := &reader{opts: decoded, spec: &plan.IndexSearchScan{}}
			if tc.snapshot {
				u.spec.ScanSnapshot = &plan.Snapshot{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var q fulltext2.Fulltext2Query
			u.configureConditionalTopK(ctx, &q)
			require.Equal(t, tc.want, q.ConditionalTopK)
			if tc.want != 0 {
				require.Same(t, ctx, q.Context)
				require.NotNil(t, q.SourceUnchanged)
				cancel()
				require.ErrorIs(t, q.Context.Err(), context.Canceled)
			}
		})
	}
	_, err := ft2plan.DecodeScanOptions([]byte(`{"config":"{}","conditional_stream_topk":{"k":"bad"}}`))
	require.Error(t, err)
}
