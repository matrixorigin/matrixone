//go:build mo_m0

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

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type m0MappingTestRelation struct {
	engine.Relation
	def *plan.TableDef
	id  uint64
}

func (r *m0MappingTestRelation) GetTableDef(context.Context) *plan.TableDef { return r.def }
func (r *m0MappingTestRelation) GetTableID(context.Context) uint64          { return r.id }
func (*m0MappingTestRelation) GetDBID(context.Context) uint64               { return 7 }
func (*m0MappingTestRelation) Ranges(context.Context, engine.RangesParam) (engine.RelData, error) {
	return readutil.NewBlockListRelationData(0), nil
}

func TestM0RemoteMemberMappingBeforePrune(t *testing.T) {
	parent := &plan.TableDef{Cols: []*plan.ColDef{{Name: "a", ColId: 1, Seqnum: 0,
		Typ: plan.Type{Id: 20, Width: 12, Scale: 2}, Default: &plan.Default{NullAbility: true}}}}
	for _, tc := range []struct {
		name string
		edit func(*plan.TableDef)
		bad  bool
	}{
		{name: "valid differing table name", edit: func(d *plan.TableDef) { d.Name = "child"; d.Cols[0].Typ.Table = "child" }},
		{name: "implicit and explicit null", edit: func(d *plan.TableDef) {
			d.Cols[0].Default.OriginString = "NULL"
			d.Cols[0].Default.Expr = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
		}},
		{name: "unselected member sequence", edit: func(d *plan.TableDef) { d.Cols[0].Seqnum++ }, bad: true},
		{name: "unselected member type", edit: func(d *plan.TableDef) { d.Cols[0].Typ.Width++ }, bad: true},
		{name: "unselected member identity", edit: func(d *plan.TableDef) { d.Cols[0].ColId++ }, bad: true},
		{name: "unselected member default", edit: func(d *plan.TableDef) { d.Cols[0].Default.OriginString = "1" }, bad: true},
		{name: "unselected member nullability", edit: func(d *plan.TableDef) { d.Cols[0].Default.NullAbility = false }, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := proto.Clone(parent).(*plan.TableDef)
			other := proto.Clone(parent).(*plan.TableDef)
			tc.edit(other)
			selected := &m0MappingTestRelation{def: good, id: 1}
			unselected := &m0MappingTestRelation{def: other, id: 2}
			pruned := false
			table := &combinedTxnTable{
				primary:    &txnTable{tableDef: parent},
				tablesFunc: func(context.Context) ([]engine.Relation, error) { return []engine.Relation{selected, unselected}, nil },
				pruneFunc: func(context.Context, engine.RangesParam) ([]engine.Relation, error) {
					pruned = true
					return []engine.Relation{selected}, nil
				},
			}
			_, err := table.Ranges(m0read.WithRemoteMemberValidation(t.Context()), engine.RangesParam{})
			if tc.bad {
				require.Error(t, err)
				require.False(t, pruned, "a bad member must not be hidden by pruning")
			} else {
				require.NoError(t, err)
				require.True(t, pruned)
			}
		})
	}
}

func TestM0RemoteMemberMappingSelectionAndCancel(t *testing.T) {
	def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "a", ColId: 1, Typ: plan.Type{Id: 20}}}}
	member := &m0MappingTestRelation{def: def, id: 1}
	table := &combinedTxnTable{
		primary:    &txnTable{tableDef: def},
		tablesFunc: func(context.Context) ([]engine.Relation, error) { return []engine.Relation{member}, nil },
		pruneFunc: func(context.Context, engine.RangesParam) ([]engine.Relation, error) {
			return []engine.Relation{&m0MappingTestRelation{def: def, id: 2}}, nil
		},
	}
	ctx := m0read.WithRemoteMemberValidation(t.Context())
	_, err := (&combinedTxnTable{}).Ranges(ctx, engine.RangesParam{})
	require.Error(t, err, "partially initialized selectors must fail without a panic")
	_, err = table.Ranges(ctx, engine.RangesParam{})
	require.Error(t, err, "pruning must not publish an unvalidated member")

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = table.Ranges(ctx, engine.RangesParam{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = (&combinedTxnTable{}).Ranges(ctx, engine.RangesParam{})
	require.ErrorIs(t, err, context.Canceled, "cancellation must precede incomplete selector errors")

	// The ordinary path has no experimental marker and keeps its prior flow.
	_, err = table.Ranges(t.Context(), engine.RangesParam{})
	require.NoError(t, err)
}

func TestM0RemoteMemberMappingInvalidatesDuringRanges(t *testing.T) {
	op, txn := newResetTxnForTest(t, &Engine{})
	def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "a", ColId: 1, Typ: plan.Type{Id: 20}}}}
	primary := &txnTable{tableDef: def, lastTS: op.SnapshotTS(), db: &txnDatabase{op: op}}
	member := &m0MappingTestRelation{def: def, id: 1}
	mutate := false
	started, changed := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(m0read.WithRemoteMemberValidation(t.Context()), 5*time.Second)
	defer cancel()
	go func() {
		defer close(changed)
		select {
		case <-started:
			txn.beginCatalogMutation()
			txn.endCatalogMutation()
		case <-ctx.Done():
		}
	}()
	combined := &combinedTxnTable{
		primary:    primary,
		tablesFunc: func(context.Context) ([]engine.Relation, error) { return []engine.Relation{member}, nil },
		pruneFunc: func(context.Context, engine.RangesParam) ([]engine.Relation, error) {
			if mutate {
				close(started)
				select {
				case <-changed:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []engine.Relation{member}, nil
		},
	}
	delegate := &txnTableDelegate{origin: primary}
	delegate.combined.is = true
	delegate.combined.tbl = combined
	delegate.combined.revision, delegate.combined.cacheable = txn.CatalogVisibility()
	_, err := delegate.Ranges(ctx, engine.RangesParam{})
	require.NoError(t, err)
	mutate = true
	data, err := delegate.Ranges(ctx, engine.RangesParam{})
	require.Error(t, err)
	require.Nil(t, data, "a changed workspace must not publish selected members")
	mutate = false
	_, err = delegate.Ranges(ctx, engine.RangesParam{})
	require.Error(t, err, "an old combined relation cannot be reused after a catalog change")
	canceled, cancelRead := context.WithCancel(ctx)
	cancelRead()
	_, err = delegate.Ranges(canceled, engine.RangesParam{})
	require.ErrorIs(t, err, context.Canceled, "cancellation must not be masked by stale member state")
}
