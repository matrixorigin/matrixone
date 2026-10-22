//go:build mo_m0

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

package rpc

import (
	"context"
	"testing"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestM0HandleCommitAppliedSchemaRollback(t *testing.T) {
	runM0HandleCommitAppliedSchemaRollback(t, false)
}

func TestM0BirthHandleCommitAppliedSchemaRollback(t *testing.T) {
	runM0HandleCommitAppliedSchemaRollback(t, true)
}

func runM0HandleCommitAppliedSchemaRollback(t *testing.T, birthFalse bool) {
	const point = "fj/m0/alter/tn-after-apply"
	wasEnabled := fault.Status()
	fault.Enable()
	defer func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), point)
		_, _ = fault.RemoveFaultPoint(context.Background(), point+"/waiters")
		if !wasEnabled {
			fault.Disable()
		}
	}()
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "cancel"}[canceled], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			h := mockTAEHandle(ctx, t, config.WithLongScanAndCKPOpts(nil))
			defer h.HandleClose(context.Background())
			schema := catalog.NewEmptySchema("m0_tn_rollback")
			schema.Relkind = pkgcatalog.SystemOrdinaryRel
			require.NoError(t, schema.AppendCol("id", types.T_int32.ToType()))
			require.NoError(t, schema.AppendCol("tenant", types.T_int32.ToType()))
			require.NoError(t, schema.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
			schema.ColDefs[2].Hidden = true
			require.NoError(t, schema.Finalize(false))
			_, created := testutil.CreateRelation(t, h.db, testutil.DefaultTestDB, schema, true)
			databaseID := created.GetMeta().(*catalog.TableEntry).GetDB().ID
			oldTxn, old := testutil.GetDefaultRelation(t, h.db, schema.Name)
			defer func() { _ = oldTxn.Rollback(context.Background()) }()
			before := old.Schema(false).(*catalog.Schema).Clone()

			add := api.NewAddColumnReq(databaseID, created.ID(), "added", types.NewProtoType(types.T_int32), -1)
			if birthFalse {
				added := add.GetAddColumn()
				next := before.Extra.NextColSeqnum
				added.Column.Typ = types.NewProtoType(types.T_bool)
				added.Column.ColId = uint64(next) + 1
				added.Column.Seqnum = next
				added.Column.Default = &plan.Default{NullAbility: true, OriginString: "false", Expr: &plan.Expr{Typ: added.Column.Typ,
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}
				added.ExpectedSchemaVersion = before.Version
				added.ExpectedNextColSeqnum = next
				added.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: next, PkSeqnum: next - 1,
					Columns: []*plan.ColumnBirthFill{{Seqnum: next, BirthAppendOrdinal: 1, Type: added.Column.Typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}
			}
			encoded, err := add.Marshal()
			require.NoError(t, err)
			bat := batch.NewWithSize(1)
			bat.Attrs = []string{"alter_table"}
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			defer bat.Clean(h.m)
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], encoded, false, h.m))
			bat.SetRowCount(1)
			entry, err := makePBEntry(INSERT, databaseID, created.ID(), testutil.DefaultTestDB, schema.Name, "", bat)
			require.NoError(t, err)
			entry.EntryType = api.Entry_Alter
			payload, err := (&api.PrecommitWriteCmd{EntryList: []*api.Entry{entry}}).MarshalBinary()
			require.NoError(t, err)
			request := makeCommitRequest(payload)
			meta := mock1PCTxn(h.db)
			action, message := "echo", "injected after TN schema apply"
			if canceled {
				action, message = "wait", ""
			}
			require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", action, 0, message, false))
			commitCtx, cancelCommit := context.WithCancel(ctx)
			defer cancelCommit()
			done := make(chan error, 1)
			finished := false
			defer func() {
				cancelCommit()
				_, _ = fault.RemoveFaultPoint(context.Background(), point)
				_, _ = fault.RemoveFaultPoint(context.Background(), point+"/waiters")
				if !finished {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("TN commit cleanup failed to join")
					}
				}
			}()
			go func() {
				_, commitErr := h.HandleCommit(commitCtx, meta, nil, request)
				done <- commitErr
			}()
			var running txnif.AsyncTxn
			if canceled {
				require.NoError(t, fault.AddFaultPoint(ctx, point+"/waiters", ":::", "getwaiters", 0, point, false))
				require.Eventually(t, func() bool {
					waiters, _, _ := fault.TriggerFault(point + "/waiters")
					return waiters == 1
				}, 5*time.Second, time.Millisecond)
				running, err = h.db.GetTxnByID(meta.ID)
				require.NoError(t, err)
				db, openErr := running.GetDatabaseByID(databaseID)
				require.NoError(t, openErr)
				rel, openErr := db.GetRelationByID(created.ID())
				require.NoError(t, openErr)
				applied := rel.Schema(false).(*catalog.Schema)
				if birthFalse {
					require.Equal(t, add.GetAddColumn().AppendMissingState, applied.Extra.AppendMissingState)
				}
				require.Equal(t, before.Version+1, applied.Version)
				require.Equal(t, before.Extra.NextColSeqnum+1, applied.Extra.NextColSeqnum)
				require.Contains(t, applied.NameMap, "added")
				require.Equal(t, before.ColDefs, old.Schema(false).(*catalog.Schema).Clone().ColDefs)
				cancelCommit()
			}
			select {
			case commitErr := <-done:
				finished = true
				if canceled {
					require.ErrorIs(t, commitErr, context.Canceled)
					require.Equal(t, txnif.TxnStateRollbacked, running.GetTxnState(false))
					waiters, _, _ := fault.TriggerFault(point + "/waiters")
					require.Zero(t, waiters)
				} else {
					require.ErrorContains(t, commitErr, message)
				}
			case <-ctx.Done():
				t.Fatal("TN commit failed to exit")
			}
			_, err = fault.RemoveFaultPoint(ctx, point)
			require.NoError(t, err)
			freshTxn, fresh := testutil.GetDefaultRelation(t, h.db, schema.Name)
			defer func() { _ = freshTxn.Rollback(context.Background()) }()
			restored := fresh.Schema(false).(*catalog.Schema)
			require.Equal(t, before.Version, restored.Version)
			require.Equal(t, before.Extra.NextColSeqnum, restored.Extra.NextColSeqnum)
			require.Equal(t, before.ColDefs, restored.Clone().ColDefs)
			require.Equal(t, before.NameMap, restored.NameMap)
			require.Equal(t, before.SeqnumMap, restored.SeqnumMap)
			require.Equal(t, before.Extra.AppendMissingState, restored.Extra.AppendMissingState)
			require.NoError(t, freshTxn.Rollback(ctx))
			_, err = h.HandleCommit(ctx, mock1PCTxn(h.db), nil, request)
			require.NoError(t, err)
			successTxn, success := testutil.GetDefaultRelation(t, h.db, schema.Name)
			defer func() { _ = successTxn.Rollback(context.Background()) }()
			final := success.Schema(false).(*catalog.Schema)
			require.Equal(t, before.Version+1, final.Version)
			require.Equal(t, before.Extra.NextColSeqnum+1, final.Extra.NextColSeqnum)
			require.Contains(t, final.NameMap, "added")
			if birthFalse {
				require.Equal(t, add.GetAddColumn().AppendMissingState, final.Extra.AppendMissingState)
				require.Equal(t, add.GetAddColumn().Column.Default.OriginString, "false")
				stored, err := types.Encode(add.GetAddColumn().Column.Default)
				require.NoError(t, err)
				require.Equal(t, stored, final.ColDefs[3].Default)
			}
		})
	}
}
