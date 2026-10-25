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

package frontend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// replayRecoveryChangesHandle resembles the engine's snapshot-recovery
// behavior during Next: the in-Next recovery path reopens the physical table
// by id through Engine.GetRelationById, which resolves in the *context's*
// account. The fake requires that account to be the physical owner; anything
// else is the replay-loss failure the review finding described. When the
// correct owner is attached it replays a deterministic recovered batch.
type replayRecoveryChangesHandle struct {
	owner     uint32
	recovered *batch.Batch
	// observed records every account and live-marker value Next was called
	// with, so the test can prove the context came from the *current* caller.
	mu      sync.Mutex
	seen    []uint32
	markers []any
	closed  bool
}

type liveMarkerContextKey struct{}

func newReplayRecoveryChangesHandle(t *testing.T, owner uint32, mp *mpool.MPool) *replayRecoveryChangesHandle {
	t.Helper()
	recovered := batch.NewWithSize(2)
	recovered.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(recovered.Vecs[0], types.Rowid{}, false, mp))
	recovered.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(recovered.Vecs[1], int64(42), false, mp))
	recovered.SetRowCount(1)
	return &replayRecoveryChangesHandle{owner: owner, recovered: recovered}
}

func (h *replayRecoveryChangesHandle) Next(
	ctx context.Context, mp *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	account, err := defines.GetAccountId(ctx)
	h.mu.Lock()
	h.seen = append(h.seen, account)
	h.markers = append(h.markers, ctx.Value(liveMarkerContextKey{}))
	h.mu.Unlock()
	if err != nil || account != h.owner {
		return nil, nil, engine.ChangesHandle_Tail_done, moerr.NewInternalErrorNoCtxf(
			"replay-loss recovery: physical table resolves only under account %d, got %d",
			h.owner, account,
		)
	}
	out := batch.NewWithSize(h.recovered.VectorCount())
	for i := range h.recovered.Vecs {
		out.Vecs[i] = vector.NewVec(*h.recovered.Vecs[i].GetType())
		if err := out.Vecs[i].UnionBatch(h.recovered.Vecs[i], 0, h.recovered.RowCount(), nil, mp); err != nil {
			return nil, nil, engine.ChangesHandle_Tail_done, err
		}
	}
	out.SetRowCount(h.recovered.RowCount())
	return out, nil, engine.ChangesHandle_Tail_done, nil
}

func (h *replayRecoveryChangesHandle) Close() error {
	h.closed = true
	return nil
}

// TestOwnerScopedChangesHandleReroutesReplayRecovery pins review finding 1 of
// round 3 (design §6, §6.4): the physical identity attached at handle
// construction must survive into every Next, because replay (and its
// snapshot-recovery reopen) runs inside Next. Each segment's owner is derived
// from the *current* caller context by identityCtx, so a subscriber-driven
// diff replays recovered rows under the publisher of the target and base
// segments, while a segment already owned by the caller's account passes its
// live context through byte-identical.
func TestOwnerScopedChangesHandleReroutesReplayRecovery(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 200) // subscriber stmt account
	mp := mpool.MustNewZero()

	requireRecovered := func(t *testing.T, handle engine.ChangesHandle) {
		t.Helper()
		data, tombstone, _, nextErr := handle.Next(ctx, mp)
		require.NoError(t, nextErr)
		require.Nil(t, tombstone)
		require.NotNil(t, data)
		defer data.Clean(mp)
		require.Equal(t, 1, data.RowCount())
		require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](data.Vecs[1])[0])
	}

	t.Run("routed target segment replays recovered rows under its owner", func(t *testing.T) {
		inner := newReplayRecoveryChangesHandle(t, 100, mp)
		handle := scopeChangesHandleToSegmentOwner(inner, branchIdentity{
			account: 100,
			ctx:     context.Background(),
		})
		requireRecovered(t, handle)
		// Unwrapped, the subscriber context would hit the replay-loss failure.
		_, _, _, rawErr := inner.Next(ctx, mp)
		require.Error(t, rawErr)
		require.NoError(t, handle.Close())
		require.True(t, inner.closed)
	})

	t.Run("routed base segment replays recovered rows under its owner", func(t *testing.T) {
		inner := newReplayRecoveryChangesHandle(t, 300, mp)
		handle := scopeChangesHandleToSegmentOwner(inner, branchIdentity{
			account: 300,
			ctx:     context.Background(),
		})
		requireRecovered(t, handle)
		_, _, _, rawErr := inner.Next(ctx, mp)
		require.Error(t, rawErr)
	})

	t.Run("owner context derives from the live caller context", func(t *testing.T) {
		inner := newReplayRecoveryChangesHandle(t, 100, mp)
		handle := scopeChangesHandleToSegmentOwner(inner, branchIdentity{
			account: 100,
			ctx:     context.Background(),
		})
		live := context.WithValue(ctx, liveMarkerContextKey{}, "attached-after-construction")
		data, _, _, nextErr := handle.Next(live, mp)
		require.NoError(t, nextErr)
		data.Clean(mp)
		require.Len(t, inner.markers, 1)
		require.Equal(t, "attached-after-construction", inner.markers[0],
			"the wrapper must not replay the captured resolution context")
	})

	t.Run("locally owned segment passes the caller context through unchanged", func(t *testing.T) {
		inner := newReplayRecoveryChangesHandle(t, 200, mp)
		handle := scopeChangesHandleToSegmentOwner(inner, localOperandIdentity(ctx, 200))
		requireRecovered(t, handle)
		require.Len(t, inner.seen, 1)
		require.Equal(t, uint32(200), inner.seen[0])
	})

	t.Run("identity without a resolution context is a pass-through", func(t *testing.T) {
		inner := newReplayRecoveryChangesHandle(t, 200, mp)
		handle := scopeChangesHandleToSegmentOwner(inner, branchIdentity{account: 100})
		requireRecovered(t, handle)
		require.Equal(t, uint32(200), inner.seen[0])
	})
}

// TestLCAScanDerivesOwnerContextFromLiveCallerContext pins review finding 2 of
// round 3 (design §6.4): the LCA reader-fallback scan attaches the lineage
// node's owner to the *current* caller context. Reusing the resolution-time
// context makes the enclosing probe's child cancellation unable to terminate
// in-flight catalog and storage reads (the async LCA probe creates the child
// and cancels it on failure, then waits for cleanup).
func TestLCAScanDerivesOwnerContextFromLiveCallerContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newSes(nil, ctrl)
	defer ses.Close()
	ctx := defines.AttachAccountId(context.Background(), 200) // subscriber stmt account

	type scanCall struct {
		account uint32
	}
	scanSeen := make(chan scanCall, 4)
	type scannerMode int
	const (
		scannerBlocks scannerMode = iota
		scannerRecovers
	)
	var mode scannerMode
	stub := gostub.Stub(&scanSnapshotRelationByIDWithFallback,
		func(
			scanCtx context.Context,
			_ string,
			_ *Session,
			_ uint64,
			_ types.TS,
			_ engine.Relation,
			attrs []string,
			_ []types.Type,
			_ *plan.Expr,
			_ int,
			onBatch func(*batch.Batch) error,
		) error {
			account, _ := defines.GetAccountId(scanCtx)
			scanSeen <- scanCall{account: account}
			if mode == scannerBlocks {
				<-scanCtx.Done()
				return scanCtx.Err()
			}
			readBatch := batch.NewWithSize(len(attrs))
			mp := ses.proc.Mp()
			for i := range attrs {
				readBatch.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
				if i == 0 {
					if err := vector.AppendBytes(readBatch.Vecs[i], []byte("k1"), false, mp); err != nil {
						return err
					}
				}
			}
			readBatch.SetRowCount(1)
			defer readBatch.Clean(mp)
			return onBatch(readBatch)
		})
	t.Cleanup(stub.Reset)

	fixture := func() (*batch.Batch, tableStuff) {
		lcaRel := mock_frontend.NewMockRelation(ctrl)
		tarRel := mock_frontend.NewMockRelation(ctrl)
		lcaRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(777)).AnyTimes()
		tarRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(777)).AnyTimes()
		lcaRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
		tarRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()

		var tblStuff tableStuff
		tblStuff.lcaRel = lcaRel
		tblStuff.tarRel = tarRel
		tblStuff.tarOperand = &branchOperand{
			userDb: "subdb", userTable: "t",
			physAccount: 100, physDb: "pubdb", tableName: "t",
			// resolution-time context: attached at classification, with no
			// cancellation of its own; the live caller context must win.
			ctx:            context.Background(),
			isSubscription: true,
		}
		tblStuff.def.colNames = []string{"c1"}
		tblStuff.def.colTypes = []types.Type{types.T_varchar.ToType()}
		tblStuff.def.pkColIdx = 0

		tBat := batch.NewWithSize(1)
		tBat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(tBat.Vecs[0], []byte("k1"), false, ses.proc.Mp()))
		tBat.SetRowCount(1)
		return tBat, tblStuff
	}
	snapshotTS := types.BuildTS(100, 1)

	t.Run("child cancellation terminates the blocked in-flight probe", func(t *testing.T) {
		mode = scannerBlocks
		tBat, tblStuff := fixture()
		defer tBat.Clean(ses.proc.Mp())

		child, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, probeErr := runLCAProbeWithReaderFallback(child, ses, tBat, tblStuff, snapshotTS)
			done <- probeErr
		}()

		select {
		case seen := <-scanSeen:
			require.Equal(t, uint32(100), seen.account,
				"the scan must run under the lineage node's owner, not the subscriber")
		case <-time.After(30 * time.Second):
			t.Fatal("reader-fallback scan never started")
		}
		cancel()
		select {
		case probeErr := <-done:
			require.ErrorIs(t, probeErr, context.Canceled)
		case <-time.After(30 * time.Second):
			t.Fatal("cancellation did not unwind the blocked probe")
		}
	})

	t.Run("owner-scoped scan recovers and returns matching rows", func(t *testing.T) {
		mode = scannerRecovers
		tBat, tblStuff := fixture()
		defer tBat.Clean(ses.proc.Mp())

		sqlRet, probeErr := runLCAProbeWithReaderFallback(ctx, ses, tBat, tblStuff, snapshotTS)
		require.NoError(t, probeErr)
		seen := <-scanSeen
		require.Equal(t, uint32(100), seen.account)
		require.NotEmpty(t, sqlRet.Batches)
		require.Equal(t, 1, sqlRet.Batches[0].RowCount(),
			"the row recovered under the segment owner reaches the probe result")
		// The probe output is [input index, target columns...]: c1 is vec 1.
		require.Equal(t, "k1", string(sqlRet.Batches[0].Vecs[1].GetRawBytesAt(0)))
		sqlRet.Close()
	})
}
