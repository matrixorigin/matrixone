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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// The D220 (r100) C.1 pins: every pk-move provenance read runs under the
// physical account the routing layer authorized for its segment
// (pkMoveLinkTableWindow.ident), never under the applying session's
// account. The subscription defect these tests pin: a subscriber-side
// MERGE/PICK read the SUBSCRIBER's empty mo_data_branch_update_link, saw
// complete absence, and degraded a recorded publisher-side pk move to
// DELETE+INSERT replay (silent ON DELETE CASCADE child loss).

// accountCapturingBh serves canned query results while recording the
// account attached to every Exec context, proving which physical catalog
// each provenance read targeted.
type accountCapturingBh struct {
	t        *testing.T
	ctrl     *gomock.Controller
	bh       *mock_frontend.MockBackgroundExec
	accounts []uint32
	// sqls records every executed statement so snapshot-anchor pins can
	// assert the probe's MO_TS clause; serveSQL, when set, resolves the
	// page from the caller account AND the statement text.
	sqls     []string
	serveSQL func(account uint32, sql string) []interface{}
	// serve resolves one page per executed statement from the caller
	// account of the LAST recorded Exec.
	serve func(account uint32) []interface{}
}

func newAccountCapturingBh(
	t *testing.T,
	ctrl *gomock.Controller,
	serve func(account uint32) []interface{},
) *accountCapturingBh {
	h := &accountCapturingBh{t: t, ctrl: ctrl, serve: serve}
	h.bh = mock_frontend.NewMockBackgroundExec(ctrl)
	h.bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, sql string) error {
			account, err := defines.GetAccountId(ctx)
			h.sqls = append(h.sqls, sql)
			if err != nil {
				h.accounts = append(h.accounts, ^uint32(0))
				return nil
			}
			h.accounts = append(h.accounts, account)
			return nil
		},
	).AnyTimes()
	h.bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
		if len(h.accounts) == 0 {
			return []interface{}{buildPkMovePickProbeResultSet()}
		}
		if h.serveSQL != nil {
			return h.serveSQL(
				h.accounts[len(h.accounts)-1], h.sqls[len(h.sqls)-1])
		}
		return h.serve(h.accounts[len(h.accounts)-1])
	}).AnyTimes()
	h.bh.EXPECT().ClearExecResultSet().AnyTimes()
	return h
}

func d220OneRow(t *testing.T) []interface{} {
	rowID := buildHashDiffRowID(t, 1)
	return []interface{}{buildPkMovePickProbeResultSet(
		[]interface{}{string(rowID[:]), string([]byte{1}), string([]byte{2}), int64(1)},
	)}
}

func d220EmptyRows() []interface{} {
	return []interface{}{buildPkMovePickProbeResultSet()}
}

// TestPkMoveLinkWindowsFromRangeProjectsSegmentIdentities pins D220 (B.1):
// the window projection carries collectRange.idents' routing winner for
// every segment, and a range without routing identities (unit fixtures,
// pre-routing constructions) projects the zero identity.
func TestPkMoveLinkWindowsFromRangeProjectsSegmentIdentities(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	relA := mock_frontend.NewMockRelation(ctrl)
	relA.EXPECT().GetTableID(gomock.Any()).Return(uint64(111)).AnyTimes()
	relB := mock_frontend.NewMockRelation(ctrl)
	relB.EXPECT().GetTableID(gomock.Any()).Return(uint64(222)).AnyTimes()

	base := defines.AttachAccountId(context.Background(), 100)
	idents := []branchIdentity{
		{account: 200, ctx: base},
		{account: 300, ctx: base},
	}
	r := collectRange{
		rel:    []engine.Relation{relA, relB},
		from:   []types.TS{types.BuildTS(10, 0), types.BuildTS(11, 0)},
		end:    []types.TS{types.BuildTS(20, 0), types.BuildTS(21, 0)},
		idents: idents,
	}
	wins := pkMoveLinkWindowsFromRange(base, r)
	require.Len(t, wins, 2)
	require.Equal(t, uint64(111), wins[0].tableID)
	require.Equal(t, uint64(20), wins[0].endPhys)
	require.Equal(t, idents[0], wins[0].ident,
		"segment 0 keeps the physical owner it was opened under")
	require.Equal(t, idents[1], wins[1].ident,
		"segment 1 keeps the physical owner it was opened under")

	rNoIdents := r
	rNoIdents.idents = nil
	wins = pkMoveLinkWindowsFromRange(base, rNoIdents)
	require.Len(t, wins, 2)
	require.Equal(t, branchIdentity{}, wins[0].ident,
		"an unrouted range projects the zero identity (caller-context posture)")
	require.Equal(t, branchIdentity{}, wins[1].ident)
}

// TestPkMoveWindowReadCtxAttachesOwner pins D220 (B.2)'s context rule: a
// routing-resolved segment identity re-attaches its owner account, an
// identity already matching the caller keeps the exact caller context
// (local-local diffs pay nothing), and a zero or context-less identity
// keeps the caller context.
func TestPkMoveWindowReadCtxAttachesOwner(t *testing.T) {
	base := defines.AttachAccountId(context.Background(), 100)

	zero := pkMoveLinkTableWindow{tableID: 7}
	require.True(t, zero.readCtx(base) == base,
		"zero identity keeps the caller context byte-identically")

	same := pkMoveLinkTableWindow{
		tableID: 7,
		ident:   branchIdentity{account: 100, ctx: base},
	}
	require.True(t, same.readCtx(base) == base,
		"an identity equal to the caller account attaches nothing")

	other := pkMoveLinkTableWindow{
		tableID: 7,
		ident:   branchIdentity{account: 200, ctx: base},
	}
	rctx := other.readCtx(base)
	require.False(t, rctx == base, "a different owner derives a new context")
	account, err := defines.GetAccountId(rctx)
	require.NoError(t, err)
	require.Equal(t, uint32(200), account,
		"the derived context carries the segment's physical owner")

	noCtx := pkMoveLinkTableWindow{
		tableID: 7,
		ident:   branchIdentity{account: 200},
	}
	require.True(t, noCtx.readCtx(base) == base,
		"a context-less identity cannot attach and keeps the caller context")
}

// TestPkMoveProviderReadsRunUnderOwnerCtx pins D220 (B.2) across the
// provider's three SQL read paths: the whole-window probe, the per-key
// overflow probe, and the LIMIT 1 existence scan all run under the
// window's physical owner account, not the applying session's account.
func TestPkMoveProviderReadsRunUnderOwnerCtx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), 100)
	ses := newValidateSession(t)
	ownerWindow := pkMoveLinkTableWindow{
		tableID: 88,
		endPhys: 777,
		ident:   branchIdentity{account: 200, ctx: ctx},
	}
	localWindow := pkMoveLinkTableWindow{
		tableID: 88,
		endPhys: 777,
		ident:   branchIdentity{account: 100, ctx: ctx},
	}

	t.Run("the whole-window probe reads the owner catalog", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} {
			return []interface{}{buildPkMoveLinkProbeResultSet()}
		})
		p := newPkMoveLinkProvider(ctx, ses, h.bh, []pkMoveLinkTableWindow{ownerWindow})
		require.NoError(t, p.probeOldPKs(ownerWindow, [][]byte{{1}}))
		require.Equal(t, []uint32{200}, h.accounts,
			"the subscriber-side diff must read the publisher's link catalog")
	})

	t.Run("the per-key overflow probe reads the owner catalog", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} {
			return []interface{}{buildPkMoveLinkProbeResultSet()}
		})
		p := newPkMoveLinkProvider(ctx, ses, h.bh, []pkMoveLinkTableWindow{ownerWindow})
		require.NoError(t, p.probeKeysIndividually(ownerWindow, [][]byte{{1}}))
		require.Equal(t, []uint32{200}, h.accounts)
	})

	t.Run("the LIMIT 1 existence scan reads the owner catalog", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} {
			return []interface{}{buildPkMoveLinkProbeResultSet()}
		})
		p := newPkMoveLinkProvider(ctx, ses, h.bh, []pkMoveLinkTableWindow{ownerWindow})
		exists, err := p.probeAnyLinkInWindow(ownerWindow)
		require.NoError(t, err)
		require.False(t, exists)
		require.Equal(t, []uint32{200}, h.accounts)
	})

	t.Run("a caller-owned window keeps the caller account", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} {
			return []interface{}{buildPkMoveLinkProbeResultSet()}
		})
		p := newPkMoveLinkProvider(ctx, ses, h.bh, []pkMoveLinkTableWindow{localWindow})
		require.NoError(t, p.probeOldPKs(localWindow, [][]byte{{1}}))
		require.Equal(t, []uint32{100}, h.accounts,
			"local-local diffs keep their pre-routing account byte-identically")
	})
}

// TestPkMoveCaptureStreamsUnderOwnerCtx pins D220 (B.2) on the D156
// scope-to-commit capture path: the capture context handed to the stream
// hook (and, through the same derivation, to relation resolution and the
// CollectChanges call) carries the window's physical owner.
func TestPkMoveCaptureStreamsUnderOwnerCtx(t *testing.T) {
	base := defines.AttachAccountId(context.Background(), 100)

	build := func(window pkMoveLinkTableWindow, got *[]uint32) *pkMoveLinkProvider {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 30, 3, 31, 10))
		p.ctx = base
		p.windows = []pkMoveLinkTableWindow{window}
		p.streamLinkCommits = func(
			ctx context.Context, _ pkMoveLinkTableWindow, _, _ types.TS,
			accept func(scope pkMoveTxnScope, cts types.TS) error,
		) error {
			account, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			*got = append(*got, account)
			return accept(pkMoveTxnScope{writeTS: types.BuildTS(10, 0)}, types.BuildTS(20, 0))
		}
		return p
	}

	var got []uint32
	p := build(pkMoveLinkTableWindow{
		tableID: 1,
		ident:   branchIdentity{account: 200, ctx: base},
	}, &got)
	p.captureArrivalCommitsLocked()
	require.Equal(t, []uint32{200}, got,
		"the capture streams the publisher's link relation, not the subscriber's")
	require.NotNil(t, p.arrivalCommitIdx,
		"the admitted scope still lands in the captured index")

	got = nil
	p2 := build(pkMoveLinkTableWindow{tableID: 1}, &got)
	p2.captureArrivalCommitsLocked()
	require.Equal(t, []uint32{100}, got,
		"a zero-identity window captures under the caller account")
	require.NotNil(t, p2.arrivalCommitIdx)
}

// TestPkMoveWindowsWithCatalogDegradesPerAccount pins D220 (B.4): the
// catalog existence probe runs once per owner account under that account,
// and a pre-upgrade owner's absence drops exactly its own windows while
// every other account's windows stay served.
func TestPkMoveWindowsWithCatalogDegradesPerAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := defines.AttachAccountId(context.Background(), 100)
	ses := newValidateSession(t)
	wPublisher := pkMoveLinkTableWindow{
		tableID: 1,
		endPhys: 50,
		ident:   branchIdentity{account: 200, ctx: base},
	}
	wPreUpgrade := pkMoveLinkTableWindow{
		tableID: 2,
		endPhys: 60,
		ident:   branchIdentity{account: 300, ctx: base},
	}
	wLocal := pkMoveLinkTableWindow{tableID: 3, endPhys: 70}

	h := newAccountCapturingBh(t, ctrl, func(account uint32) []interface{} {
		if account == 300 {
			return d220EmptyRows()
		}
		return d220OneRow(t)
	})
	out, err := pkMoveWindowsWithCatalog(
		base, ses, h.bh,
		[]pkMoveLinkTableWindow{wPublisher, wPreUpgrade, wLocal},
	)
	require.NoError(t, err)
	require.Equal(t, []pkMoveLinkTableWindow{wPublisher, wLocal}, out,
		"only the pre-upgrade account's own windows degrade to absence")
	require.Equal(t, []uint32{200, 300, 100}, h.accounts,
		"existence is probed once per owner account, in first-seen order")

	t.Run("a probe error propagates instead of guessing", func(t *testing.T) {
		injected := moerr.NewInternalErrorNoCtx("existence probe blew up")
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(injected).Times(1)
		_, err := pkMoveWindowsWithCatalog(base, ses, bh, []pkMoveLinkTableWindow{wLocal})
		require.ErrorIs(t, err, injected)
	})
}

// TestResolvePkMoveWindowOwner pins D220 (B.3)'s ordered-candidate rule:
// the first candidate whose catalog owns the physical table wins, a
// single-candidate list pays no catalog probe (local-local collapse), a
// probe error propagates, and no-owner yields the zero identity.
func TestResolvePkMoveWindowOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := defines.AttachAccountId(context.Background(), 100)
	ses := newValidateSession(t)
	cand1 := branchIdentity{account: 200, ctx: base}
	cand2 := branchIdentity{account: 300, ctx: base}

	t.Run("no candidates and a zero table id need no catalog work", func(t *testing.T) {
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		owner, err := resolvePkMoveWindowOwner(base, ses, bh, nil, 42, types.TS{})
		require.NoError(t, err)
		require.Equal(t, branchIdentity{}, owner)
		owner, err = resolvePkMoveWindowOwner(base, ses, bh, []branchIdentity{cand1}, 0, types.TS{})
		require.NoError(t, err)
		require.Equal(t, branchIdentity{}, owner)
	})

	t.Run("a single candidate wins without a probe", func(t *testing.T) {
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		owner, err := resolvePkMoveWindowOwner(base, ses, bh, []branchIdentity{cand1}, 42, types.TS{})
		require.NoError(t, err)
		require.Equal(t, cand1, owner,
			"equal-account collapse keeps local-local reads probe-free")
	})

	t.Run("the first owning candidate wins", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220OneRow(t) })
		owner, err := resolvePkMoveWindowOwner(
			base, ses, h.bh, []branchIdentity{cand1, cand2}, 42, types.TS{})
		require.NoError(t, err)
		require.Equal(t, cand1, owner)
		require.Equal(t, []uint32{200}, h.accounts,
			"a first-candidate hit never probes the second account")
	})

	t.Run("a verified miss advances to the next candidate", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(account uint32) []interface{} {
			if account == 200 {
				return d220EmptyRows()
			}
			return d220OneRow(t)
		})
		owner, err := resolvePkMoveWindowOwner(
			base, ses, h.bh, []branchIdentity{cand1, cand2}, 42, types.TS{})
		require.NoError(t, err)
		require.Equal(t, cand2, owner)
		require.Equal(t, []uint32{200, 300}, h.accounts)
	})

	t.Run("no owning candidate keeps the zero identity", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220EmptyRows() })
		owner, err := resolvePkMoveWindowOwner(
			base, ses, h.bh, []branchIdentity{cand1, cand2}, 42, types.TS{})
		require.NoError(t, err)
		require.Equal(t, branchIdentity{}, owner,
			"an unowned table keeps the pre-routing caller-context posture")
		require.Equal(t, []uint32{200, 300}, h.accounts)
	})

	t.Run("a probe error propagates", func(t *testing.T) {
		injected := moerr.NewInternalErrorNoCtx("owner probe blew up")
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(injected).Times(1)
		_, err := resolvePkMoveWindowOwner(base, ses, bh, []branchIdentity{cand1, cand2}, 42, types.TS{})
		require.ErrorIs(t, err, injected)
	})
}

// TestResolvePkMoveWindowOwnerAnchorsProbeAtSnapshot pins r101 D222: an
// anchored probe reads the candidate's catalog at the historical
// transition snapshot the collect walk opens that segment at, so a
// generation retired from the current catalog by a COPY ALTER still
// resolves its owner; an unanchored probe of the same retired generation
// finds no owner and would re-scope the window's provenance reads to the
// caller.
func TestResolvePkMoveWindowOwnerAnchorsProbeAtSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := defines.AttachAccountId(context.Background(), 100)
	ses := newValidateSession(t)
	cand1 := branchIdentity{account: 200, ctx: base}
	cand2 := branchIdentity{account: 300, ctx: base}
	transition := types.BuildTS(1_700_000_000_000_000_000, 7)

	t.Run("an anchored probe carries the full transition timestamp", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220OneRow(t) })
		owner, err := resolvePkMoveWindowOwner(
			base, ses, h.bh, []branchIdentity{cand1, cand2}, 42, transition)
		require.NoError(t, err)
		require.Equal(t, cand1, owner)
		require.Len(t, h.sqls, 1)
		require.Contains(t, h.sqls[0],
			" {MO_TS = '"+transition.ToTimestamp().DebugString()+"'}",
			"the anchored probe keeps physical AND logical time, matching the reopen walk's snapshot")
	})

	t.Run("an empty snapshot keeps the unanchored probe", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220OneRow(t) })
		_, err := resolvePkMoveWindowOwner(
			base, ses, h.bh, []branchIdentity{cand1, cand2}, 42, types.TS{})
		require.NoError(t, err)
		require.Len(t, h.sqls, 1)
		require.NotContains(t, h.sqls[0], "MO_TS")
	})

	t.Run("a retired generation resolves its owner only at the transition snapshot", func(t *testing.T) {
		// Post-ALTER catalog truth: the retired physical generation is
		// visible only in an anchored read, never in a current-catalog
		// read, and the subscriber never owned it at any instant.
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220EmptyRows() })
		h.serveSQL = func(account uint32, sql string) []interface{} {
			if account == 200 && strings.Contains(sql, " {MO_TS = '"+
				transition.ToTimestamp().DebugString()+"'}") {
				return d220OneRow(t)
			}
			return d220EmptyRows()
		}
		owner, err := resolvePkMoveWindowOwner(
			base, ses, h.bh, []branchIdentity{cand1, cand2}, 42, transition)
		require.NoError(t, err)
		require.Equal(t, cand1, owner,
			"the publisher's anchored catalog owns the retired generation; the probe never reaches the subscriber")
		require.Equal(t, []uint32{200}, h.accounts)

		h2 := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220EmptyRows() })
		owner, err = resolvePkMoveWindowOwner(
			base, ses, h2.bh, []branchIdentity{cand1, cand2}, 42, types.TS{})
		require.NoError(t, err)
		require.Equal(t, branchIdentity{}, owner,
			"the pre-D222 unanchored probe of a retired generation finds no owner")
		require.Equal(t, []uint32{200, 300}, h2.accounts)
	})
}

// TestPkMoveWindowOwnerSnapshot pins the D222 snapshot rule: the path's
// endpoint segment resolves at the endpoint snapshot, an intermediate
// generation at the transition into its successor, and a missing
// transition entry degrades to the endpoint snapshot.
func TestPkMoveWindowOwnerSnapshot(t *testing.T) {
	endpointSP := types.BuildTS(300, 7)
	ts := []types.TS{{}, types.BuildTS(100, 1), types.BuildTS(200, 3)}

	require.Equal(t, ts[1], pkMoveWindowOwnerSnapshot(ts, 0, endpointSP),
		"an intermediate generation resolves at the transition into its successor")
	require.Equal(t, ts[2], pkMoveWindowOwnerSnapshot(ts, 1, endpointSP))
	require.Equal(t, endpointSP, pkMoveWindowOwnerSnapshot(ts, 2, endpointSP),
		"the endpoint segment resolves at the endpoint snapshot")
	require.Equal(t, endpointSP, pkMoveWindowOwnerSnapshot(nil, 0, endpointSP),
		"missing transition bookkeeping degrades to the endpoint snapshot")
	require.Equal(t, endpointSP, pkMoveWindowOwnerSnapshot(ts[:1], 3, endpointSP),
		"an index past the transitions degrades to the endpoint snapshot")
}

// TestPkMovePickProbesRunUnderOwnerCtx pins D220 (B.2) on the PICK closure
// reads: both the whole-window closure scan and the overflow per-key
// fallback probe the physical owner's link catalog.
func TestPkMovePickProbesRunUnderOwnerCtx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), 100)
	ses := newValidateSession(t)
	ownerWindow := pkMoveLinkTableWindow{
		tableID: 88,
		endPhys: 777,
		ident:   branchIdentity{account: 200, ctx: ctx},
	}

	t.Run("the whole-window closure scan reads the owner catalog", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220EmptyRows() })
		_, truncated, err := probePickWindowLinks(ctx, ses, h.bh, ownerWindow, [][]byte{{1}})
		require.NoError(t, err)
		require.False(t, truncated)
		require.Equal(t, []uint32{200}, h.accounts)
	})

	t.Run("the overflow per-key probe reads the owner catalog", func(t *testing.T) {
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220EmptyRows() })
		_, err := probePickWindowKeysIndividually(ctx, ses, h.bh, ownerWindow, [][]byte{{1}})
		require.NoError(t, err)
		require.Equal(t, []uint32{200}, h.accounts)
	})

	t.Run("a zero-identity window reads the caller catalog", func(t *testing.T) {
		plain := pkMoveLinkTableWindow{tableID: 88, endPhys: 777}
		h := newAccountCapturingBh(t, ctrl, func(uint32) []interface{} { return d220EmptyRows() })
		_, _, err := probePickWindowLinks(ctx, ses, h.bh, plain, [][]byte{{1}})
		require.NoError(t, err)
		require.Equal(t, []uint32{100}, h.accounts)
	})
}
