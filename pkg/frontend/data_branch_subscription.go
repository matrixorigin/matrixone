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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// This file implements the shared subscription classification boundary and
// the per-operand identity model of
// docs/design/20261023-subscription-table-branch-snapshot-routing.md (v7,
// approved in pre-pr-review-1792751823194.json) for issue #30949.
//
// A subscription database N in the statement account S is an alias over a
// published database D. Ordinary reads route through getSubscriptionMeta:
// the physical owner identity (SubscriptionMeta.AccountId, P_src) is attached
// to the context and the database name is rebound to the physical name
// (SubscriptionMeta.DbName == D). The DATA BRANCH and snapshot owner paths
// historically bypassed that routing; this file provides the single shared
// classification helper (design §4.1) and the operand descriptor/identity
// primitives every downstream catalog read threads (design §6).

// subscriptionOperand is the classification result for one named operand
// (design §4.1). local operands keep today's behavior byte-for-byte; a
// visible operand carries the resolved (P_pub, P_src, D) routing facts.
type subscriptionOperand struct {
	local bool
	// P_pub: the publication owner. Publication metadata (mo_pubs lookups,
	// membership) is consulted under this identity.
	pubAccount string
	pubName    string
	// P_src: the physical source account that owns D and its tables. Catalog
	// reads and snapshot artifacts targeting the operand run under it.
	physAccount uint32
	physDb      string
	meta        *plan.SubscriptionMeta
}

// resolveBranchSubscriptionMeta is the production binding behind
// classifySubscriptionOperand; unit tests substitute it through the same
// gostub precedent as getPubInfo so classification stays table-driven
// without a subscription-capable engine.
var resolveBranchSubscriptionMeta = getSubscriptionMeta

// classifySubscriptionOperand classifies one table operand (design §4.1):
//
//   - local: dbName is not a live subscription database. The caller keeps
//     today's behavior, including the engine's genuine no-such-table errors.
//   - visible: live subscription and tblName is inside the publication scope.
//     The returned operand carries (P_pub, P_src, D) for routing.
//   - out of scope: live subscription but tblName is not in the publication's
//     table list. outOfScope is true and the caller returns the
//     visibility-cause error for its statement (design §4.1, I5).
//   - liveness failure (revoked publication, suspended publisher, dropped
//     subscription): getSubscriptionMeta's error is propagated unchanged.
//
// tblName may be empty to classify a database-only destination.
func classifySubscriptionOperand(
	ctx context.Context,
	ses *Session,
	txnOp TxnOperator,
	bh BackgroundExec,
	dbName string,
	tblName string,
) (operand *subscriptionOperand, outOfScope bool, err error) {
	meta, err := resolveBranchSubscriptionMeta(ctx, dbName, ses, txnOp, bh)
	if err != nil {
		return nil, false, err
	}
	if meta == nil {
		return &subscriptionOperand{local: true}, false, nil
	}
	if tblName != "" && !pubsub.InSubMetaTables(meta, tblName) {
		return nil, true, nil
	}
	return &subscriptionOperand{
		local:       false,
		pubAccount:  meta.AccountName,
		pubName:     meta.Name,
		physAccount: uint32(meta.AccountId),
		physDb:      meta.DbName,
		meta:        meta,
	}, false, nil
}

// newBranchOperandOutsidePublicationScopeError is the visibility-cause error
// for a table that lives in a subscription database but outside the
// publication's scope (design §4.1, I5). It must stay distinguishable from
// the engine's genuine no-such-table error for a missing local table.
func newBranchOperandOutsidePublicationScopeError(ctx context.Context, dbName, tblName string) error {
	return moerr.NewInternalErrorf(ctx,
		"table %s.%s is outside the publication's scope", dbName, tblName)
}

// newBranchSubscriptionWriteDestinationError rejects DATA BRANCH writes into
// a subscription database (design §4.3, I3), mirroring the C5 prohibition for
// CREATE TABLE in a subscription database.
func newBranchSubscriptionWriteDestinationError(ctx context.Context, op, dbName, tblName string) error {
	return moerr.NewInternalErrorf(ctx,
		"cannot perform DATA BRANCH %s into subscription database %s.%s", op, dbName, tblName)
}

// rejectSubscriptionBranchWriteDestination classifies a MERGE/PICK
// destination; any non-local result is a write-prohibition error raised
// before any write (design §4.1 consumer 3, §4.3).
func rejectSubscriptionBranchWriteDestination(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	op string,
	name tree.TableName,
) error {
	normalized, err := normalizeBranchTableName(ctx, ses, name)
	if err != nil {
		return err
	}
	dbName := normalized.SchemaName.String()
	tblName := normalized.ObjectName.String()
	txnOp := ses.GetTxnHandler().GetTxn()
	operand, outOfScope, err := classifySubscriptionOperand(ctx, ses, txnOp, bh, dbName, tblName)
	if err != nil {
		return err
	}
	if outOfScope || !operand.local {
		return newBranchSubscriptionWriteDestinationError(ctx, op, dbName, tblName)
	}
	return nil
}

// branchIdentity is the resolved per-operand physical identity (design §6):
// every catalog read that targets an operand's physical owner runs under
// ctx (the statement context with the owner account attached) and uses
// account for identity-keyed probes such as the mo_database CP key or a
// tenant-pinned snapshot.
type branchIdentity struct {
	account uint32
	ctx     context.Context
}

// branchOperand is the operand descriptor of design §6.1: one DIFF operand
// or one MERGE/PICK source, with its physical routing resolved by the
// shared classification boundary.
type branchOperand struct {
	physAccount uint32
	physDb      string

	ctx context.Context

	isSubscription bool
}

func (o *branchOperand) identity() branchIdentity {
	return branchIdentity{account: o.physAccount, ctx: o.ctx}
}

// localOperandIdentity describes a non-routed operand: its physical identity
// is the statement account itself.
func localOperandIdentity(ctx context.Context, account uint32) branchIdentity {
	return branchIdentity{account: account, ctx: ctx}
}

// identityCtx derives the execution context for ident from base. The owner
// account is re-attached only when it differs from base's, so the local path
// keeps the exact statement context (including values attached after
// classification, such as the visible-state recovery resources).
func identityCtx(base context.Context, ident branchIdentity) context.Context {
	if ident.ctx == nil {
		return base
	}
	if account, err := defines.GetAccountId(base); err == nil && account == ident.account {
		return base
	}
	return defines.AttachAccountId(base, ident.account)
}

// dedupeBranchIdentities returns the candidates of design §6.4 in their
// deterministic order (walking side's endpoint first, then the opposite
// endpoint's), collapsing equal accounts so a local-local diff performs
// exactly one lookup per node, byte-identical to the pre-change behavior.
func dedupeBranchIdentities(first, second branchIdentity) []branchIdentity {
	if first.account == second.account {
		return []branchIdentity{first}
	}
	return []branchIdentity{first, second}
}

// reopenDataBranchRelationByID is the physical lookup behind
// reopenDataBranchNodeByID; unit tests substitute it (the getPubInfo
// precedent) to drive the candidate order without a storage engine.
var reopenDataBranchRelationByID = getRelationById

// reopenDataBranchNodeByID reopens a non-endpoint lineage node by table id
// under the candidate owners in order (design §6.4). The first candidate
// that resolves the node wins, and the node's tenant-pinned snapshot is set
// from the winning identity. Only a verified missing-relation error advances
// to the next candidate: a wrong-owner absence must never overwrite the true
// owner's storage, transaction, or cancellation failure — the caller folds a
// reported absence into the zero-history degradation, and a masked failure
// would let DIFF/MERGE proceed without available ancestor history. When
// every candidate misses, the last missing error is returned so the caller
// can apply the preserved same-transaction / zero-history degradation.
func reopenDataBranchNodeByID(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	candidates []branchIdentity,
	tableID uint64,
	snapshotTS types.TS,
) (rel engine.Relation, winner branchIdentity, err error) {
	var lastErr error
	for _, candidate := range candidates {
		snapshot := snapshotTS.ToTimestamp()
		rel, err = reopenDataBranchRelationByID(
			identityCtx(ctx, candidate), ses, bh, tableID, &plan.Snapshot{
				Tenant: &plan.SnapshotTenant{TenantID: candidate.account},
				TS:     &snapshot,
			},
		)
		if err == nil {
			return rel, candidate, nil
		}
		if !isMissingDataBranchRelationByID(err) {
			return nil, branchIdentity{}, err
		}
		lastErr = err
	}
	return nil, branchIdentity{}, lastErr
}
