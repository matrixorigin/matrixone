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

package compile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const viewRecoveryMaxWork = 65536
const viewRecoveryMaxStateBytes = 16384

// ViewRecoveryScope describes the restored/invalidated source, not an account
// reset. All is reserved for initial cluster catalog discovery. Empty names
// with All=false mean precisely AccountID, including the system account.
type ViewRecoveryScope struct {
	All       bool   `json:"all,omitempty"`
	AccountID uint32 `json:"account,omitempty"`
	Database  string `json:"database,omitempty"`
	Relation  string `json:"relation,omitempty"`
}

type ViewRecoveryClaim struct {
	Epoch      uint64 `json:"epoch,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	ClaimID    uint64 `json:"claim,omitempty"`
	LeaseEpoch uint64 `json:"lease,omitempty"`
	Owner      string `json:"owner,omitempty"`
}

// ViewRecoveryState is the committed catalog truth. The outbox is exactly three
// slots, not an event log; publication and its evidence cannot commit separately.
type ViewRecoveryState struct {
	Version          uint32 `json:"version"`
	MutationRevision uint64 `json:"mutation,omitempty"`
	catalogMutation  uint64
	ViewRecoveryClaim
	Completed uint64                        `json:"completed,omitempty"`
	Scope     ViewRecoveryScope             `json:"scope"`
	WorkRows  uint64                        `json:"work_rows,omitempty"`
	Outbox    [3]*pb.CatalogMetadataReceipt `json:"outbox"`
}

type viewRecoveryContextKey struct{}

// ViewMetadataCoordinator is an internal, inactive-by-default catalog owner.
// Constructing it starts no worker and grants no SQL or metadata admission.
// Its caller must obtain E/R and the logical ClaimID from the trusted HAKeeper
// coordinator; ordinary SQL/heartbeats cannot invoke these Go APIs.
type ViewMetadataCoordinator struct{ SQL executor.SQLExecutor }

// CatalogReceiptTransport belongs to the authenticated control plane. The
// response must be a linearizable read of committed RSM state, never an enqueue
// ack, heartbeat snapshot, timeout or an unverified caller-supplied boolean.
// No public transport/activation endpoint is installed by this lifecycle layer.
type CatalogReceiptTransport interface {
	ApplyCatalogReceipt(context.Context, *pb.CatalogMetadataReceipt) (*pb.CatalogMetadataBarrierState, error)
}

// CatalogReceiptReader is the optional linearizable readback used to reclaim
// stale evidence without submitting a completion invalidated by later DDL.
type CatalogReceiptReader interface {
	ReadCatalogBarrier(context.Context) (*pb.CatalogMetadataBarrierState, error)
}

func viewRecoveryExec(txn executor.TxnExecutor, sql string) (uint64, error) {
	result, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return 0, err
	}
	defer result.Close()
	return result.AffectedRows, nil
}

func viewRecoveryStrings(result executor.Result, count int) ([][]string, error) {
	defer result.Close()
	rows := make([][]string, 0)
	var err error
	result.ReadRows(func(n int, columns []*vector.Vector) bool {
		if len(columns) != count || n < 0 || len(rows)+n > viewMetadataRecoveryPageSize {
			err = moerr.NewInvalidStateNoCtx("invalid View recovery catalog page")
			return false
		}
		for i := 0; i < n; i++ {
			row := make([]string, count)
			for j := range columns {
				row[j] = columns[j].GetStringAt(i)
			}
			rows = append(rows, row)
		}
		return true
	})
	return rows, err
}

func loadViewRecovery(txn executor.TxnExecutor) (ViewRecoveryState, uint64, bool, error) {
	var state ViewRecoveryState
	result, err := txn.Exec("select state,cast(revision as char),case when lease_expires_at is null or lease_expires_at<=now() then '1' else '0' end,cast(mutation_revision as char) from mo_catalog.mo_view_recovery where id=1 for update", executor.StatementOption{})
	if err != nil {
		return state, 0, false, err
	}
	rows, err := viewRecoveryStrings(result, 4)
	if err != nil {
		return state, 0, false, err
	}
	if len(rows) != 1 {
		return state, 0, false, moerr.NewInvalidStateNoCtx("View recovery coordinator is not initialized")
	}
	if len(rows[0][0]) > viewRecoveryMaxStateBytes {
		return state, 0, false, moerr.NewInvalidStateNoCtx("View recovery state exceeds budget")
	}
	decoder := json.NewDecoder(strings.NewReader(rows[0][0]))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&state); err != nil {
		return state, 0, false, err
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return state, 0, false, moerr.NewInvalidStateNoCtx("invalid trailing View recovery state")
	}
	if err = state.validate(); err != nil {
		return state, 0, false, err
	}
	state.catalogMutation, err = viewRecoveryUint(rows[0][3])
	if err != nil {
		return state, 0, false, err
	}
	revision, err := viewRecoveryUint(rows[0][1])
	return state, revision, rows[0][2] == "1", err
}

func (s *ViewRecoveryState) validate() error {
	if s.Version != 1 || s.Completed > s.Generation || s.WorkRows > viewRecoveryMaxWork || len(s.Owner) > 128 ||
		(s.Generation == 0 && (s.Epoch != 0 || s.ClaimID != 0 || s.LeaseEpoch != 0 || s.Owner != "")) ||
		(s.Generation != 0 && s.Epoch == 0) || (s.ClaimID == 0 && s.Owner != "") {
		return moerr.NewInvalidStateNoCtx("invalid View recovery state")
	}
	if (s.Scope.Relation != "" && s.Scope.Database == "") || len(s.Scope.Database) > 5000 || len(s.Scope.Relation) > 5000 ||
		(s.Scope.All && (s.Scope.AccountID != 0 || s.Scope.Database != "" || s.Scope.Relation != "")) {
		return moerr.NewInvalidInputNoCtx("invalid View recovery scope")
	}
	for slot, r := range s.Outbox {
		if r == nil {
			continue
		}
		if r.MembershipEpoch != s.Epoch || r.RequiredGeneration != s.Generation || r.Action != viewRecoveryActions[slot] ||
			len(r.Digest) != sha256.Size || (slot == 0 && r.ClaimID != 0) || (slot != 0 && (r.ClaimID == 0 || r.ClaimID != s.ClaimID)) {
			return moerr.NewInvalidStateNoCtx("invalid View recovery outbox identity")
		}
	}
	return nil
}

var viewRecoveryActions = [3]pb.CatalogMetadataAction{pb.CATALOG_ACTION_CATALOG_REQUIRED, pb.CATALOG_ACTION_RECOVERY_STARTED, pb.CATALOG_ACTION_COMPLETE}

func (s *ViewRecoveryState) evidence(slot int) {
	claim := s.ClaimID
	if slot == 0 {
		claim = 0
	}
	// Fixed struct field order is the version-1 digest encoding. Lease ownership
	// deliberately is not part of a logical claim's evidence: takeover replays it.
	encoded, _ := json.Marshal(struct {
		Version                  uint32
		Epoch, Generation, Claim uint64
		Mutation                 uint64
		Action                   pb.CatalogMetadataAction
		Scope                    ViewRecoveryScope
	}{1, s.Epoch, s.Generation, claim, s.MutationRevision, viewRecoveryActions[slot], s.Scope})
	digest := sha256.Sum256(encoded)
	s.Outbox[slot] = &pb.CatalogMetadataReceipt{MembershipEpoch: s.Epoch, RequiredGeneration: s.Generation, ClaimID: claim, Action: viewRecoveryActions[slot], Digest: append([]byte(nil), digest[:]...)}
}

func (c ViewMetadataCoordinator) transaction(ctx context.Context, fn func(executor.TxnExecutor, *ViewRecoveryState, bool) (bool, error)) error {
	if c.SQL == nil {
		return moerr.NewInvalidStateNoCtx("View recovery SQL executor is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
	defer cancel()
	return c.SQL.ExecTxn(process.WithSystemCTELimits(ctx), func(txn executor.TxnExecutor) error {
		return updateViewRecoveryTxn(txn, fn)
	}, executor.Options{}.WithAccountID(catalog.System_Account).
		WithResolveVariableFunc(executor.DefaultResolveVariable).WithWaitCommittedLogApplied())
}

func updateViewRecoveryTxn(txn executor.TxnExecutor, fn func(executor.TxnExecutor, *ViewRecoveryState, bool) (bool, error)) error {
	if err := catalog.LockViewMetadataLifecycle(func(sql string) error { _, err := viewRecoveryExec(txn, sql); return err }); err != nil {
		return err
	}
	state, revision, expired, err := loadViewRecovery(txn)
	if err != nil {
		return err
	}
	originalClaim := state.ViewRecoveryClaim
	renew, err := fn(txn, &state, expired)
	if err != nil {
		return err
	}
	if err = state.validate(); err != nil {
		return err
	}
	if revision == math.MaxUint64 {
		return moerr.NewInvalidStateNoCtx("View recovery revision exhausted")
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(data) > viewRecoveryMaxStateBytes {
		return moerr.NewInvalidStateNoCtx("View recovery state exceeds budget")
	}
	deadline := ""
	if renew {
		deadline = ",lease_expires_at=date_add(now(),interval 60 second)"
	}
	predicate := ""
	if !expired && originalClaim.Owner != "" && state.ViewRecoveryClaim == originalClaim {
		// A page must not renew or complete a lease that expired during its work.
		// Expired takeover changes L, and therefore does not use this predicate.
		predicate = " and lease_expires_at>now()"
	}
	affected, err := viewRecoveryExec(txn, fmt.Sprintf("update mo_catalog.mo_view_recovery set state='%s',revision=%d%s where id=1 and revision=%d%s", sqlquote.EscapeString(string(data)), revision+1, deadline, revision, predicate))
	if err != nil {
		return err
	}
	if affected != 1 {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return nil
}

// Require atomically installs one immutable recovery scope and required outbox.
// A duplicate must describe the same scope. Old evidence must be acknowledged
// or proven retired before a new generation can reuse the three outbox slots.
func (c ViewMetadataCoordinator) Require(ctx context.Context, epoch, generation uint64, scope ViewRecoveryScope) error {
	return c.transaction(ctx, requireViewRecovery(epoch, generation, scope))
}

// RequireViewMetadataRecoveryInTxn is the DDL/restore transaction boundary. Its
// caller already owns the sealed HAKeeper E/R and calls this AFTER its object
// mutations, before committing that same transaction. A rollback rolls back both
// restored objects and this required marker/outbox; it must never be ignored.
func RequireViewMetadataRecoveryInTxn(txn executor.TxnExecutor, epoch, generation uint64, scope ViewRecoveryScope) error {
	return updateViewRecoveryTxn(txn, requireViewRecovery(epoch, generation, scope))
}

func requireViewRecovery(epoch, generation uint64, scope ViewRecoveryScope) func(executor.TxnExecutor, *ViewRecoveryState, bool) (bool, error) {
	return func(txn executor.TxnExecutor, s *ViewRecoveryState, _ bool) (bool, error) {
		if epoch == 0 || generation == 0 {
			return false, moerr.NewInvalidInputNoCtx("zero View recovery generation")
		}
		if epoch == s.Epoch && generation == s.Generation {
			if scope != s.Scope || s.MutationRevision != s.catalogMutation {
				return false, moerr.NewInvalidStateNoCtx("conflicting View recovery scope")
			}
			return false, nil
		}
		if epoch <= s.Epoch || generation <= s.Generation {
			return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		if s.Completed == 0 && !scope.All {
			return false, moerr.NewInvalidStateNoCtx("initial View recovery requires full catalog discovery")
		}
		if s.Generation > s.Completed && !viewRecoveryScopeContains(scope, s.Scope) {
			return false, moerr.NewInvalidStateNoCtx("new View recovery scope omits unfinished work")
		}
		for _, r := range s.Outbox {
			if r != nil {
				return false, moerr.NewInvalidStateNoCtx("View recovery outbox backpressure")
			}
		}
		s.Epoch = epoch
		s.Generation = generation
		s.Scope = scope
		s.MutationRevision = s.catalogMutation
		s.ClaimID = 0
		s.Owner = ""
		if err := s.validate(); err != nil {
			return false, err
		}
		for _, kind := range []string{"roots", "scan", "orphan", "edges"} {
			if err := enqueueViewRecoveryWork(txn, s, viewRecoveryWork{kind: kind}); err != nil {
				return false, err
			}
		}
		s.evidence(0)
		return false, nil
	}
}

func viewRecoveryScopeContains(parent, child ViewRecoveryScope) bool {
	if parent.All {
		return true
	}
	if child.All || parent.AccountID != child.AccountID {
		return false
	}
	if parent.Database == "" {
		return true
	}
	return parent.Database == child.Database && (parent.Relation == "" || parent.Relation == child.Relation)
}

// Claim retains logical ClaimID across physical worker crash/restart. The
// catalog lease epoch, not time or UUID reuse, fences the old physical owner.
func (c ViewMetadataCoordinator) Claim(ctx context.Context, epoch, generation, claimID uint64, owner string) (ViewRecoveryClaim, error) {
	var claim ViewRecoveryClaim
	err := c.transaction(ctx, func(_ executor.TxnExecutor, s *ViewRecoveryState, expired bool) (bool, error) {
		if owner == "" || len(owner) > 128 || claimID == 0 {
			return false, moerr.NewInvalidInputNoCtx("invalid View recovery claimant")
		}
		if s.MutationRevision != s.catalogMutation || s.Epoch != epoch || s.Generation != generation || s.Completed == generation || (s.ClaimID != 0 && s.ClaimID != claimID) {
			return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		if s.Owner != "" && s.Owner != owner && !expired {
			return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		if s.Owner != owner || expired {
			if s.LeaseEpoch == math.MaxUint64 {
				return false, moerr.NewInvalidStateNoCtx("View recovery lease exhausted")
			}
			s.LeaseEpoch++
		}
		first := s.ClaimID == 0
		s.ClaimID = claimID
		s.Owner = owner
		claim = s.ViewRecoveryClaim
		if first {
			s.evidence(1)
		}
		return true, nil
	})
	if err != nil {
		return ViewRecoveryClaim{}, err
	}
	return claim, nil
}

func (s *ViewRecoveryState) check(claim ViewRecoveryClaim, expired bool) error {
	if s.MutationRevision != s.catalogMutation || claim.Owner == "" || claim.ClaimID == 0 || s.ViewRecoveryClaim != claim || expired || s.Completed == s.Generation {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return nil
}

// Page performs one bounded transaction. Callers schedule ticks; this API never
// spins on unavailable dependencies and starts no goroutine or timer.
func (c ViewMetadataCoordinator) Page(ctx context.Context, claim ViewRecoveryClaim) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
	defer cancel()
	progressed := false
	var attempted viewRefreshTarget
	ctx = context.WithValue(ctx, viewRecoveryContextKey{}, claim)
	err := c.transaction(ctx, func(txn executor.TxnExecutor, s *ViewRecoveryState, expired bool) (bool, error) {
		if err := s.check(claim, expired); err != nil {
			return false, err
		}
		var err error
		progressed, err = advanceViewRecoveryPage(txn, s, &attempted)
		return err == nil, err
	})
	if err != nil && attempted.relationID != 0 && classifyViewRefreshFailure(err).code == viewRefreshFailureTxnConflict {
		// The failed transaction cannot persist its own backoff. Fence a separate
		// transaction with both the coordinator claim and the attempted target V.
		progressed = false
		err = c.transaction(ctx, func(txn executor.TxnExecutor, s *ViewRecoveryState, expired bool) (bool, error) {
			if err := s.check(claim, expired); err != nil {
				return false, err
			}
			affected, err := viewRecoveryExec(txn, fmt.Sprintf("update mo_catalog.mo_view_refresh set next_retry_at=date_add(now(),interval 2 second),attempts=attempts+1 where account_id=%d and target_relation_id=%d and target_generation=%d and status in ('PENDING','DISCOVERING')", attempted.accountID, attempted.relationID, attempted.generation))
			progressed = affected == 1
			return false, err
		})
	}
	return err == nil && progressed, err
}

func (c ViewMetadataCoordinator) Complete(ctx context.Context, claim ViewRecoveryClaim) error {
	return c.transaction(ctx, func(txn executor.TxnExecutor, s *ViewRecoveryState, expired bool) (bool, error) {
		if s.MutationRevision == s.catalogMutation && claim.Owner != "" && claim.ClaimID != 0 && s.ViewRecoveryClaim == claim && s.Completed == s.Generation {
			return false, nil
		}
		if err := s.check(claim, expired); err != nil {
			return false, err
		}
		pending, err := viewRecoveryHasPending(txn, s.Generation)
		if err != nil {
			return false, err
		}
		if pending {
			return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		s.Completed = s.Generation
		s.evidence(2)
		return false, nil
	})
}

// Cleanup reclaims only permanently obsolete or completed-generation work. It
// is bounded even after repeated supersession, and cannot delete active work.
func (c ViewMetadataCoordinator) Cleanup(ctx context.Context) (bool, error) {
	progress := false
	err := c.transaction(ctx, func(txn executor.TxnExecutor, s *ViewRecoveryState, _ bool) (bool, error) {
		n, err := cleanupViewRecoveryWork(txn, s)
		progress = n != 0
		return false, err
	})
	return err == nil && progress, err
}

// Read returns committed state without retaining a process-local authority cache.
func (c ViewMetadataCoordinator) Read(ctx context.Context) (ViewRecoveryState, error) {
	var state ViewRecoveryState
	err := c.transaction(ctx, func(_ executor.TxnExecutor, s *ViewRecoveryState, _ bool) (bool, error) {
		state = *s
		return false, nil
	})
	return state, err
}

// IsCurrent is a catalog proof only. The authority layer must also validate
// the independently owned HAKeeper epoch and its response/commit fence.
func (c ViewMetadataCoordinator) IsCurrent(ctx context.Context, epoch, generation uint64, account uint32, relation uint64) (bool, error) {
	current := false
	err := c.transaction(ctx, func(txn executor.TxnExecutor, s *ViewRecoveryState, _ bool) (bool, error) {
		if s.MutationRevision != s.catalogMutation || epoch == 0 || generation == 0 || s.Epoch != epoch || s.Generation != generation || s.Completed != generation {
			return false, nil
		}
		rows, err := viewRecoveryQuery(txn, fmt.Sprintf("select r.status from mo_catalog.mo_view_refresh r join mo_catalog.mo_tables t on t.account_id=r.account_id and t.rel_id=r.target_relation_id where r.account_id=%d and r.target_relation_id=%d and t.relkind='v' and r.status='CURRENT' and r.completed_generation=r.target_generation limit 1", account, relation), 1)
		current = len(rows) == 1
		return false, err
	})
	return current, err
}

func viewRecoveryReceiptProven(receipt *pb.CatalogMetadataReceipt, state *pb.CatalogMetadataBarrierState) bool {
	if state == nil || !state.EvidenceInitialized || state.Arbitration == nil {
		return false
	}
	if state.MembershipEpoch > receipt.MembershipEpoch && state.RequiredGeneration > receipt.RequiredGeneration {
		return true
	}
	if state.MembershipEpoch != receipt.MembershipEpoch || state.RequiredGeneration != receipt.RequiredGeneration {
		return false
	}
	if receipt.ClaimID != 0 && state.Arbitration.ClaimID > receipt.ClaimID {
		return true
	}
	var accepted *pb.CatalogMetadataReceipt
	switch receipt.Action {
	case pb.CATALOG_ACTION_CATALOG_REQUIRED:
		accepted = state.Arbitration.RequiredReceipt
	case pb.CATALOG_ACTION_RECOVERY_STARTED:
		accepted = state.Arbitration.StartedReceipt
	case pb.CATALOG_ACTION_COMPLETE:
		accepted = state.Arbitration.CompletedReceipt
	}
	return accepted != nil && accepted.MembershipEpoch == receipt.MembershipEpoch && accepted.RequiredGeneration == receipt.RequiredGeneration && accepted.ClaimID == receipt.ClaimID && accepted.Action == receipt.Action && bytes.Equal(accepted.Digest, receipt.Digest)
}

// Publish sends one already committed outbox entry, then conditionally reclaims
// that exact entry. A lost response or failed deletion is safely replayable.
func (c ViewMetadataCoordinator) Publish(ctx context.Context, transport CatalogReceiptTransport) (bool, error) {
	ctx, cancelCall := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
	defer cancelCall()
	if transport == nil {
		return false, moerr.NewInvalidStateNoCtx("View recovery receipt transport is unavailable")
	}
	s, err := c.Read(ctx)
	if err != nil {
		return false, err
	}
	slot := -1
	for i, r := range s.Outbox {
		if r != nil {
			slot = i
			break
		}
	}
	if slot < 0 {
		return false, nil
	}
	receipt := s.Outbox[slot]
	callCtx, cancel := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
	defer cancel()
	var observed *pb.CatalogMetadataBarrierState
	if receipt.Action == pb.CATALOG_ACTION_COMPLETE && s.MutationRevision != s.catalogMutation {
		reader, ok := transport.(CatalogReceiptReader)
		if !ok {
			return false, moerr.NewInvalidStateNoCtx("dirty View recovery completion requires committed readback")
		}
		observed, err = reader.ReadCatalogBarrier(callCtx)
	} else {
		observed, err = transport.ApplyCatalogReceipt(callCtx, receipt)
	}
	if err != nil {
		return false, err
	}
	if !viewRecoveryReceiptProven(receipt, observed) {
		return false, moerr.NewInvalidStateNoCtx("View recovery receipt lacks committed acceptance or retirement proof")
	}
	err = c.transaction(ctx, func(_ executor.TxnExecutor, current *ViewRecoveryState, _ bool) (bool, error) {
		r := current.Outbox[slot]
		if r == nil {
			return false, nil
		}
		if r.MembershipEpoch != receipt.MembershipEpoch || r.RequiredGeneration != receipt.RequiredGeneration || r.ClaimID != receipt.ClaimID || !bytes.Equal(r.Digest, receipt.Digest) {
			return false, moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}
		current.Outbox[slot] = nil
		return false, nil
	})
	return err == nil, err
}

func checkViewRecoveryContext(proc *process.Process) error {
	claim, ok := proc.Ctx.Value(viewRecoveryContextKey{}).(ViewRecoveryClaim)
	if !ok {
		return nil
	}
	value, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInvalidStateNoCtx("View recovery SQL executor is unavailable")
	}
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return value.(executor.SQLExecutor).Exec(proc.Ctx, sql, executor.Options{}.WithTxn(proc.GetTxnOperator()).WithDisableIncrStatement().WithAccountID(catalog.System_Account))
	}, nil)
	s, _, expired, err := loadViewRecovery(txn)
	if err != nil {
		return err
	}
	return s.check(claim, expired)
}
