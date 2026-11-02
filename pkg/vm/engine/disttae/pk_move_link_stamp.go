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
	"encoding/hex"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"go.uber.org/zap"
)

// D225 (design doc
// docs/design/20261016-data-branch-pk-move-update-provenance.md): the
// writer stages every pk-move link row with a NULL commit_ts, and the CN
// commit-completion hook below stamps the TN-assigned commit instant onto
// exactly this transaction's staged link rows, addressed by their exact
// link_id set, through ONE bounded internal executor channel per account.
// Stamping is best-effort and NEVER changes the user-visible COMMIT
// result: the operator joins completion-callback errors into the commit
// error (pkg/txn/client/operator.go), so the callback swallows every
// failure (log only) and returns nil. A stamping failure, its
// cancellation, a CN crash, or a lost commit response leaves commit_ts
// NULL as the durable stamp-pending intent, and the apply-side D227
// recovery outcome table re-derives the instant (stream heal for a
// stream-bound identity, continuity-pairing inheritance for a
// transaction-local identity, proven window absence, or a loud abort on
// genuinely incomplete or ambiguous evidence).

// pkMoveLinkStampChunk bounds the link_id IN-list of one stamping UPDATE.
// One link id is 64 bytes, so 1024 ids keep one statement near 140KB of
// SQL — well inside the internal executor's limits while bounding the
// statement count of a large moving transaction.
const pkMoveLinkStampChunk = 1024

// pkMoveLinkStampTimeout bounds the whole stamping channel. The callback
// runs before the user's COMMIT response is released, so it must stay
// fast; a timeout simply leaves the rows stamp-pending (D227 recovers).
const pkMoveLinkStampTimeout = 30 * time.Second

// collectPkMoveLinkStampSetLocked scans the converged post-prune staged
// link rows and returns, per account, a COPY of every staged link id. The
// workspace batches may be freed once the commit finishes, so the callback
// must own its key bytes. Called with txn locked, right after the D228
// pairing, so the collected set is exactly the committed link set.
//
// Only rows whose commit_ts is still NULL are collected. That qualifier is
// what keeps the set "this transaction's staged link rows" (D225): the
// stamping UPDATE itself stages a delete+insert of the addressed link rows
// with commit_ts filled, and collecting those re-inserted images would arm
// the completion callback on the STAMPING transaction's own commit — an
// unbounded self-retriggering chain of stamping transactions, each
// appending one more same-key row version for the next one's dedup scan.
// A genuinely staged link always has commit_ts NULL here: the moving
// statement stages it NULL (D225(b)), and no pre-commit pass fills it.
func (txn *Transaction) collectPkMoveLinkStampSetLocked() map[uint32][][]byte {
	var out map[uint32][][]byte
	for i := range txn.writes {
		we := &txn.writes[i]
		if we.typ != INSERT || we.bat == nil || we.bat.RowCount() == 0 ||
			!isPkMoveLinkTableInsert(we.databaseName, we.tableName) {
			continue
		}
		idIdx := pkMoveLinkColIndexLocked(we, catalog.DataBranchUpdateLinkColLinkID)
		if idIdx < 0 || idIdx >= len(we.bat.Vecs) {
			continue
		}
		// A staged batch without the commit_ts column predates the D225
		// layout; treat every row as unstamped, preserving the pre-filter
		// behavior for that drift case.
		tsIdx := pkMoveLinkColIndexLocked(we, catalog.DataBranchUpdateLinkColCommitTS)
		var tsVec *vector.Vector
		if tsIdx >= 0 && tsIdx < len(we.bat.Vecs) {
			tsVec = we.bat.Vecs[tsIdx]
		}
		idVec := we.bat.Vecs[idIdx]
		for r := 0; r < we.bat.RowCount(); r++ {
			if idVec.IsNull(uint64(r)) {
				continue
			}
			if tsVec != nil && !tsVec.IsNull(uint64(r)) {
				continue
			}
			raw := idVec.GetBytesAt(r)
			if len(raw) != catalog.DataBranchUpdateLinkIDBytes {
				continue
			}
			id := make([]byte, len(raw))
			copy(id, raw)
			if out == nil {
				out = make(map[uint32][][]byte)
			}
			out[we.accountId] = append(out[we.accountId], id)
		}
	}
	return out
}

// registerPkMoveLinkCommitStampLocked arms the commit-completion stamping
// callback when this transaction staged any link rows. Registration runs
// inside Transaction.Commit — after the D228 pairing, before the workspace
// merge/dump serializes the staged set — so the operator is still open and
// the callback observes the final TN-assigned commit instant.
func (txn *Transaction) registerPkMoveLinkCommitStampLocked() {
	if txn.op == nil {
		return
	}
	stampSet := txn.collectPkMoveLinkStampSetLocked()
	if len(stampSet) == 0 {
		return
	}
	ieFactory := txn.engine.config.ieFactory
	txn.op.AppendEventCallback(client.CommitEvent, client.TxnEventCallback{
		Value: stampSet,
		Func: func(_ context.Context, _ client.TxnOperator, event client.TxnEvent, v any) error {
			ids, _ := v.(map[uint32][][]byte)
			if !event.CostEvent || event.Err != nil || !event.Committed() || len(ids) == 0 {
				return nil
			}
			commitTS := event.Txn.CommitTS
			if commitTS.PhysicalTime == 0 && commitTS.LogicalTime == 0 {
				return nil
			}
			stampPkMoveLinkCommitTS(ieFactory, commitTS.PhysicalTime, commitTS.LogicalTime, ids)
			return nil
		},
	})
}

// stampPkMoveLinkCommitTS writes the TN-assigned commit instant onto the
// exact staged link_id set, one chunked UPDATE per account shard. Every
// failure is logged and swallowed: stamping is best-effort (D225), and the
// D227 recovery outcome table re-derives any instant left pending.
func stampPkMoveLinkCommitTS(
	ieFactory func() ie.InternalExecutor,
	physical int64,
	logical uint32,
	stampSet map[uint32][][]byte,
) {
	// types.BuildTS lays out the instant exactly as the apply side decodes
	// it (logical in the low 4 bytes, physical in the high 8 bytes,
	// little-endian; commit_ts is a binary(12) verbatim types.TS image).
	ts := types.BuildTS(physical, logical)
	tsHex := hex.EncodeToString(ts[:])

	for account, ids := range stampSet {
		if len(ids) == 0 {
			continue
		}
		if objectio.CNDataBranchPkMoveLinkStampFailedInjected(account) {
			// Test-only kill switch: leave every staged row stamp-pending
			// so BVT fixtures drive the D227 recovery channel.
			logutil.Warn(
				"pk-move link commit-ts stamping skipped by fault point",
				zap.Uint32("account", account),
				zap.Int("link-count", len(ids)),
			)
			continue
		}
		if ieFactory == nil {
			// A CN without the SQL executor channel (unit tests, early
			// boot) cannot stamp; the rows stay stamp-pending and D227
			// recovery covers them.
			logutil.Warn(
				"pk-move link commit-ts stamping unavailable: no internal executor",
				zap.Uint32("account", account),
				zap.Int("link-count", len(ids)),
			)
			continue
		}
		// The frontend's session-txn creation reads the account from the
		// request context (TxnHandler.createUnsafe -> defines.GetAccountId),
		// so the stamping channel must attach its target account exactly as
		// every other internal-executor caller does; a bare Background makes
		// the Exec fail before the statement runs.
		ctx, cancel := context.WithTimeout(
			defines.AttachAccountId(context.Background(), account),
			pkMoveLinkStampTimeout)
		for start := 0; start < len(ids); start += pkMoveLinkStampChunk {
			end := start + pkMoveLinkStampChunk
			if end > len(ids) {
				end = len(ids)
			}
			var sb strings.Builder
			sb.Grow((end-start)*140 + 160)
			sb.WriteString("update ")
			sb.WriteString(catalog.MO_CATALOG)
			sb.WriteString(".")
			sb.WriteString(catalog.MO_DATA_BRANCH_UPDATE_LINK)
			sb.WriteString(" set ")
			sb.WriteString(catalog.DataBranchUpdateLinkColCommitTS)
			sb.WriteString(" = x'")
			sb.WriteString(tsHex)
			sb.WriteString("' where ")
			sb.WriteString(catalog.DataBranchUpdateLinkColLinkID)
			sb.WriteString(" in (")
			for i := start; i < end; i++ {
				if i > start {
					sb.WriteString(", ")
				}
				sb.WriteString("x'")
				sb.WriteString(hex.EncodeToString(ids[i]))
				sb.WriteString("'")
			}
			sb.WriteString(")")
			err := ieFactory().Exec(
				ctx,
				sb.String(),
				ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).AccountId(account).Finish(),
			)
			if err != nil {
				// Best-effort: the remaining chunks still run, and every
				// unstamped row keeps the durable stamp-pending intent for
				// the D227 recovery outcome table.
				logutil.Warn(
					"pk-move link commit-ts stamping failed",
					zap.Uint32("account", account),
					zap.Int("chunk-start", start),
					zap.Int("chunk-size", end-start),
					zap.Error(err),
				)
			}
		}
		cancel()
	}
}

// encodePkMoveLinkStampTS lays out the commit instant exactly as
// types.BuildTS does; kept for unit tests asserting the stored byte image
// without constructing a types.TS.
func encodePkMoveLinkStampTS(physical int64, logical uint32) (ts [catalog.DataBranchUpdateLinkTSBytes]byte) {
	built := types.BuildTS(physical, logical)
	copy(ts[:], built[:])
	return
}
