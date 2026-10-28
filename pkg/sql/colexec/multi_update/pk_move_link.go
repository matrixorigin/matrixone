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

package multi_update

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	lock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// isPkMoveLinkTableMissingErr reports whether err is exactly the
// supported pre-upgrade condition: the tenant predates v4.0.15, so the
// catalog holds no mo_data_branch_update_link table. Any OTHER
// resolution failure is FATAL to the statement, never a degrade: the
// resolver (getRelationByObjRef) also propagates arbitrary
// Engine.Database / Database.Relation failures, and treating a
// transient catalog error as "table missing" would commit the user's
// rows WITHOUT their provenance links. A later MERGE/PICK apply reads
// missing provenance as proven absence and replays plain DELETE+INSERT,
// so the compatibility path must stay this narrow.
func isPkMoveLinkTableMissingErr(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrNoSuchTable)
}

// resolvePkMoveLinkSource returns the lazily resolved per-account link
// relation for linkCtx, or nil when the write must degrade: a tenant
// predating the catalog table sets the sticky disable flag once and every
// route (workspace, S3 ferry) then keeps the legacy delete+insert
// behaviour. Both writer entry points share this so their resolution and
// disable bookkeeping can never drift. Resolution failures other than
// the pre-upgrade missing-table condition are RETURNED: the statement
// aborts and its user rows and provenance commit or abort together.
func (update *MultiUpdate) resolvePkMoveLinkSource(
	proc *process.Process,
	linkCtx *MultiUpdateCtx,
) (engine.Relation, error) {
	info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
	if info == nil || info.pkMoveLinkDisabled {
		return nil, nil
	}
	if info.Source == nil {
		if update.Engine == nil {
			// D165: contexts without a CN engine (mock procs,
			// harnesses) degrade exactly like a missing catalog
			// table; staged mock sources above still flow.
			info.pkMoveLinkDisabled = true
			logutil.Warn("skip pk-move update links: no engine in context")
			return nil, nil
		}
		rel, rerr := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, update.Engine, linkCtx.ObjRef)
		if rerr != nil {
			if !isPkMoveLinkTableMissingErr(rerr) {
				return nil, rerr
			}
			info.pkMoveLinkDisabled = true
			logutil.Warn("skip pk-move update links: link table unavailable", zap.Error(rerr))
			return nil, nil
		}
		info.Source = rel
	}
	return info.Source, nil
}

// loadSpilledPkMoveLinkBatch reads a spilled link object back into the
// exact in-memory batch shape the workspace route stages: blockInfoBat
// is the ferried block-info (block locations and object stats the
// producer's spill wrote), and the returned batch carries one proc-owned
// vector per link column in the schema's column order. Empty and
// column-mismatched objects fail loudly: dropped or misparsed provenance
// must degrade through the disable path, not through silent row loss.
func (update *MultiUpdate) loadSpilledPkMoveLinkBatch(
	proc *process.Process,
	blockInfoBat *batch.Batch,
	linkCtx *MultiUpdateCtx,
) (bat *batch.Batch, err error) {
	fs, err := colexec.GetSharedFSFromProc(proc)
	if err != nil {
		return nil, err
	}
	mp := proc.Mp()
	out := newPkMoveLinkBatch(proc, linkCtx)
	defer func() {
		if err != nil {
			out.Clean(mp)
		}
	}()
	if blockInfoBat.RowCount() == 0 {
		return out, nil
	}
	blkVec := blockInfoBat.Vecs[0]
	for i := 0; i < blockInfoBat.RowCount(); i++ {
		blk := objectio.DecodeBlockInfo(blkVec.GetBytesAt(i))
		// DecodeBlockInfo aliases the ferried batch's vector memory, and
		// that batch is released once the consumer finishes staging.
		// Fileservice caches keep (hash) keys derived from the object-name
		// bytes for each entry's whole lifetime, so the load must go
		// through an owned location copy: no cache key may outlive this
		// batch's freed (possibly off-heap) memory.
		loc := append(objectio.Location(nil), blk.MetaLocation()...)
		blockBat, _, _, lerr := ioutil.LoadOneBlockWithSpecialLayout(
			proc.Ctx, fs, loc, objectio.SchemaData)
		if lerr != nil {
			return nil, lerr
		}
		if blockBat.VectorCount() != out.VectorCount() {
			err = moerr.NewInternalErrorNoCtxf(
				"pk-move link spill object has %d columns, the link schema expects %d",
				blockBat.VectorCount(), out.VectorCount())
			blockBat.Clean(mp)
			return nil, err
		}
		for col := range out.Vecs {
			if uerr := out.Vecs[col].UnionBatch(blockBat.Vecs[col], 0,
				blockBat.Vecs[col].Length(), nil, mp); uerr != nil {
				blockBat.Clean(mp)
				return nil, uerr
			}
		}
		blockBat.Clean(mp)
	}
	out.SetRowCount(out.Vecs[0].Length())
	return out, nil
}

// consumePkMoveLinkSpillOwnership reclaims a successfully consumed spill
// ferry: the link rows are staged in the coordinator's committing
// workspace, so the producer's spill objects are dead input. The
// producer PUBLISHED these records when it emitted the tail row and its
// teardown never touches them again, so terminal ownership lands here,
// on the only component that knows the data was actually used — object
// first, marker released only after the object is confirmed absent,
// exactly like the producer's own terminal pass. Best-effort on a fresh
// bounded context: the staged provenance is already durable, so a
// cleanup error must not fail the statement; survivors stay owned by
// their markers' expired GC replay.
func (update *MultiUpdate) consumePkMoveLinkSpillOwnership(
	proc *process.Process, infoBat *batch.Batch,
) {
	if infoBat == nil || infoBat.RowCount() == 0 || len(infoBat.Vecs) == 0 {
		return
	}
	fs, err := colexec.GetSharedFSFromProc(proc)
	if err != nil {
		logutil.Warn("pk-move link spill consume-cleanup lost its filesystem; GC replay owns the spill",
			zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), pkMoveLinkSpillCleanupTimeout)
	defer cancel()
	blkVec := infoBat.Vecs[0]
	// Locate the marker column by name: the block-info batch's own
	// column count varies with the sinker layout, so a positional guess
	// would silently read (or miss) the wrong vector.
	var markerVec *vector.Vector
	for i, attr := range infoBat.Attrs {
		if attr != pkMoveLinkSpillMarkerCol || i >= len(infoBat.Vecs) {
			continue
		}
		if v := infoBat.Vecs[i]; v.GetType().Oid == types.T_varchar &&
			v.Length() >= infoBat.RowCount() {
			markerVec = v
		}
		break
	}
	type reclaim struct{ marker string }
	files := make(map[string]reclaim)
	for i := 0; i < infoBat.RowCount(); i++ {
		blk := objectio.DecodeBlockInfo(blkVec.GetBytesAt(i))
		file := blk.MetaLocation().Name().String()
		rec := reclaim{}
		if markerVec != nil && !markerVec.IsNull(uint64(i)) {
			rec.marker = string(markerVec.GetBytesAt(i))
		}
		files[file] = rec
	}
	for file, rec := range files {
		if _, err := pkMoveLinkDeleteSpillObjects(ctx, fs, file); err != nil {
			logutil.Warn("pk-move link spill consume cleanup failed; GC replay owns the object",
				zap.String("file", file), zap.Error(err))
			continue
		}
		if rec.marker == "" {
			continue
		}
		if err := pkMoveLinkReleaseSpillMarker(ctx, fs, rec.marker); err != nil {
			logutil.Warn("pk-move link spill consume marker release failed; GC replay owns it",
				zap.String("marker", rec.marker), zap.Error(err))
		}
	}
}

// pkMoveLinkInsertPos returns the position of the owner's pk vector among its
// InsertCols: table insert order follows TableDef.Cols minus the hidden rowid.
func pkMoveLinkInsertPos(ownerCtx *MultiUpdateCtx) (int, bool) {
	if ownerCtx == nil || ownerCtx.TableDef == nil || ownerCtx.TableDef.Pkey == nil {
		return 0, false
	}
	pkName := ownerCtx.TableDef.Pkey.PkeyColName
	pos := 0
	for _, col := range ownerCtx.TableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		if col.Name == pkName {
			return pos, true
		}
		pos++
	}
	return 0, false
}

// insertUpdatePkMoveLinkRows compares the old and new pk of every updated row
// in contextBatch (the owning main table's rows) and inserts one link row per
// row whose pk changed. Rows whose pk is unchanged still write nothing when
// their pre-image is durable; only a workspace-minted pre-image (a row an
// earlier statement of this transaction staged) emits a same-key continuity
// marker, which the workspace fill hook consumes and drops before commit —
// so statements that merely touch the pk column on committed rows (set id =
// id) still cost one encoding compare per row and no link writes.
// REPLACE/ODKU never reach here because their plans never carry the
// synthetic link ctx.
//
// The link's new_row_id column is left NULL here: the workspace-stage row
// id of the replacement row only exists once the txn layer stages the
// owning insert, and the staged batch is a txn-private copy this operator
// never sees. The disttae workspace resolves that placeholder identity
// when the link write is staged
// (Transaction.writeBatchWithAutoIncrEpochKnown back-fills new_row_id from
// the same transaction's staged inserts). The owning main-table ctx always
// executes before its synthetic link ctx inside one statement, so the
// sibling insert is guaranteed visible to the fixup. The placeholder
// chains same-transaction generations netted out of the committed stream;
// the fold itself runs on the deleting tombstone's commit-timestamp tie,
// so rows that stay NULL still fold single-shot and only netted
// continuations become unprovable (fail closed).
func (update *MultiUpdate) insertUpdatePkMoveLinkRows(
	proc *process.Process,
	analyzer process.Analyzer,
	linkCtx *MultiUpdateCtx,
	ownerCtx *MultiUpdateCtx,
	contextBatch *batch.Batch,
) (err error) {
	source, err := update.resolvePkMoveLinkSource(proc, linkCtx)
	if err != nil || source == nil {
		return err
	}

	linkBat, err := update.collectPkMoveLinkBatch(proc, linkCtx, ownerCtx, contextBatch)
	if err != nil || linkBat == nil {
		return err
	}
	defer linkBat.Clean(proc.Mp())

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	newCtx = update.writeContext(newCtx, targetTableID(ownerCtx))
	// Retention genesis rides the SAME transaction as the link write (D159):
	// one transaction carries both, so rollback leaves neither row and a
	// commit publishes both atomically. Stage the identity row FIRST — a
	// link write failure below fails the statement and the positional cut
	// removes both. D218: genesis admission requires this batch to carry a
	// real moved-address row; continuity markers alone stage nothing.
	if pkMoveLinkBatchHasRealMove(linkBat) {
		if err = update.stagePkMoveRetentionRows(proc, analyzer, linkCtx, ownerCtx); err != nil {
			return err
		}
	}
	if err = process.MeasureFilesystemWaitErr(analyzer, func() error {
		return source.Write(newCtx, linkBat)
	}); err != nil {
		return err
	}
	analyzer.AddWrittenRows(int64(linkBat.RowCount()))
	return nil
}

// collectPkMoveLinkBatch synthesizes the pk-move link batch for the rows
// of contextBatch whose pk moved WITHOUT staging it anywhere. The
// UpdateWriteS3 route must use this instead of writing the batch into a
// producer-side relation: a remote producer writes into an independent
// mirror workspace whose mutations never reach the committing
// transaction (remoterunServer), so remotely produced provenance used to
// vanish before commit. The S3 writer buffers the result and ferries it
// through the tail output (actionPkMoveLink) to UpdateFlushS3Info, which
// stages it in the coordinator's committing workspace. The collection
// side still needs no catalog access: the link context carries the
// synthesized schema, and the disable state resolved at Prepare time
// carries over.
//
// CreateTS (not SnapshotTS): the statement executors refresh the
// snapshot per statement under RC, so snapshot timestamps of two
// UPDATEs in one transaction differ; the create timestamp is constant
// for the transaction, which window upper-bound filtering relies on.
// Because a transaction's commits can postdate a later window's
// start, write_ts only supports an UPPER-bound filter on the
// consumer side: window probes through the endpoint stay sound, but
// a lower predicate would hide straddling transactions whose commits
// landed inside the window — those moves separate transaction
// identity from commit-window membership, and the walk decides
// membership by stream commit instants instead. Transaction IDENTITY
// itself does not ride the create timestamp: two transactions
// created inside one physical instant share it (the HLC advances the
// logical component), so the recording transaction's operator id is
// persisted as its own column and namespaces workspace-minted link
// ids and the consumer's same-transaction matching.
func (update *MultiUpdate) collectPkMoveLinkBatch(
	proc *process.Process,
	linkCtx *MultiUpdateCtx,
	ownerCtx *MultiUpdateCtx,
	contextBatch *batch.Batch,
) (*batch.Batch, error) {
	info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
	if info == nil || info.pkMoveLinkDisabled {
		return nil, nil
	}
	return collectUpdatePkMoveLinkRows(
		proc, linkCtx, ownerCtx, contextBatch,
		uint64(types.TimestampToTS(proc.GetTxnOperator().CreateTS()).Physical()),
		proc.GetTxnOperator().Txn().ID,
	)
}

// collectUpdatePkMoveLinkRows builds the link batch for the rows of
// contextBatch whose pk actually changed, plus one continuity marker per
// same-key replacement of a workspace-minted row, or nil when neither
// exists. Old and new
// pk values are Packer-encoded with the same encoding data-branch hash maps
// use for keys, so stored bytes can drive encoded-key lookups verbatim.
// writeTS is the updating transaction's create timestamp in physical
// nanoseconds: constant across the transaction's statements (the snapshot
// timestamp is not — it refreshes per statement) and still a lower bound
// of the commit timestamp for window probes. txnID is the recording
// transaction's operator identity: write_ts alone is not a transaction
// namespace (two transactions created inside one physical instant share
// it), so txn_id namespaces workspace-minted link ids and the consumer's
// same-transaction matching, with write_ts kept for window filtering. The
// new_row_id column is appended NULL for every row: the disttae workspace
// back-fills the replacement row's identity when the link write is staged
// (see insertUpdatePkMoveLinkRows).
func collectUpdatePkMoveLinkRows(
	proc *process.Process,
	linkCtx *MultiUpdateCtx,
	ownerCtx *MultiUpdateCtx,
	contextBatch *batch.Batch,
	writeTS uint64,
	txnID []byte,
) (bat *batch.Batch, err error) {
	pkInsertPos, ok := pkMoveLinkInsertPos(ownerCtx)
	if !ok || pkInsertPos >= len(ownerCtx.InsertCols) || len(ownerCtx.DeleteCols) < 2 {
		return nil, nil
	}
	if contextBatch == nil || contextBatch.RowCount() == 0 {
		return nil, nil
	}
	oldRowIDPos := ownerCtx.DeleteCols[0]
	oldPKPos := ownerCtx.DeleteCols[1]
	newPKPos := ownerCtx.InsertCols[pkInsertPos]
	if oldRowIDPos < 0 || oldRowIDPos >= len(contextBatch.Vecs) ||
		oldPKPos < 0 || oldPKPos >= len(contextBatch.Vecs) ||
		newPKPos < 0 || newPKPos >= len(contextBatch.Vecs) {
		return nil, nil
	}
	oldRowIDVec := contextBatch.Vecs[oldRowIDPos]
	if oldRowIDVec.GetType().Oid != types.T_Rowid {
		return nil, nil
	}
	oldPKVec := contextBatch.Vecs[oldPKPos]
	newPKVec := contextBatch.Vecs[newPKPos]
	normTxn := pkMoveLinkNormTxnID(txnID)

	packer := types.NewPacker()
	defer packer.Close()

	rowCount := contextBatch.RowCount()
	for i := 0; i < rowCount; i++ {
		if oldRowIDVec.IsNull(uint64(i)) {
			continue
		}
		packer.Reset()
		if err = vector.EncodeRowToPacker(packer, oldPKVec, i); err != nil {
			return nil, err
		}
		oldBytes := append([]byte(nil), packer.Bytes()...)
		packer.Reset()
		if err = vector.EncodeRowToPacker(packer, newPKVec, i); err != nil {
			return nil, err
		}
		newBytes := append([]byte(nil), packer.Bytes()...)
		rowID := vector.GetFixedAtNoTypeCheck[types.Rowid](oldRowIDVec, i)
		// Same-key replacement (a non-key UPDATE, or SET id = id): no
		// move happened, but an earlier statement's move may have
		// recorded its replacement identity against exactly this row id
		// — and this statement is about to replace it again. The
		// unconditional emission below then doubles as a continuity
		// marker (old pk bytes == new pk bytes) so the workspace fill
		// hook can carry the recorded identities onto the fresh row; the
		// hook drops marker rows, they never commit. Durable pre-images
		// need the marker too: when a later statement moves the fresh
		// row, the link's old side must name the durable generation the
		// committed stream shows departing, and only this marker records
		// that lineage.

		if bat == nil {
			bat = newPkMoveLinkBatch(proc, linkCtx)
		}
		oldSha := sha256.Sum256(oldBytes)
		newSha := sha256.Sum256(newBytes)
		linkID := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
		binary.BigEndian.PutUint64(linkID, ownerCtx.TableDef.TblId)
		copy(linkID[8:], oldSha[:])
		copy(linkID[8+catalog.DataBranchUpdateLinkPKShaBytes:], pkMoveLinkIDRowidPart(&rowID, writeTS, txnID))
		if err = vector.AppendBytes(bat.Vecs[0], linkID, false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendFixed[uint64](bat.Vecs[1], ownerCtx.TableDef.TblId, false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendBytes(bat.Vecs[2], oldSha[:], false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendBytes(bat.Vecs[3], rowID[:], false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendBytes(bat.Vecs[4], oldBytes, false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendBytes(bat.Vecs[5], newSha[:], false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendBytes(bat.Vecs[6], newBytes, false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		// new_row_id: resolved by the disttae workspace when this write is
		// staged; NULL marks "identity not recorded yet".
		if err = vector.AppendBytes(bat.Vecs[7], nil, true, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendFixed[uint64](bat.Vecs[8], writeTS, false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
		if err = vector.AppendBytes(bat.Vecs[9], normTxn[:], false, proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return nil, err
		}
	}
	if bat != nil {
		bat.SetRowCount(bat.Vecs[0].Length())
	}
	return bat, nil
}

// pkMoveLinkBatchHasRealMove is the D218 actual-move admission predicate:
// the link batch carries at least one REAL moved-address row — old pk
// bytes != new pk bytes under the same equality the disttae fill hook's
// marker consumption uses. A continuity-only batch (every row a same-key
// marker, or empty) admits NO retention genesis or sealing: gating the
// two staging call sites on this predicate keeps an ordinary UPDATE off
// the keyed ledger probe, its exclusive locks, and the HIT-lane seal
// while continuity markers keep flowing to the workspace fill hook.
// Columns are located by attribute name, mirroring the consumer's
// drift discipline; an unlocatable pair keeps the conservative admission
// so provenance never commits without its retention identity.
func pkMoveLinkBatchHasRealMove(bat *batch.Batch) bool {
	if bat == nil {
		return false
	}
	oldPKIdx, newPKIdx := -1, -1
	for i, attr := range bat.Attrs {
		switch attr {
		case catalog.DataBranchUpdateLinkColOldPK:
			oldPKIdx = i
		case catalog.DataBranchUpdateLinkColNewPK:
			newPKIdx = i
		}
	}
	if oldPKIdx < 0 || newPKIdx < 0 {
		return true
	}
	oldVec, newVec := bat.Vecs[oldPKIdx], bat.Vecs[newPKIdx]
	for i := 0; i < bat.RowCount(); i++ {
		if oldVec.IsNull(uint64(i)) || newVec.IsNull(uint64(i)) ||
			!bytes.Equal(oldVec.GetBytesAt(i), newVec.GetBytesAt(i)) {
			return true
		}
	}
	return false
}

// pkMoveLinkIDRowidPart derives the row-id segment of a link id from the
// replaced row's old identity. Workspace-stage row ids restart at the same
// values in every transaction (the txn layer resets allocation to the
// fixed workspace segment), so two transactions whose moves cross the same
// table key would mint byte-identical link ids and collide on the catalog
// primary key — blocking the second transaction outright (duplicate-key
// check) or storing duplicate-identity rows. The salt namespaces by the
// recording TRANSACTION (id first, then the create timestamp): the create
// timestamp's physical component is shared by transactions created inside
// one physical instant (hlc.go advances the logical component instead).
// Durable identities pass through unaltered: they are unique already, and
// keeping them verbatim preserves the pre-fix id derivation for links
// anchored on committed rows.
func pkMoveLinkIDRowidPart(rid *types.Rowid, writeTS uint64, txnID []byte) []byte {
	part := make([]byte, types.RowidSize)
	if !rid.BorrowSegmentID().EQ(&colexec.TxnWorkspaceSegment) {
		copy(part, rid[:])
		return part
	}
	ns := make([]byte, catalog.DataBranchUpdateLinkTxnIDBytes+8+types.RowidSize)
	norm := pkMoveLinkNormTxnID(txnID)
	copy(ns, norm[:])
	binary.LittleEndian.PutUint64(ns[catalog.DataBranchUpdateLinkTxnIDBytes:], writeTS)
	copy(ns[catalog.DataBranchUpdateLinkTxnIDBytes+8:], rid[:])
	sum := sha256.Sum256(ns)
	copy(part, sum[:types.RowidSize])
	return part
}

// pkMoveLinkNormTxnID normalizes the recording transaction's operator id
// to the fixed 16-byte catalog width; malformed or absent ids degrade to
// the all-zero "identity unknown" value, which the consumer's
// same-transaction matching treats as unprovable and falls back to the
// create-timestamp namespace.
func pkMoveLinkNormTxnID(txnID []byte) (norm [catalog.DataBranchUpdateLinkTxnIDBytes]byte) {
	if len(txnID) == catalog.DataBranchUpdateLinkTxnIDBytes {
		copy(norm[:], txnID)
	}
	return norm
}

func newPkMoveLinkBatch(proc *process.Process, linkCtx *MultiUpdateCtx) *batch.Batch {
	bat := batch.NewWithSize(len(linkCtx.TableDef.Cols) - 1)
	attrs := make([]string, 0, len(linkCtx.TableDef.Cols)-1)
	vecIdx := 0
	for _, col := range linkCtx.TableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		bat.Vecs[vecIdx] = vector.NewVec(plan2.MakeTypeByPlan2Type(col.Typ))
		attrs = append(attrs, col.GetOriginCaseName())
		vecIdx++
	}
	bat.SetAttributes(attrs)
	return bat
}

// ---------------------------------------------------------------------------
// Retention-ledger genesis (D159-D166): the producer stages at most one
// identity row per distinct moved address per transaction into
// mo_data_branch_link_retention, riding the same transaction as the link
// write. The workspace admission (disttae) owns dedup, the EXTERNAL probe
// memo, the prune cancellation, and the forced-dump keep membership; this
// side owns resolution, batch composition, and the live keyed probe the
// engine cannot run (the internal-SQL executor must not become an engine
// dependency).

// resolvePkMoveRetentionSource returns the lazily resolved per-account
// retention relation, or nil when genesis must degrade: the pre-upgrade
// missing-table condition sticks once, mirroring the link resolver. Every
// other failure is FATAL to the statement — degrading past it would commit
// link rows without their retention identity.
func (update *MultiUpdate) resolvePkMoveRetentionSource(
	proc *process.Process,
	linkCtx *MultiUpdateCtx,
) (engine.Relation, error) {
	info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
	if info == nil || info.pkMoveRetentionDisabled {
		return nil, nil
	}
	if info.retentionSource == nil {
		if update.Engine == nil {
			// D165: contexts without a CN engine (mock procs,
			// harnesses) degrade exactly like a missing catalog
			// table; staged mock sources above still flow.
			info.pkMoveRetentionDisabled = true
			logutil.Warn("skip pk-move retention genesis: no engine in context")
			return nil, nil
		}
		rel, rerr := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc,
			update.Engine, &plan2.ObjectRef{
				SchemaName: catalog.MO_CATALOG,
				ObjName:    catalog.MO_DATA_BRANCH_LINK_RETENTION,
			})
		if rerr != nil {
			if !isPkMoveLinkTableMissingErr(rerr) {
				return nil, rerr
			}
			info.pkMoveRetentionDisabled = true
			logutil.Warn("skip pk-move retention genesis: retention table unavailable",
				zap.Error(rerr))
			return nil, nil
		}
		info.retentionSource = rel
	}
	return info.retentionSource, nil
}

// stagePkMoveRetentionRows stages the owner address's identity row in the
// retention ledger through the marked genesis write path. The workspace
// admission dedups repeat touches (zero inserts), so this emits on every
// move statement of the transaction; the keyed probe the engine runs before
// staging fires exactly once per address per registration epoch.
func (update *MultiUpdate) stagePkMoveRetentionRows(
	proc *process.Process,
	analyzer process.Analyzer,
	linkCtx *MultiUpdateCtx,
	ownerCtx *MultiUpdateCtx,
) (err error) {
	info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
	if info == nil || info.pkMoveLinkDisabled || info.pkMoveRetentionDisabled {
		return nil
	}
	source, err := update.resolvePkMoveRetentionSource(proc, linkCtx)
	if err != nil || source == nil {
		return err
	}
	ownerTableID := targetTableID(ownerCtx)
	bat, err := newPkMoveRetentionBatch(proc, source, ownerCtx, ownerTableID,
		uint64(types.TimestampToTS(proc.GetTxnOperator().CreateTS()).Physical()))
	if err != nil {
		return err
	}
	defer bat.Clean(proc.Mp())

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
	newCtx = update.writeContext(newCtx, ownerTableID)
	newCtx = disttae.WithPkMoveGenesisWrite(newCtx)
	if err = process.MeasureFilesystemWaitErr(analyzer, func() error {
		return pkMoveGenesisWriteFn(source, newCtx, bat)
	}); err != nil {
		return err
	}
	analyzer.AddWrittenRows(int64(bat.RowCount()))
	return nil
}

// pkMoveGenesisWriteFn indirection lets failure-injection cells abort the
// genesis write deterministically; the production path is the plain relation
// write.
var pkMoveGenesisWriteFn = func(
	source engine.Relation, ctx context.Context, bat *batch.Batch,
) error {
	return source.Write(ctx, bat)
}

// newPkMoveRetentionBatch composes the single identity row for one moved
// address in the retention relation's insert layout (every non-rowid column
// in TableDef order, including the materialized composite-pk __mo_cpkey
// when the definition carries it). createdTS is the recording transaction's
// create timestamp in physical nanoseconds — the first-registration instant
// the ledger's refresh rule keeps immutable. A descriptor logical id of 0
// marks "unresolved": the engine-side probe back-fills the durable
// rel_logical_id before the row lands in the workspace.
func newPkMoveRetentionBatch(
	proc *process.Process,
	source engine.Relation,
	ownerCtx *MultiUpdateCtx,
	ownerTableID uint64,
	createdTS uint64,
) (*batch.Batch, error) {
	tableDef := source.GetTableDef(proc.Ctx)
	if tableDef == nil || ownerCtx == nil || ownerCtx.ObjRef == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"pk-move retention genesis: retention or owner table definition absent")
	}
	logicalID := uint64(0)
	if ownerCtx.TableDef != nil {
		logicalID = ownerCtx.TableDef.GetLogicalId()
	}
	mp := proc.Mp()
	rowKindVec := vector.NewVec(types.T_int8.ToType())
	defer rowKindVec.Free(mp)
	if err := vector.AppendFixed[int8](
		rowKindVec, int8(catalog.DataBranchLinkRetentionRowKindReal), false, mp); err != nil {
		return nil, err
	}
	tableIDVec := vector.NewVec(types.T_uint64.ToType())
	defer tableIDVec.Free(mp)
	if err := vector.AppendFixed[uint64](tableIDVec, ownerTableID, false, mp); err != nil {
		return nil, err
	}
	bat := batch.NewWithSize(len(tableDef.Cols) - 1)
	attrs := make([]string, 0, len(tableDef.Cols)-1)
	vecIdx := 0
	for _, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		bat.Vecs[vecIdx] = vector.NewVec(plan2.MakeTypeByPlan2Type(col.Typ))
		attrs = append(attrs, col.GetOriginCaseName())
		vecIdx++
	}
	bat.SetAttributes(attrs)
	packer := types.NewPacker()
	defer packer.Close()
	for j, attr := range bat.Attrs {
		switch attr {
		case catalog.CPrimaryKeyColName:
			encKind, err := planfunction.NewSerialValueEncoder(rowKindVec)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}
			encTable, err := planfunction.NewSerialValueEncoder(tableIDVec)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}
			packer.Reset()
			encKind(rowKindVec, 0, packer)
			encTable(tableIDVec, 0, packer)
			if err := vector.AppendBytes(
				bat.Vecs[j], append([]byte(nil), packer.Bytes()...), false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		case catalog.DataBranchLinkRetentionColRowKind:
			if err := vector.AppendFixed[int8](
				bat.Vecs[j], int8(catalog.DataBranchLinkRetentionRowKindReal), false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		case catalog.DataBranchLinkRetentionColTableID, catalog.DataBranchLinkRetentionColObjID:
			if err := vector.AppendFixed[uint64](bat.Vecs[j], ownerTableID, false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		case catalog.DataBranchLinkRetentionColDatabaseName:
			if err := vector.AppendBytes(
				bat.Vecs[j], []byte(ownerCtx.ObjRef.SchemaName), false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		case catalog.DataBranchLinkRetentionColTableName:
			if err := vector.AppendBytes(
				bat.Vecs[j], []byte(ownerCtx.ObjRef.ObjName), false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		case catalog.DataBranchLinkRetentionColCreatedTS:
			if err := vector.AppendFixed[uint64](bat.Vecs[j], createdTS, false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		case catalog.DataBranchLinkRetentionColLogicalID:
			if err := vector.AppendFixed[uint64](bat.Vecs[j], logicalID, false, mp); err != nil {
				bat.Clean(mp)
				return nil, err
			}
		default:
			bat.Clean(mp)
			return nil, moerr.NewInternalErrorNoCtxf(
				"pk-move retention genesis: unexpected retention column %q", attr)
		}
	}
	bat.SetRowCount(1)
	return bat, nil
}

// The live genesis probe: registration existence, committed link presence
// (own transaction excluded), and the conditional rel_logical_id resolution
// — exactly the charged probe set of one epoch's first touch, re-derived
// per epoch, never cached. Registered at init; overridable for the
// producer-side fabric cells.
var pkMoveGenesisRunSql = sqlexec.RunSql

func init() {
	disttae.PkMoveGenesisProbe = pkMoveGenesisProbeLive
}

func pkMoveGenesisProbeLive(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	accountID uint32,
	tableID uint64,
	ownTxnIDHex string,
	resolveLogical bool,
) (registered, committedLinks bool, logicalID uint64, err error) {
	sqlproc := sqlexec.NewSqlProcess(proc)
	regSQL := fmt.Sprintf(
		"select table_id from %s.%s where %s = %d and %s = %d limit 1",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind,
		catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID)
	if registered, err = pkMoveGenesisProbeExists(sqlproc, regSQL); err != nil {
		if !databranchMissingColumnErr(err) {
			return false, false, 0, err
		}
		// Five-column dev-window shape (D160(B)): no row_kind column. The
		// capability verdict lives per contact — no persistent cache (D162).
		regSQL = fmt.Sprintf(
			"select table_id from %s.%s where %s = %d limit 1",
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColTableID, tableID)
		if registered, err = pkMoveGenesisProbeExists(sqlproc, regSQL); err != nil {
			return false, false, 0, err
		}
	}
	if registered {
		// D198(B.1)/D199(B.2): the HIT verdict is SEALED - this transaction
		// stages a BYTE-IDENTITY self-update of the consumed ledger row
		// through the ordinary account-scoped internal statement; the seal
		// statement's ordinary first-touch EXCLUSIVE row lock is the
		// producer's first and only ledger-key acquisition on every
		// isolation shape (the r81-r84 shared writer-fair anchor is
		// RETIRED), and TN MVCC adjudicates the consumed row's survival at
		// commit (D198(B.2)'s two-cause model). Lock/statement errors
		// propagate unswallowed through D171/D201's transition taxonomy.
		if err = sealPkMoveRetentionRowShared(sqlproc, proc, tableID); err != nil {
			return false, false, 0, err
		}
	} else {
		// D186(B.4)(a): a MISSED (OWNED genesis) verdict takes the
		// identity key's EXCLUSIVE row lock in this transaction NOW -
		// the staged insert's own row lock, established explicitly
		// because the marked genesis row bypasses the DML planner's
		// lock target. Held to transaction end: a LIVE writer blocks a
		// concurrent retirement's exclusive request on the same key
		// (D168(C) schedules), a COMMITTED writer has already folded
		// its commitTS into the ledger table's watermark and waiter
		// queue (D173), and the D165(F.2) duplicate-key model stays
		// the committed racing-registration failure shape verbatim.
		// Lock errors propagate unswallowed (D171).
		if err = pkMoveGenesisOwnedLockFn(ctx, proc, eng, accountID, tableID); err != nil {
			return false, false, 0, err
		}
	}
	// D161(A.1) charges the committed-link presence read as one KEYED
	// read mirroring pkMoveLinkPresenceProbe (pkg/sql/compile/
	// pk_move_link_retention.go): the link_id primary-key half-open
	// prefix range [tableID, tableID+1) bounds the committed-memory
	// scan to this address - the catalog's sole primary key is link_id,
	// so without the range the reader falls back to a full partition
	// scan on every first touch (round-50 pre-PR P2), violating the
	// D159(F) keyed producer charge. The table_id guard and the
	// own-transaction exclusion survive; tableID+1 == 0 overflows only
	// for a never-assigned MaxUint64 catalog id, where the shared
	// probe's absent verdict is mirrored.
	if tableID+1 == 0 {
		committedLinks = false
	} else {
		linkSQL := fmt.Sprintf(
			"select table_id from %s.%s where %s >= x'%016x' and %s < x'%016x' and %s = %d and %s <> unhex('%s') limit 1",
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
			catalog.DataBranchUpdateLinkColLinkID, tableID,
			catalog.DataBranchUpdateLinkColLinkID, tableID+1,
			catalog.DataBranchUpdateLinkColTableID, tableID,
			catalog.DataBranchUpdateLinkColTxnID, ownTxnIDHex)
		if committedLinks, err = pkMoveGenesisProbeExists(sqlproc, linkSQL); err != nil {
			return false, false, 0, err
		}
	}
	if resolveLogical {
		logSQL := fmt.Sprintf(
			"select coalesce(nullif(rel_logical_id,0),rel_id) from %s.%s where rel_id = %d",
			catalog.MO_CATALOG, catalog.MO_TABLES, tableID)
		var (
			res  executor.Result
			rerr error
		)
		res, rerr = pkMoveGenesisRunSql(sqlproc, logSQL)
		if rerr != nil {
			return false, false, 0, rerr
		}
		res.ReadRows(func(n int, cols []*vector.Vector) bool {
			if n > 0 && len(cols) == 1 && !cols[0].IsNull(0) {
				switch cols[0].GetType().Oid {
				case types.T_uint64:
					logicalID = vector.MustFixedColNoTypeCheck[uint64](cols[0])[0]
				case types.T_int64:
					logicalID = uint64(vector.MustFixedColNoTypeCheck[int64](cols[0])[0])
				}
			}
			return true
		})
		res.Close()
	}
	return registered, committedLinks, logicalID, nil
}

// pkMoveRetentionSealDisabled is the seal-staging defect flag (D206(C.1)'s
// seal-removed arm; the D155-C discipline): harness-only installers, never
// enabled on any production path, never referenced from production flows
// except the single check site below.
var pkMoveRetentionSealDisabled atomic.Bool

// DisablePkMoveRetentionSealForTest flips the seal-staging defect flag and
// returns a restore func; harness-only.
func DisablePkMoveRetentionSealForTest(disabled bool) (restore func()) {
	prev := pkMoveRetentionSealDisabled.Swap(disabled)
	return func() { pkMoveRetentionSealDisabled.Store(prev) }
}

// sealPkMoveRetentionRowShared stages the verdict seal (D198(B.1)): a
// BYTE-IDENTITY self-update of the consumed mo_data_branch_link_retention
// identity row in the producer's own transaction — every declared column
// assigned to itself through the ordinary account-scoped internal
// statement, one statement per consumed verdict (D198(C.7)). No lock
// request of its own is issued: the seal's ordinary DML lock discipline
// (platform WAIT policy under the deadlock detector and the lock-wait
// timeout, no FastFail override, no writer-fair bit — D199(B.3)) is the
// producer's first and only ledger-key acquisition on every isolation
// shape. The wide shape assigns the row_kind/logical_id pair; a
// pre-upgrade five-column catalog takes the names-only arm on the
// missing-column signal, mirroring the probe's two-shape degradation.
// D206(B.1) SETTER SITE (2) marks the sealing transaction. Errors
// propagate unswallowed (D171/D201).
func sealPkMoveRetentionRowShared(
	sqlproc *sqlexec.SqlProcess,
	proc *process.Process,
	tableID uint64,
) error {
	if pkMoveRetentionSealDisabled.Load() {
		// D206(C.1) seal-removed control: no seal staged, no mark applied.
		return nil
	}
	// D206(B.1) SETTER SITE (2): the sealing transaction staged a
	// fabric-owned protocol artifact - idempotent and nil-safe.
	client.MarkTxnExpectedConflictIfSupported(proc.GetTxnOperator())
	stmt := fmt.Sprintf(
		"update %s.%s set %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s where %s = %d and %s = %d",
		catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionColRowKind,
		catalog.DataBranchLinkRetentionColTableID, catalog.DataBranchLinkRetentionColTableID,
		catalog.DataBranchLinkRetentionColDatabaseName, catalog.DataBranchLinkRetentionColDatabaseName,
		catalog.DataBranchLinkRetentionColTableName, catalog.DataBranchLinkRetentionColTableName,
		catalog.DataBranchLinkRetentionColObjID, catalog.DataBranchLinkRetentionColObjID,
		catalog.DataBranchLinkRetentionColCreatedTS, catalog.DataBranchLinkRetentionColCreatedTS,
		catalog.DataBranchLinkRetentionColLogicalID, catalog.DataBranchLinkRetentionColLogicalID,
		catalog.DataBranchLinkRetentionColRowKind, catalog.DataBranchLinkRetentionRowKindReal,
		catalog.DataBranchLinkRetentionColTableID, tableID,
	)
	res, err := pkMoveGenesisRunSql(sqlproc, stmt)
	if err != nil {
		if !databranchMissingColumnErr(err) {
			return err
		}
		// Legacy five-column catalog: no row_kind/logical_id columns.
		legacy := fmt.Sprintf(
			"update %s.%s set %s = %s, %s = %s, %s = %s, %s = %s, %s = %s where %s = %d",
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION,
			catalog.DataBranchLinkRetentionColTableID, catalog.DataBranchLinkRetentionColTableID,
			catalog.DataBranchLinkRetentionColDatabaseName, catalog.DataBranchLinkRetentionColDatabaseName,
			catalog.DataBranchLinkRetentionColTableName, catalog.DataBranchLinkRetentionColTableName,
			catalog.DataBranchLinkRetentionColObjID, catalog.DataBranchLinkRetentionColObjID,
			catalog.DataBranchLinkRetentionColCreatedTS, catalog.DataBranchLinkRetentionColCreatedTS,
			catalog.DataBranchLinkRetentionColTableID, tableID,
		)
		res, err = pkMoveGenesisRunSql(sqlproc, legacy)
		if err != nil {
			return err
		}
	}
	res.Close()
	return nil
}

// pkMoveGenesisOwnedLockFn is the OWNED-genesis half of the locking
// probe (D186(B.4)(a), untouched by D199): ONE EXCLUSIVE row lock on the
// missed identity key - the staged insert's own row lock, established
// explicitly because the marked genesis row bypasses the DML planner's
// lock target. A variable so fabric cells can record the request site;
// production performs the real lock.
var pkMoveGenesisOwnedLockFn = func(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	accountID uint32,
	tableID uint64,
) error {
	return lockPkMoveRetentionRow(ctx, proc, eng, accountID, tableID)
}

// lockPkMoveRetentionRow resolves the retention relation under the
// recording account and acquires ONE EXCLUSIVE row lock on the addressed
// identity key in this transaction, held to transaction end (D186(B.4)(a)).
// The lock rides lock group 0 so it meets every plan-emitted row lock the
// protocol's writers and paired deletes take (performLock never overrides
// the default group); a non-zero group would land on a different lock table
// and silently never conflict. The key batch derives from the retention
// relation's LIVE primary-key shape via the one shared contract
// (databranchutils.PkMoveRetentionRowLockKey), covering both the wide
// composite-pk catalog and the pre-upgrade dev-window catalog keyed by
// plain table_id.
//
// D197(B.1)/D199(B.2) final shape: the producer path requests NO admission
// entry and NO shared acquisition on any isolation shape - the request
// below rides the ordinary lock path with the platform WAIT policy; the HIT
// lane's writer-fair shared anchor is RETIRED and the seal's ordinary
// first-touch exclusive (the byte-identity self-update's own DML lock) is
// that lane's only ledger-key acquisition. Optimistic transactions lock
// nothing anywhere in the pipeline (lockRows's own gate) and acquire
// nothing here either.
func lockPkMoveRetentionRow(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	accountID uint32,
	tableID uint64,
) error {
	txnOp := proc.GetTxnOperator()
	if txnOp == nil || !txnOp.Txn().IsPessimistic() {
		return nil
	}
	if eng == nil {
		// Fabric and harness paths arrive without the transaction's own
		// engine; the session-attached one covers those contexts.
		eng = proc.GetSessionInfo().StorageEngine
	}
	if eng == nil {
		return moerr.NewInternalErrorNoCtx(
			"pk-move genesis probe: storage engine absent")
	}
	actx := defines.AttachAccountId(ctx, accountID)
	db, err := eng.Database(actx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	rel, err := db.Relation(actx, catalog.MO_DATA_BRANCH_LINK_RETENTION, nil)
	if err != nil {
		return err
	}
	keys, pkType, err := databranchutils.PkMoveRetentionRowLockKey(
		proc.Mp(), rel.GetTableDef(actx), tableID)
	if err != nil {
		return err
	}
	defer keys.Clean(proc.Mp())
	physTableID := rel.GetTableID(actx)
	origCtx := proc.Ctx
	newCtx := defines.AttachAccountId(origCtx, accountID)
	proc.Ctx = newCtx
	defer func() { proc.Ctx = origCtx }()
	return lockop.LockRowsWithWaitPolicy(
		eng, proc, rel, physTableID, keys, 0, pkType,
		lock.LockMode_Exclusive, lock.Sharding_None, 0, lock.WaitPolicy_Wait)
}

func pkMoveGenesisProbeExists(sqlproc *sqlexec.SqlProcess, sql string) (bool, error) {
	res, err := pkMoveGenesisRunSql(sqlproc, sql)
	if err != nil {
		return false, err
	}
	found := false
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n > 0 {
			found = true
		}
		return true
	})
	res.Close()
	return found, nil
}

func databranchMissingColumnErr(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrBadFieldError)
}
