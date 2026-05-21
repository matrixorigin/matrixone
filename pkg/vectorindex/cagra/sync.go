//go:build gpu

// Copyright 2022 Matrix Origin
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

package cagra

// CagraSync is the CDC sync object for a single CAGRA index. It runs in the
// indexConsumer goroutine in pkg/iscp (NOT server-side of any SQL function —
// the cagra_cdc_update / hnsw_cdc_update SQL builtin layer mentioned in
// earlier drafts of cuvs_cdc.md does not exist; the writer's ToSql() returns
// raw JSON and the consumer hands it directly to Update).
//
// CagraSync is **stateless across flushes**: it never loads the model tar
// (tag=0) and never reads prior tag=1 contents. Each Update encodes incoming
// CDC events directly into op-tagged records and Save appends them as new
// tag=1 chunks under the fixed vectorindex.CdcTailId sentinel. Per-pkid
// last-event-wins falls out of replay at search-side load time.
//
// Why a fixed CdcTailId rather than a per-sub-index id:
//  1. A build can produce multiple sub-indexes (one per IndexCapacity worth
//     of rows), all sharing one timestamp in the metadata table. ORDER BY
//     timestamp ASC has no deterministic tie-break, so two LoadMetadata
//     calls could disagree on which sub-index is "newest" — and CDC writes
//     would oscillate between sub-index ids across iterations.
//  2. With a unified event log, oscillation would still split events across
//     sub-index ids and break replay's monotonic chunk_id ordering.
//  3. Routing every CDC write to CdcTailId avoids both — chunk_ids form a
//     single global sequence.
//
// Concurrency invariant: CDC for a given index is single-threaded, and
// re-index quiesces the CDC consumer before clearing the storage table.
// CdcTailId rows are wiped by re-index's existing DELETE FROM idx_table.

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var runTxn = sqlexec.RunTxn

// CagraSync buffers encoded event records for one CDC flush.
type CagraSync struct {
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	idxname string

	// activeIndexId is always vectorindex.CdcTailId (kept as a field so the
	// SQL-emit helpers stay parameterized rather than hard-coding the
	// constant in dozens of fmt.Sprintfs).
	activeIndexId string

	dim                int
	includeBytesPerRow int
	colMetaJSON        string

	// pendingRecords / pendingSizes accumulate encoded records ready to be
	// appended as new tag=1 chunks at Save time. Reset on Save.
	pendingRecords []byte
	pendingSizes   []int

	// counters surfaced in logutil.Infof; mirror HnswSync's shape.
	ninsert atomic.Int32
	ndelete atomic.Int32
	nupdate atomic.Int32
}

// NewCagraSync constructs a sync object. colMetaJSON describes the INCLUDE
// column layout (parsed by cuvscdc.CdcIncludeBytesPerRow); pass "" for
// indexes without INCLUDE columns.
func NewCagraSync(
	sqlproc *sqlexec.SqlProcess,
	db string,
	tbl string,
	idxname string,
	idxdefs []*plan.IndexDef,
	dimension int32,
	colMetaJSON string,
) (*CagraSync, error) {
	if dimension <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("CagraSync: invalid dimension")
	}

	var idxtblcfg vectorindex.IndexTableConfig
	idxtblcfg.DbName = db
	idxtblcfg.SrcTable = tbl

	for _, idxdef := range idxdefs {
		switch idxdef.IndexAlgoTableType {
		case catalog.Cagra_TblType_Metadata:
			idxtblcfg.MetadataTable = idxdef.IndexTableName
		case catalog.Cagra_TblType_Storage:
			idxtblcfg.IndexTable = idxdef.IndexTableName
		}
	}
	if idxtblcfg.MetadataTable == "" || idxtblcfg.IndexTable == "" {
		return nil, moerr.NewInternalErrorNoCtx("CagraSync: missing metadata or storage table in idxdefs")
	}

	var idxcfg vectorindex.IndexConfig
	idxcfg.Type = vectorindex.CAGRA
	idxcfg.CuvsCagra.Dimensions = uint(dimension)

	includeBytesPerRow, err := cuvscdc.CdcIncludeBytesPerRow(colMetaJSON)
	if err != nil {
		return nil, err
	}

	s := &CagraSync{
		idxcfg:             idxcfg,
		tblcfg:             idxtblcfg,
		idxname:            idxname,
		dim:                int(dimension),
		includeBytesPerRow: includeBytesPerRow,
		colMetaJSON:        colMetaJSON,
		activeIndexId:      vectorindex.CdcTailId,
	}
	return s, nil
}

// RunOnce is the standard one-shot pattern: Update + Save + Destroy.
func (s *CagraSync) RunOnce(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[float32]) (err error) {
	defer s.Destroy()
	if err = s.Update(sqlproc, cdc); err != nil {
		return err
	}
	return s.Save(sqlproc)
}

// Destroy releases the in-memory pending buffer. Safe to call multiple times.
func (s *CagraSync) Destroy() {
	s.pendingRecords = nil
	s.pendingSizes = nil
}

// Update encodes a CDC batch into the pending event-record buffer. Each
// CDC event maps 1:1 to a wire record:
//
//	CDC_INSERT → CdcOpInsert
//	CDC_UPSERT → CdcOpUpsert (distinct from INSERT so the idxcron count
//	             gate can ignore UPSERTs — MO UPSERT may be a duplicate
//	             or replay-from-corruption, only INSERT is guaranteed
//	             new-row)
//	CDC_DELETE → CdcOpDelete
//
// Replay handles UPSERT and INSERT identically (idempotent overflow-map
// write), so the previous DELETE+INSERT decomposition for UPSERTs is no
// longer needed for correctness — and removing it stops over-counting
// inserts in the frame's n_inserts counter.
func (s *CagraSync) Update(sqlproc *sqlexec.SqlProcess, cdc *vectorindex.VectorIndexCdc[float32]) error {
	start := time.Now()

	var ninsert, nupdate, ndelete int32
	for _, e := range cdc.Data {
		switch e.Type {
		case vectorindex.CDC_DELETE:
			if err := s.appendRecord(cuvscdc.CdcOpDelete, e.PKey, nil, nil); err != nil {
				return err
			}
			ndelete++
		case vectorindex.CDC_INSERT:
			if err := s.appendRecord(cuvscdc.CdcOpInsert, e.PKey, e.Vec, e.IncludeBytes); err != nil {
				return err
			}
			ninsert++
		case vectorindex.CDC_UPSERT:
			if err := s.appendRecord(cuvscdc.CdcOpUpsert, e.PKey, e.Vec, e.IncludeBytes); err != nil {
				return err
			}
			nupdate++
		default:
			return moerr.NewInternalErrorNoCtx("CagraSync: unknown CDC event type " + e.Type)
		}
	}

	s.ninsert.Store(ninsert)
	s.nupdate.Store(nupdate)
	s.ndelete.Store(ndelete)
	logutil.Infof("CAGRA cdc[%p]: db=%s table=%s index=%s len=%d ins=%d del=%d upd=%d elapsed=%dms",
		s, s.tblcfg.DbName, s.tblcfg.SrcTable, s.idxname,
		len(cdc.Data), ninsert, ndelete, nupdate, time.Since(start).Milliseconds())
	return nil
}

// AppendRecords appends pre-encoded EncodeEventRecord byte chunks
// produced by the CDC writer (pkg/vectorindex/cagra/plugin/iscp) to
// the pending buffer. Used by RunCuvs (pkg/iscp) when draining the
// consumer's send channel — skips the per-event EncodeEventRecord
// call that Update does internally, so the wire format is the same
// bytes Save will eventually flush to the storage table.
//
// Walks the byte stream once to recover per-record sizes for
// pendingSizes (cheap: each record's size is determined by its op
// byte + the index's dim + includeBytesPerRow).
func (s *CagraSync) AppendRecords(_ *sqlexec.SqlProcess, recordBytes []byte) error {
	before := len(s.pendingSizes)
	pos := 0
	for pos < len(recordBytes) {
		op := cuvscdc.CdcOp(recordBytes[pos])
		var n int
		switch op {
		case cuvscdc.CdcOpDelete:
			n = 9 // op (1) + pkid (8)
		case cuvscdc.CdcOpInsert, cuvscdc.CdcOpUpsert:
			// UPSERT shares INSERT's payload shape; only the op byte differs.
			n = 9 + 4*s.dim + s.includeBytesPerRow
		default:
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"CagraSync.AppendRecords: unknown op %d at offset %d", op, pos))
		}
		if pos+n > len(recordBytes) {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"CagraSync.AppendRecords: truncated record at offset %d (need %d, have %d)",
				pos, n, len(recordBytes)-pos))
		}
		s.pendingSizes = append(s.pendingSizes, n)
		pos += n
	}
	s.pendingRecords = append(s.pendingRecords, recordBytes...)
	logutil.Infof("[plugin] cagra Sync.AppendRecords IN: index=%s records=%d bytes=%d pending=%d",
		s.idxname, len(s.pendingSizes)-before, len(recordBytes), len(s.pendingSizes))
	return nil
}

// appendRecord encodes a single record onto the pending buffer.
func (s *CagraSync) appendRecord(op cuvscdc.CdcOp, pkid int64, vec []float32, include []byte) error {
	if op == cuvscdc.CdcOpInsert || op == cuvscdc.CdcOpUpsert {
		if len(vec) != s.dim {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"CagraSync.appendRecord: vec length %d != dim %d", len(vec), s.dim))
		}
		if s.includeBytesPerRow > 0 && len(include) != s.includeBytesPerRow {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"CagraSync.appendRecord: include bytes length %d != includeBytesPerRow %d",
				len(include), s.includeBytesPerRow))
		}
		if s.includeBytesPerRow == 0 && len(include) != 0 {
			return moerr.NewInternalErrorNoCtx(
				"CagraSync.appendRecord: include bytes supplied but index has no INCLUDE columns")
		}
	}
	before := len(s.pendingRecords)
	out, err := cuvscdc.EncodeEventRecord(s.pendingRecords, op, pkid, vec, include, s.dim, s.includeBytesPerRow)
	if err != nil {
		return err
	}
	s.pendingRecords = out
	s.pendingSizes = append(s.pendingSizes, len(s.pendingRecords)-before)
	return nil
}

// Save appends the pending records as new tag=1 chunks at the next available
// chunk_id. Append-only — never rewrites existing chunks. The model tar
// (tag=0) is never touched.
func (s *CagraSync) Save(sqlproc *sqlexec.SqlProcess) error {
	if len(s.pendingSizes) == 0 {
		return nil
	}
	nRecords := len(s.pendingSizes)
	nBytes := len(s.pendingRecords)
	nextId, err := s.nextChunkId(sqlproc, vectorindex.Tag_CdcEvents)
	if err != nil {
		return err
	}
	// Pass the captured colMetaJSON so each emitted chunk's frame
	// header carries the INCLUDE-column layout. Search-side replay
	// uses it when no tag=0 sub-index is loaded (small-data-only
	// indexes); when a sub-index IS loaded the search prefers the
	// model tar's colMetaJSON, so the redundancy is harmless.
	sqls := cuvscdc.CdcAppendEventsSql(s.tblcfg, s.activeIndexId, nextId, s.pendingRecords, s.pendingSizes, s.colMetaJSON)
	if len(sqls) == 0 {
		return nil
	}
	if err = s.runSqls(sqlproc, sqls); err != nil {
		return err
	}
	logutil.Infof("[plugin] cagra Sync.Save OUT: index=%s records=%d bytes=%d chunks=%d startChunkId=%d",
		s.idxname, nRecords, nBytes, len(sqls), nextId)
	// Reset pending buffer; subsequent Update + Save cycles re-grow it.
	s.pendingRecords = s.pendingRecords[:0]
	s.pendingSizes = s.pendingSizes[:0]

	// search-side caches load (tag=0+tag=1) at fault time, keyed by
	// index_id; bump the cache so the next search reloads the new events.
	veccache.Cache.Remove(s.tblcfg.IndexTable)
	return nil
}

// nextChunkId returns the chunk_id one past the current MAX(chunk_id) for
// (activeIndexId, tag), or 0 if no rows exist for that tag.
func (s *CagraSync) nextChunkId(sqlproc *sqlexec.SqlProcess, tag vectorindex.ChunkTag) (int64, error) {
	sql := cuvscdc.NextChunkIdSql(s.tblcfg, s.activeIndexId, tag)
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat.RowCount() == 0 {
			continue
		}
		return vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0), nil
	}
	return 0, nil
}

// runSqls executes a batch of SQL statements in a single transaction.
func (s *CagraSync) runSqls(sqlproc *sqlexec.SqlProcess, sqls []string) error {
	if len(sqls) == 0 {
		return nil
	}
	opts := executor.Options{}
	return runTxn(sqlproc, func(exec executor.TxnExecutor) error {
		for _, sql := range sqls {
			res, err := exec.Exec(sql, opts.StatementOption())
			if err != nil {
				return err
			}
			res.Close()
		}
		return nil
	})
}
