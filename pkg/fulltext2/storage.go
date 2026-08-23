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

package fulltext2

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// TableConfig locates a fulltext2 index's persistent segment store + metadata
// table; it is the JSON const arg passed to the fulltext2_create / fulltext2_search
// TVFs. Mirrors bm25.wand.TableConfig.
type TableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	IndexTable    string `json:"index"`    // chunk store (FullText2Index_TblType_Storage)
	MetadataTable string `json:"metadata"` // metadata (FullText2Index_TblType_Metadata)
	PKey          string `json:"pkey"`
	Parser        string `json:"parser,omitempty"`
	// Capacity is max_index_capacity: the create build splits the tag=0 base into
	// sub-indexes of at most Capacity docs each (0 => DefaultBuildCapacity).
	Capacity int64 `json:"capacity,omitempty"`
	// PostingCapacity is max_postings_capacity: a segment also seals once it holds
	// this many postings (term occurrences), whichever comes first with Capacity.
	// Bounds per-segment build memory regardless of doc size (0 => DefaultPostingCapacity).
	PostingCapacity int64 `json:"posting_capacity,omitempty"`
	// PositionFree builds segments without the positional payload (bag-of-words
	// retrieval only; ~half the footprint, FST kept). Sourced from the persisted
	// position_free algo_param so every build path (create, CDC tail, compact) agrees.
	PositionFree bool `json:"position_free,omitempty"`
	FromSource   bool `json:"from_source,omitempty"`
	// IncludeTypes is the INCLUDE columns' types.T in column order. The build SQL passes the
	// INCLUDE source columns as the trailing fulltext2_create args (after the text columns),
	// so the TVF knows how many trailing argVecs are INCLUDE values (vs text) and their types
	// to store the actual per-doc value in the docmap. nil ⇒ no INCLUDE columns.
	IncludeTypes []int32 `json:"include_types,omitempty"`
	// IncludeColumns is the INCLUDE columns' NAMES in column order — carried in the SEARCH
	// cfg so Fulltext2Search.Search can map a covering query's RequestedIncludeColumns (by
	// name) to each result's positional Include values. nil ⇒ no INCLUDE columns.
	IncludeColumns []string `json:"include_columns,omitempty"`
}

// runSql / runStreamingSql indirect the sqlexec executor entry points so unit tests can
// mock the DB round-trips (metadata reads, base/tail chunk loads, budget gates,
// compaction). Production points them at the real executor; tests swap them for fakes
// that return synthetic result batches, optionally dispatching by SQL text.
var (
	runSql          = sqlexec.RunSql
	runStreamingSql = sqlexec.RunStreamingSql
)

// SubIndexId is the index_id for the i-th tag=0 base sub-index of a build
// identified by uid (which must carry a per-build-unique component so concurrent
// builds never collide). Load enumerates ids from the metadata table.
func SubIndexId(uid string, i int) string { return fmt.Sprintf("%s:%d", uid, i) }

// maxInsertTuples bounds the (index_id, chunk_id, data, tag) rows per INSERT (base-segment and
// batched CDC-tail persist). Its primary job is cutting RunSql round-trips — many tiny frames become
// a few multi-row INSERTs. It is kept MODERATE (500, well below hnsw's 2000 in
// pkg/vectorindex/hnsw/model.go) because each row's load_file data flows into the storage engine's
// ASYNC object writer (file/S3), which buffers-and-flushes and thus accumulates memory as rows are
// pushed; fulltext2 CDC persist is a BACKGROUND op, so a smaller batch hands that writer a bounded
// slice (<= maxInsertTuples*MaxChunkSize ≈ 32 MiB) per statement instead of a large burst. NOTE: this is only
// a secondary throttle — the dominant memory bound is the TOTAL data pushed per iteration (the whole
// tail delta), which the per-iteration work bound (not batch size) is what actually caps.
const maxInsertTuples = 500

// ToInsertSqls serializes the segment, spills it to a temp file under the LOCAL
// fileservice's __fulltext2 dir (load_file reads it back by path), and emits the
// SQL to persist it: one metadata row + the bytes split into <= MaxChunkSize
// (index_id, chunk_id, data, tag) rows read via load_file. tag=0 is a compacted
// base (sync CREATE/REINDEX build); tag=1 is a CDC delta. sqlproc resolves the LOCAL
// SSD spill dir (falls back to /tmp when none is attached). The returned cleanup MUST
// run after the SQLs execute (they read the temp file at execution).
func (s *Segment) ToInsertSqls(sqlproc *sqlexec.SqlProcess, cfg TableConfig, ts int64, tag int) (sqls []string, cleanup func(), err error) {
	buf, err := s.Serialize()
	if err != nil {
		return nil, nil, err
	}
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	fp, err := createLocalSpillFile(sqlproc, "ft2build")
	if err != nil {
		return nil, nil, err
	}
	path := fp.Name()
	cleanup = func() { fp.Close(); os.Remove(path) }
	if _, err = fp.Write(buf); err != nil {
		cleanup()
		return nil, nil, err
	}
	if err = fp.Sync(); err != nil {
		cleanup()
		return nil, nil, err
	}

	metaTbl := sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable)
	sqls = append(sqls, fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (%s, %d, %s, %d, %d, %d)",
		metaTbl,
		catalog.FullText2Index_TblCol_Metadata_Index_Id, catalog.FullText2Index_TblCol_Metadata_Timestamp,
		catalog.FullText2Index_TblCol_Metadata_Checksum, catalog.FullText2Index_TblCol_Metadata_Filesize,
		catalog.FullText2Index_TblCol_Metadata_Recency, catalog.FullText2Index_TblCol_Metadata_Nrow,
		sqlquote.String(s.Id), ts, sqlquote.String(checksum), filesize, s.Recency, s.N))
	sqls = append(sqls, fileChunkInsertSqls(cfg, s.Id, 0, path, 0, int(filesize), tag)...)
	return sqls, cleanup, nil
}

// fileChunkRange is one contiguous [offset, offset+length) byte range of a spilled file to persist
// as storage rows: offset 0 for a whole-file base segment, or a frame's offset within a PACKED CDC
// spool file (many frames share one file).
type fileChunkRange struct {
	path   string
	offset int64
	length int
}

// framesInsertSqls emits the load_file storage-row tuples for a SEQUENCE of file ranges, each range
// split into <= MaxChunkSize rows, batched at maxInsertTuples rows per INSERT statement
// ACROSS range boundaries. chunk_ids are assigned contiguously from startChunkId in range order;
// the next free chunk_id is returned. Batching across ranges is what keeps a burst of tiny CDC tail
// frames to O(totalChunks/maxInsertTuples) statements instead of one INSERT per frame. Mirrors
// bm25.wand.FileChunkInsertSqls (single-range) generalized to many ranges.
func framesInsertSqls(cfg TableConfig, id string, startChunkId int64, ranges []fileChunkRange, tag int) (sqls []string, nextChunkId int64) {
	prefix := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES ",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, catalog.FullText2Index_TblCol_Storage_Chunk_Id,
		catalog.FullText2Index_TblCol_Storage_Data, catalog.FullText2Index_TblCol_Storage_Tag)
	var vals []string
	chunkID := startChunkId
	for _, r := range ranges {
		for off := 0; off < r.length; off += vectorindex.MaxChunkSize {
			sz := vectorindex.MaxChunkSize
			if off+sz > r.length {
				sz = r.length - off
			}
			url := fmt.Sprintf("file://%s?offset=%d&size=%d", r.path, r.offset+int64(off), sz)
			vals = append(vals, fmt.Sprintf("(%s, %d, load_file(cast(%s as datalink)), %d)",
				sqlquote.String(id), chunkID, sqlquote.String(url), tag))
			chunkID++
			if len(vals) == maxInsertTuples {
				sqls = append(sqls, prefix+strings.Join(vals, ", "))
				vals = vals[:0]
			}
		}
	}
	if len(vals) > 0 {
		sqls = append(sqls, prefix+strings.Join(vals, ", "))
	}
	return sqls, chunkID
}

// fileChunkInsertSqls splits a [baseOffset, baseOffset+dataLen) BYTE RANGE of a spilled file into
// <= MaxChunkSize storage rows read via load_file (no hex/unhex). Single-range convenience over
// framesInsertSqls (used by the base-segment persist).
func fileChunkInsertSqls(cfg TableConfig, id string, startChunkId int64, path string, baseOffset int64, dataLen int, tag int) []string {
	sqls, _ := framesInsertSqls(cfg, id, startChunkId, []fileChunkRange{{path: path, offset: baseOffset, length: dataLen}}, tag)
	return sqls
}

// TailFileInsertSqls persists ONE spilled tag=1 CDC frame (index_id=CdcTailId) that lives at
// [offset, offset+frameLen) within a packed spool file. Its chunk rows are contiguous from
// startChunkId, so recency (chunk_id) ordering across frames is unchanged by the packing.
func TailFileInsertSqls(cfg TableConfig, startChunkId int64, path string, offset int64, frameLen int) []string {
	return fileChunkInsertSqls(cfg, vectorindex.CdcTailId, startChunkId, path, offset, frameLen, int(vectorindex.Tag_CdcEvents))
}

// TailFramesInsertSqls persists ALL of an iteration's spilled tag=1 CDC frames as FEW multi-row
// INSERTs: the per-chunk load_file tuples of every frame, batched at maxInsertTuples rows per
// statement ACROSS frame boundaries — so N tiny frames cost ~N/maxInsertTuples statements (and
// RunSql round-trips) instead of one INSERT per frame. chunk_ids are contiguous from startChunkId in frame
// order (recency identical to a per-frame persist); the next free chunk_id (end of the tail range)
// is returned.
func TailFramesInsertSqls(cfg TableConfig, startChunkId int64, frames []TailSegment) (sqls []string, nextChunkId int64) {
	ranges := make([]fileChunkRange, len(frames))
	for i, f := range frames {
		ranges[i] = fileChunkRange{path: f.Path, offset: f.Offset, length: f.FrameLen}
	}
	return framesInsertSqls(cfg, vectorindex.CdcTailId, startChunkId, ranges, int(vectorindex.Tag_CdcEvents))
}

// DeleteSqls removes one index id's chunks + metadata row (rebuild idempotency).
func DeleteSqls(cfg TableConfig, id string) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(id)),
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
			catalog.FullText2Index_TblCol_Metadata_Index_Id, sqlquote.String(id)),
	}
}

// DeleteAllBasesSqls removes every tag=0 base (all sub-index chunk rows + all
// metadata rows); the tag=1 CdcTail is untouched. Makes CREATE idempotent.
func DeleteAllBasesSqls(cfg TableConfig) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %d", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_ModelChunk)),
		fmt.Sprintf("DELETE FROM %s WHERE TRUE", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable)),
	}
}

// DeleteTailSqls removes the entire tag=1 CdcTail (a REINDEX discards the delta log).
func DeleteTailSqls(cfg TableConfig) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %d", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents)),
	}
}

// readMetadata fetches an index id's metadata (checksum + filesize + recency).
func readMetadata(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (checksum string, filesize, recency int64, found bool, err error) {
	sql := fmt.Sprintf("SELECT %s, %s, %s FROM %s WHERE %s = %s",
		catalog.FullText2Index_TblCol_Metadata_Checksum, catalog.FullText2Index_TblCol_Metadata_Filesize,
		catalog.FullText2Index_TblCol_Metadata_Recency,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
		catalog.FullText2Index_TblCol_Metadata_Index_Id, sqlquote.String(id))
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return "", 0, 0, false, err
	}
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		checksum = bat.Vecs[0].GetStringAt(0)
		filesize = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
		recency = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[2], 0)
		found = true
		break
	}
	res.Close()
	return checksum, filesize, recency, found, nil
}

// estBytesPerDocHeap approximates the Go-heap cost of loading ONE document's
// index-level metadata: the docmap slot (pk `any` + docLen) plus the per-doc
// liveness structures NewIndex builds from it (liveLoc/top map entries + a liveOrd
// bit). Base posting blocks are mmap'd (reclaimable OS page cache) and are excluded —
// they cannot OOM-kill the CN. It is deliberately generous so checkBaseLoadBudget
// fails a doomed load fast rather than letting it crash the node; a modest
// over-estimate only rejects a borderline load (recoverable via compaction / more
// memory), never silently OOMs.
const estBytesPerDocHeap = 256

// checkBaseLoadBudget fails fast if loading every tag=0 base's per-doc metadata into
// the Go heap would blow the CN memory budget — the base analogue of
// checkTailLoadBudget (which guards the tail). It gates on SUM(nrow) (doc count), NOT
// filesize: the bulk of a base (posting blocks) is mmap'd and reclaimable, so only the
// O(docs) docmap + NewIndex liveness maps land on the Go heap. Budget = MemoryTotal*0.8
// - live Go heap (cgroup-aware). Turns a node-killing OOM into a clear, actionable
// error suggesting compaction.
func checkBaseLoadBudget(sqlproc *sqlexec.SqlProcess, cfg TableConfig) error {
	// CAST AS SIGNED so the sum reads back as int64 regardless of how SUM types its
	// result (GetFixedAtNoTypeCheck[int64] would misread a decimal vector).
	sql := fmt.Sprintf("SELECT CAST(COALESCE(SUM(%s), 0) AS SIGNED) FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Nrow, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return err
	}
	defer res.Close()
	var ndoc int64
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		ndoc = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
	}
	need := ndoc * estBytesPerDocHeap
	avail := int64(system.MemoryTotal())*8/10 - int64(system.MemoryGolang())
	if need > avail {
		return moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf(
			"fulltext2 index %s.%s needs ~%d MB of heap for %d docs' metadata but only ~%d MB is free "+
				"(MemoryTotal*0.8 - Go heap). If the index has many deleted rows, MERGE "+
				"(ALTER TABLE ... ALTER REINDEX ... MERGE) reclaims them and may bring it under budget; "+
				"otherwise the live index is too large for this CN — add memory",
			cfg.DbName, cfg.IndexTable, need>>20, ndoc, avail>>20))
	}
	return nil
}

// LoadAllBases loads every tag=0 base sub-index listed in the metadata table.
// Returns nil when no base was built (empty-table create → CDC-only index).
//
// NOTE: the heap-budget guard (checkBaseLoadBudget) is deliberately NOT called here.
// This is shared by the query load path AND CompactSegments (MERGE). Guarding it here
// would make the guard self-blocking — MERGE is the very remedy the guard's error
// suggests, but MERGE must load the bases to reclaim dead docs. The query path calls
// the guard itself (Fulltext2Search.Load); MERGE stays exempt so it can always run.
func LoadAllBases(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*Segment, error) {
	return loadAllBasesUncached(sqlproc, cfg, nil)
}

func loadAllBasesUncached(sqlproc *sqlexec.SqlProcess, cfg TableConfig, trace *loadTrace) ([]*Segment, error) {
	idSQL := fmt.Sprintf("SELECT %s FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Index_Id, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	var sqlStart time.Time
	if trace != nil {
		sqlStart = time.Now()
	}
	res, err := runSql(sqlproc, idSQL)
	if trace != nil {
		trace.addInternalSQL(time.Since(sqlStart))
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		for i := 0; i < bat.RowCount(); i++ {
			ids = append(ids, bat.Vecs[0].GetStringAt(i))
		}
	}
	res.Close()
	bases := make([]*Segment, 0, len(ids))
	for _, id := range ids {
		m, lerr := loadFromStorage(sqlproc, cfg, id, trace)
		if lerr != nil {
			freeSegs(bases)
			return nil, lerr
		}
		bases = append(bases, m)
	}
	return bases, nil
}

func loadAllBases(sqlproc *sqlexec.SqlProcess, cfg TableConfig, trace *loadTrace) ([]*Segment, error) {
	idSQL := fmt.Sprintf("SELECT %s FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Index_Id, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	var sqlStart time.Time
	if trace != nil {
		sqlStart = time.Now()
	}
	res, err := runSql(sqlproc, idSQL)
	if trace != nil {
		trace.addInternalSQL(time.Since(sqlStart))
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		for i := 0; i < bat.RowCount(); i++ {
			ids = append(ids, bat.Vecs[0].GetStringAt(i))
		}
	}
	res.Close()

	bases := make([]*Segment, 0, len(ids))
	used := make(map[baseKey]struct{}, len(ids))
	for _, id := range ids {
		var metaStart time.Time
		if trace != nil {
			metaStart = time.Now()
		}
		checksum, filesize, recency, found, lerr := readMetadata(sqlproc, cfg, id)
		if trace != nil {
			trace.addInternalSQL(time.Since(metaStart))
		}
		if lerr != nil {
			freeSegs(bases)
			return nil, lerr
		}
		if !found {
			freeSegs(bases)
			return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s metadata not found", id))
		}
		if filesize <= 0 {
			freeSegs(bases)
			return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s has empty filesize", id))
		}
		key := baseKey{index: cfg.DbName + "." + cfg.IndexTable, id: id, checksum: checksum, filesize: filesize}
		m, lerr := loadedBasePool.acquire(sqlproc.GetContext(), key, func() (*Segment, error) {
			return loadFromStorageMetadata(sqlproc, cfg, id, checksum, filesize, recency, trace)
		}, recency)
		if lerr != nil {
			// Free the segments already mapped this call before bailing: each owns an
			// mmap (+ a linked /tmp spill file on the fallback path), so returning without
			// freeing leaks them, and a retrying caller (query reload / MERGE) accumulates
			// both. Mirrors LoadTailSegments' freeSegs(bases) on its own error path.
			freeSegs(bases)
			return nil, lerr
		}
		bases = append(bases, m)
		used[key] = struct{}{}
	}
	loadedBasePool.commit(cfg.DbName+"."+cfg.IndexTable, used)
	return bases, nil
}

// LoadFromStorage reads an index id's metadata + chunks, verifies the checksum,
// and deserializes it. Chunks stream by chunk_id offset into a temp file so the
// mpool never holds the whole index.
func LoadFromStorage(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (*Segment, error) {
	return loadFromStorage(sqlproc, cfg, id, nil)
}

func loadFromStorage(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string, trace *loadTrace) (*Segment, error) {
	var sqlStart time.Time
	if trace != nil {
		sqlStart = time.Now()
	}
	checksum, filesize, recency, found, err := readMetadata(sqlproc, cfg, id)
	if trace != nil {
		trace.addInternalSQL(time.Since(sqlStart))
	}
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s metadata not found", id))
	}
	if filesize <= 0 {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s has empty filesize", id))
	}
	return loadFromStorageMetadata(sqlproc, cfg, id, checksum, filesize, recency, trace)
}

func loadFromStorageMetadata(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id, checksum string, filesize, recency int64, trace *loadTrace) (*Segment, error) {
	if trace != nil {
		trace.addBaseBytes(filesize)
	}

	// Materialize the segment on the fast LOCAL (SSD) fileservice so mmap page faults
	// come off the 2 GB/s mount, not /tmp (128 MB/s on AWS). path=="" means an
	// anonymous SSD file (unlinked; munmap frees the inode) — the same
	// CreateAndRemoveFile the JOIN/mergeorder spill uses.
	fp, path, err := createLocalTempFile(sqlproc, "ft2idx")
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		fp.Close()
		if path != "" {
			os.Remove(path)
		}
	}
	if err = fp.Truncate(filesize); err != nil {
		cleanup()
		return nil, err
	}
	if err = streamChunksToFile(sqlproc, cfg, id, filesize, fp, trace); err != nil {
		cleanup()
		return nil, err
	}
	// mmap the file read-only (shared across queries; FST + positions are views into
	// it, page-cache-backed). The fd is not needed once mapped.
	var mmapStart time.Time
	if trace != nil {
		mmapStart = time.Now()
	}
	data, err := mmapReadOnly(fp)
	if trace != nil {
		trace.addMmap(time.Since(mmapStart))
	}
	fp.Close()
	if err != nil {
		if path != "" {
			os.Remove(path)
		}
		return nil, err
	}
	// Checksum the mapped bytes (the anonymous SSD file has no path to CheckSum).
	var checksumStart time.Time
	if trace != nil {
		checksumStart = time.Now()
	}
	actualChecksum := vectorindex.CheckSumFromBuffer(data)
	if trace != nil {
		trace.addChecksum(time.Since(checksumStart))
	}
	if actualChecksum != checksum {
		_ = munmap(data)
		if path != "" {
			os.Remove(path)
		}
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s checksum mismatch", id))
	}
	// The Segment OWNS the mapping (+ path for the /tmp fallback): Free() munmaps and,
	// if linked, deletes it.
	m := &Segment{Id: id, mmapData: data, mmapPath: path}
	if err := m.decodeSegment(data); err != nil {
		m.Free()
		return nil, err
	}
	m.Recency = recency
	return m, nil
}

// ft2LocalDir is fulltext2's own subdir under the LOCAL fileservice (on the SSD
// data-dir) for segment spill / mmap files — sibling to the JOIN spill's "__spill".
// EVERY fulltext2 temp file (the build-side load_file spill, the CDC tail spill, and
// the query-side mmap materialization) lives here so it lands on the fast SSD mount,
// not /tmp (128 MB/s on AWS).
const ft2LocalDir = "__fulltext2"

// localSpillDir returns the on-disk __fulltext2 scratch directory under the LOCAL
// (SSD) fileservice, creating it if absent. Returns "" when no LOCAL fileservice is
// attached (tests / one-shot tools) so callers fall back to the OS temp dir via
// os.CreateTemp("", …)/os.MkdirTemp("", …). Used by the build-side spill (load_file
// needs a real path) — the query mmap path uses the anonymous createLocalTempFile.
func localSpillDir(sqlproc *sqlexec.SqlProcess) string {
	if sqlproc == nil || sqlproc.Proc == nil {
		return ""
	}
	return LocalSpillDir(sqlproc.GetContext(), sqlproc.Proc.Base.FileService)
}

// LocalSpillDir resolves the __fulltext2 dir under the LOCAL fileservice of rootFS
// (the CN's root FileService), creating it. Engine-agnostic so the ISCP CDC tail can
// resolve it from the engine's FileService without a sqlproc. Returns "" when rootFS
// has no LOCAL fileservice.
func LocalSpillDir(ctx context.Context, rootFS fileservice.FileService) string {
	if rootFS == nil {
		return ""
	}
	local, err := fileservice.Get[*fileservice.LocalFS](rootFS, defines.LocalFileServiceName)
	if err != nil {
		return ""
	}
	dir := filepath.Join(local.RootPath(), ft2LocalDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ""
	}
	return dir
}

// createLocalSpillFile creates a NAMED temp file (prefix) under the LOCAL fileservice's
// __fulltext2 dir — its absolute on-disk path is what load_file reads back. Falls back
// to the OS temp dir when no LOCAL fileservice is attached (localSpillDir=="").
func createLocalSpillFile(sqlproc *sqlexec.SqlProcess, prefix string) (*os.File, error) {
	return os.CreateTemp(localSpillDir(sqlproc), prefix)
}

// createLocalTempFile returns a temp file for the segment's mmap. It prefers the
// LOCAL fileservice's __fulltext2 subdir (on the SSD data-dir) via the same
// CreateAndRemoveFile the JOIN spill uses — an ANONYMOUS file (unlinked; the fd +
// mapping keep the inode alive, Free just munmaps, no os.Remove). Falls back to
// os.CreateTemp (/tmp, linked → Free deletes by path) when no process/fileservice
// is attached (tests / one-shot tools). Returns (file, path, err); path=="" for the
// anonymous SSD file. name is the caller-chosen file-name prefix.
func createLocalTempFile(sqlproc *sqlexec.SqlProcess, name string) (*os.File, string, error) {
	if sqlproc != nil && sqlproc.Proc != nil {
		ctx := sqlproc.GetContext()
		// LOCAL fileservice (SSD data-dir) -> ensure our __fulltext2 subdir -> a
		// MutableFileService rooted there (mirrors process.GetSpillFileService).
		if local, e := fileservice.Get[fileservice.MutableFileService](
			sqlproc.Proc.Base.FileService, defines.LocalFileServiceName); e == nil {
			if e2 := local.EnsureDir(ctx, ft2LocalDir); e2 == nil {
				if sub, ok := fileservice.SubPath(local, ft2LocalDir).(fileservice.MutableFileService); ok {
					if f, e3 := sub.CreateAndRemoveFile(ctx, name+"_"+uuid.NewString()); e3 == nil {
						return f, "", nil
					}
				}
			}
		}
	}
	f, err := os.CreateTemp("", name)
	if err != nil {
		return nil, "", err
	}
	return f, f.Name(), nil
}

// checkTailLoadBudget fails fast when the CDC tail's stored bytes exceed the CN memory
// budget, returning a clear, actionable error instead of letting the tail load OOM-kill
// the CN (which would take down EVERY query on the node, not just this one). The tail
// chunks are read into the Go HEAP (append([]byte)) and each frame Deserialized into a
// Go-heap segment, so SUM(LENGTH(data)) — the ACTUAL stored bytes — approximates the load
// footprint. The mmap'd base is reclaimable OS page cache and is deliberately NOT counted
// (it cannot cause an OOM-kill). Budget = MemoryTotal*0.8 - live Go heap; MemoryTotal is
// cgroup-aware, and MemoryGolang already includes any tails currently resident, so this
// gates an INCREMENTAL load. The 0.8 headroom absorbs the (small) transient query-mpool
// usage the formula omits.
func checkTailLoadBudget(sqlproc *sqlexec.SqlProcess, cfg TableConfig) error {
	return checkTailLoadBudgetAfter(sqlproc, cfg, -1, nil)
}

func checkTailLoadBudgetAfter(sqlproc *sqlexec.SqlProcess, cfg TableConfig, after int64, trace *loadTrace) error {
	where := fmt.Sprintf("%s = %s AND %s = %d",
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	if after >= 0 {
		where += fmt.Sprintf(" AND %s > %d", catalog.FullText2Index_TblCol_Storage_Chunk_Id, after)
	}
	sql := fmt.Sprintf("SELECT COALESCE(SUM(LENGTH(%s)), 0) FROM %s WHERE %s",
		catalog.FullText2Index_TblCol_Storage_Data, sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		where)
	var sqlStart time.Time
	if trace != nil {
		sqlStart = time.Now()
	}
	res, err := runSql(sqlproc, sql)
	if trace != nil {
		trace.addInternalSQL(time.Since(sqlStart))
	}
	if err != nil {
		return err
	}
	defer res.Close()
	var need int64
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		need = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
	}
	// LoadTailSegments holds SEVERAL full copies of the tail bytes at once — the raw
	// per-chunk []byte copies, the reassembled per-frame buffers, and the Deserialized
	// segments — so the real peak is a multiple of the stored bytes, not 1x. Scale the
	// estimate up so the guard fails BEFORE that peak OOM-kills the CN rather than after
	// the SUM passes. Conservative (may reject a borderline load that would just fit); a
	// clear "compact/add memory" error beats a node-killing OOM.
	const tailLoadPeakFactor = 3
	need *= tailLoadPeakFactor
	avail := int64(system.MemoryTotal())*8/10 - int64(system.MemoryGolang())
	if need > avail {
		return moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf(
			"fulltext2 CDC tail for %s.%s needs ~%d MB to load but only ~%d MB is free "+
				"(MemoryTotal*0.8 - Go heap); compact it (ALTER ... REINDEX) or increase CN memory",
			cfg.DbName, cfg.IndexTable, need>>20, avail>>20))
	}
	return nil
}

// LoadTailSegments loads the tag=1 CdcTail: the SELECTed chunk rows are ordered,
// reassembled into frames, and each insert frame decoded into a segment (its
// Recency = the frame's first chunk_id). Delete frames are folded into the pk
// tombstone map. Empty tail → (nil, nil, nil).
func LoadTailSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*Segment, map[any]int64, error) {
	return loadTailSegments(sqlproc, cfg, nil)
}

func loadTailSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig, trace *loadTrace) ([]*Segment, map[any]int64, error) {
	segs, deletes, _, err := loadTailSegmentsAfter(sqlproc, cfg, -1, trace)
	return segs, deletes, err
}

func loadTailSegmentsAfter(sqlproc *sqlexec.SqlProcess, cfg TableConfig, after int64, trace *loadTrace) ([]*Segment, map[any]int64, int64, error) {
	// Fail fast if the tail can't fit in the memory budget, rather than letting it
	// OOM-kill the CN as it decodes into the Go heap.
	if err := checkTailLoadBudgetAfter(sqlproc, cfg, after, trace); err != nil {
		return nil, nil, 0, err
	}
	where := fmt.Sprintf("%s = %s AND %s = %d",
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	if after >= 0 {
		where += fmt.Sprintf(" AND %s > %d", catalog.FullText2Index_TblCol_Storage_Chunk_Id, after)
	}
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s",
		catalog.FullText2Index_TblCol_Storage_Chunk_Id, catalog.FullText2Index_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		where)
	var sqlStart time.Time
	if trace != nil {
		sqlStart = time.Now()
	}
	res, err := runSql(sqlproc, sql)
	if trace != nil {
		trace.addInternalSQL(time.Since(sqlStart))
	}
	if err != nil {
		return nil, nil, 0, err
	}
	var chunks []TailChunk
	maxChunk := after
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		cids := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
		for i := 0; i < bat.RowCount(); i++ {
			data := bat.Vecs[1].GetRawBytesAt(i)
			if trace != nil {
				trace.addTailBytes(int64(len(data)))
			}
			if cids[i] > maxChunk {
				maxChunk = cids[i]
			}
			chunks = append(chunks, TailChunk{ChunkId: cids[i], Data: append([]byte(nil), data...)})
		}
	}
	res.Close()
	if len(chunks) == 0 {
		return nil, nil, maxChunk, nil
	}

	ordered, err := orderTailChunks(chunks)
	if err != nil {
		return nil, nil, 0, err
	}
	frames, err := reassembleFrames(ordered)
	if err != nil {
		return nil, nil, 0, err
	}

	var segs []*Segment
	deletes := make(map[any]int64)
	for _, f := range frames {
		records, _, nInserts, nDeletes, _, uerr := cuvscdc.UnframeCdcChunk(f.Data)
		if uerr != nil {
			freeSegs(segs)
			return nil, nil, 0, uerr
		}
		switch {
		case nInserts > 0:
			seg, derr := Deserialize(fmt.Sprintf("tail-%d", f.ChunkId), bytes.NewReader(records))
			if derr != nil {
				freeSegs(segs)
				return nil, nil, 0, derr
			}
			seg.Recency = f.ChunkId
			segs = append(segs, seg)
		case nDeletes > 0:
			recs, derr := DecodeDeleteLog(records)
			if derr != nil {
				freeSegs(segs)
				return nil, nil, 0, derr
			}
			deletes = foldDeleteFrame(deletes, recs, f.ChunkId)
		}
	}
	return segs, deletes, maxChunk, nil
}

// streamChunksToFile streams a tag=0 index's chunk rows, writing each at
// chunk_id*MaxChunkSize into fp; the assembled bytes must fill filesize exactly.
func streamChunksToFile(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string, filesize int64, fp *os.File, trace *loadTrace) error {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullText2Index_TblCol_Storage_Chunk_Id, catalog.FullText2Index_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(id))
	written, _, err := streamChunkRowsToFile(sqlproc, sql, 0, filesize, fp, trace)
	if err != nil {
		return err
	}
	if written != filesize {
		return moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("fulltext2 index %s incomplete: wrote %d of %d bytes", id, written, filesize))
	}
	return nil
}

// streamChunkRowsToFile streams the (chunk_id, data) rows of sql and writes each
// at (chunk_id-baseChunk)*MaxChunkSize into fp, bounding the mpool to the stream
// buffer. Returns bytes written + chunk-row count.
func streamChunkRowsToFile(sqlproc *sqlexec.SqlProcess, sql string, baseChunk, bound int64, fp *os.File, trace *loadTrace) (written, nchunks int64, err error) {
	streamCh := make(chan executor.Result, 2)
	errorCh := make(chan error, 2)
	ctx, cancel := context.WithCancelCause(sqlproc.GetTopContext())
	defer cancel(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			close(streamCh)
			wg.Done()
		}()
		var sqlStart time.Time
		if trace != nil {
			sqlStart = time.Now()
		}
		if _, e := runStreamingSql(ctx, sqlproc, sql, streamCh, errorCh); e != nil {
			errorCh <- e
		}
		if trace != nil {
			trace.addInternalSQL(time.Since(sqlStart))
		}
	}()

	var loopErr error
	closed := false
	seen := make(map[int64]struct{}) // chunk_ids already written, to catch duplicates
	for !closed {
		select {
		case res, ok := <-streamCh:
			if !ok {
				closed = true
				break
			}
			for _, bat := range res.Batches {
				if bat == nil || bat.RowCount() == 0 {
					continue
				}
				cids := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
				for i, cid := range cids {
					data := bat.Vecs[1].GetRawBytesAt(i)
					off := (cid - baseChunk) * int64(vectorindex.MaxChunkSize)
					if off < 0 || off+int64(len(data)) > bound {
						loopErr = moerr.NewInternalError(sqlproc.GetContext(),
							fmt.Sprintf("fulltext2 chunk_id %d out of range [base %d, bound %d]", cid, baseChunk, bound))
						break
					}
					// A duplicate (index_id, chunk_id) row would write its region twice and
					// inflate `written`, letting the caller's written==filesize check pass while
					// a DIFFERENT chunk's region is never filled (a hole the CRC only catches
					// after a full stream+mmap). Reject the duplicate here, fail-fast.
					if _, dup := seen[cid]; dup {
						loopErr = moerr.NewInternalError(sqlproc.GetContext(),
							fmt.Sprintf("fulltext2 duplicate chunk_id %d", cid))
						break
					}
					seen[cid] = struct{}{}
					var writeStart time.Time
					if trace != nil {
						writeStart = time.Now()
					}
					if _, e := fp.WriteAt(data, off); e != nil {
						loopErr = e
						break
					}
					if trace != nil {
						trace.addTempWrite(time.Since(writeStart))
					}
					written += int64(len(data))
					nchunks++
				}
				if loopErr != nil {
					break
				}
			}
			res.Close()
			if loopErr != nil {
				closed = true
			}
		case e := <-errorCh:
			loopErr = e
			closed = true
		case <-ctx.Done():
			loopErr = context.Cause(ctx)
			closed = true
		}
	}
	if loopErr != nil {
		cancel(loopErr)
	}
	for res := range streamCh {
		res.Close()
	}
	wg.Wait()
	if loopErr == nil {
		select {
		case e := <-errorCh:
			loopErr = e
		default:
		}
	}
	if loopErr != nil {
		return 0, 0, loopErr
	}
	return written, nchunks, nil
}

// NextTailChunkIdSql returns a SELECT for the next free tag=1 CdcTail chunk_id —
// the monotonic append position (= recency) the sinker frames at. It is
// GREATEST(MAX tail chunk_id, MAX tag=0 base recency) + 1, so an appended tail is
// always newer than every base under Index liveness, and the sequence never
// resets after a compaction folds the tail into a base. Mirrors bm25's
// NextTailChunkIdSql.
func NextTailChunkIdSql(cfg TableConfig) string {
	tailMax := fmt.Sprintf("COALESCE((SELECT MAX(%s) FROM %s WHERE %s = %s AND %s = %d), 0)",
		catalog.FullText2Index_TblCol_Storage_Chunk_Id, sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	baseMax := fmt.Sprintf("COALESCE((SELECT MAX(%s) FROM %s), 0)",
		catalog.FullText2Index_TblCol_Metadata_Recency, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	return fmt.Sprintf("SELECT GREATEST(%s, %s) + 1", tailMax, baseMax)
}

// NextTailChunkId runs NextTailChunkIdSql — the next append position for the CDC
// sinker.
func NextTailChunkId(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (int64, error) {
	res, err := runSql(sqlproc, NextTailChunkIdSql(cfg))
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
	}
	return 0, nil
}

// StaleGenSqls returns the two queries whose results form the cache-freshness generation
// used by Fulltext2Search.IsStale: MAX(metadata.timestamp) (bumped by a REBUILD/MERGE that
// writes a new base model row) and MAX(storage.chunk_id) of the tag=1 CdcTail (bumped by a
// CDC append). A change in EITHER means the loaded index is stale. Two reads because
// timestamp and tag live in different tables (metadata has no tag; storage has no
// timestamp). The tail read is scoped to (CdcTailId, tag=1) — the exact CDC delta — so an
// unrelated base sub-index's higher chunk_id can't mask a fresh append.
func StaleGenSqls(cfg TableConfig) (tsSQL, tailSQL string) {
	tsSQL = fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Timestamp,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	tailSQL = fmt.Sprintf("SELECT COALESCE(MAX(%s), -1) FROM %s WHERE %s = %s AND %s = %d",
		catalog.FullText2Index_TblCol_Storage_Chunk_Id,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	return
}

func resultScalarInt64(res executor.Result) int64 {
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
	}
	return 0
}

// LoadGeneration reads the current (timestamp, tailChunk) generation using the caller's
// LIVE sqlproc/txn — called at load time so the captured generation reflects exactly the
// txn snapshot the cached index was built from.
func LoadGeneration(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (ts int64, tail int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts, tail, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("LoadGeneration recovered: %v", r))
		}
	}()
	tsSQL, tailSQL := StaleGenSqls(cfg)
	res, err := runSql(sqlproc, tsSQL)
	if err != nil {
		return 0, 0, err
	}
	ts = resultScalarInt64(res)
	res.Close()
	res, err = runSql(sqlproc, tailSQL)
	if err != nil {
		return 0, 0, err
	}
	tail = resultScalarInt64(res)
	res.Close()
	return ts, tail, nil
}

// QueryGeneration reads the current (timestamp, tailChunk) generation in the BACKGROUND
// (housekeeping goroutine, no live sqlproc) via an executor-managed auto-commit txn, keyed
// by the CN UUID + tenant captured at load. Used by IsStale.
func QueryGeneration(ctx context.Context, cnUUID string, accountID uint32, cfg TableConfig) (ts int64, tail int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			ts, tail, err = 0, 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("QueryGeneration recovered: %v", r))
		}
	}()
	tsSQL, tailSQL := StaleGenSqls(cfg)
	res, err := sqlexec.RunSqlAutoCommit(ctx, cnUUID, accountID, cfg.DbName, tsSQL)
	if err != nil {
		return 0, 0, err
	}
	ts = resultScalarInt64(res)
	res.Close()
	res, err = sqlexec.RunSqlAutoCommit(ctx, cnUUID, accountID, cfg.DbName, tailSQL)
	if err != nil {
		return 0, 0, err
	}
	tail = resultScalarInt64(res)
	res.Close()
	return ts, tail, nil
}

// CountTailChunks returns the number of tag=1 CdcTail chunk rows — the idxcron
// tail-growth gate (fold once the delta is large enough).
func CountTailChunks(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = %s AND %s = %d",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	return scanInt64(sqlproc, sql)
}

// SumBaseNrow sums metadata.nrow over the tag=0 bases — the base doc count for the
// dead-doc estimate (compared to the live source row count).
func SumBaseNrow(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (int64, error) {
	sql := fmt.Sprintf("SELECT COALESCE(SUM(%s), 0) FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Nrow, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	return scanInt64(sqlproc, sql)
}

func scanInt64(sqlproc *sqlexec.SqlProcess, sql string) (int64, error) {
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat != nil && bat.RowCount() > 0 {
			return vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0), nil
		}
	}
	return 0, nil
}

// FrameChunkCount is how many MaxChunkSize storage rows a frame of frameLen bytes
// occupies (>= 1) — the streaming sinker advances chunk_id past each spilled
// segment by this without holding the framed bytes. Mirrors bm25's FrameChunkCount.
func FrameChunkCount(frameLen int) int64 {
	n := int64((frameLen + vectorindex.MaxChunkSize - 1) / vectorindex.MaxChunkSize)
	if n < 1 {
		n = 1
	}
	return n
}
