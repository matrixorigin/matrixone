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

package wand

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// TableConfig is the JSON config passed to the fulltext_wand_create /
// fulltext_wand_search TVFs (const string arg 0). It locates the persistent
// WAND chunk store + metadata table for an index, mirroring
// vectorindex.IndexTableConfig.
type TableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	IndexTable    string `json:"index"`    // chunk store (Bm25Index_TblType_Storage)
	MetadataTable string `json:"metadata"` // metadata (Bm25Index_TblType_Metadata)
	PKey          string `json:"pkey"`
	// Capacity is max_index_capacity, resolved by the compile layer from the index's
	// persisted algo_params (the immutable flat param) and carried to the create-build TVF
	// so the base is split at the same value every compaction later reads. 0 ⇒ unset (the
	// TVF falls back to the resolver, for older indexes without the flat param).
	Capacity int64 `json:"capacity,omitempty"`
	// FromSource selects the create-build TVF's input shape. false (default): the TVF
	// reads pre-tokenized postings rows (argVecs = [cfg, word, doc_id]). true: the TVF
	// reads SOURCE rows (argVecs = [cfg, pk, cols…]) and tokenizes them in-Go — one build
	// statement straight off the source, so the postings table is never populated.
	FromSource bool `json:"from_source,omitempty"`
}

// SubIndexId is the index_id for the i-th tag=0 base sub-index of a build identified by
// uid. All of an index's sub-indexes share the one storage + metadata table and are told
// apart by this id (mirrors HNSW's "<uid>:<n>"). uid MUST carry a per-build-unique
// component (e.g. the build timestamp) — NOT a deterministic table-derived name — so two
// builds (concurrent across CNs, or a rebuild) never write colliding ids. Load
// enumerates the ids from the metadata table, so the exact form only needs uniqueness.
func SubIndexId(uid string, i int) string {
	return fmt.Sprintf("%s:%d", uid, i)
}

// ToInsertSqls serializes the model, SPILLS it to a temp file, and emits the SQL to
// persist it: one metadata row (timestamp, md5 checksum, filesize) plus the index
// bytes split into <= MaxChunkSize (index_id, chunk_id, data, tag) rows read straight
// from the file via load_file — NO hex/unhex (which doubled the SQL text and had to
// be re-parsed). Mirrors HNSW's ToSql. The returned cleanup MUST be called after the
// SQLs run (they read the temp file at execution) — typically deferred by the caller.
//
// tag selects the storage tier (Phase B): tag=0 = the compacted main index
// (the sync CREATE/REINDEX build and idxcron's merged output); tag=1 = an
// incremental CDC delta segment appended by the ISCP sinker. Both kinds coexist
// in the same ft_index store and are distinguished only by this column.
func (m *WandModel) ToInsertSqls(cfg TableConfig, ts int64, tag int) (sqls []string, cleanup func(), err error) {
	buf, err := m.Serialize()
	if err != nil {
		return nil, nil, err
	}
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	fp, err := os.CreateTemp("", "wandbuild")
	if err != nil {
		return nil, nil, err
	}
	path := fp.Name()
	cleanup = func() { fp.Close(); os.Remove(path) }
	if _, err = fp.Write(buf); err != nil {
		cleanup()
		return nil, nil, err
	}
	if err = fp.Sync(); err != nil { // durable on disk before load_file reads it
		cleanup()
		return nil, nil, err
	}

	metaTbl := sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable)
	sqls = append(sqls, fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (%s, %d, %s, %d, %d, %d)",
		metaTbl,
		catalog.Bm25Index_TblCol_Metadata_Index_Id, catalog.Bm25Index_TblCol_Metadata_Timestamp,
		catalog.Bm25Index_TblCol_Metadata_Checksum, catalog.Bm25Index_TblCol_Metadata_Filesize,
		catalog.Bm25Index_TblCol_Metadata_Recency, catalog.Bm25Index_TblCol_Metadata_Nrow,
		sqlquote.String(m.Id), ts, sqlquote.String(checksum), filesize, m.Recency, m.N))
	sqls = append(sqls, FileChunkInsertSqls(cfg, m.Id, 0, path, int(filesize), tag)...)
	return sqls, cleanup, nil
}

// DeleteSqls returns the SQL to remove an index id's chunks + metadata row
// (used before a rebuild so reindex is idempotent).
func DeleteSqls(cfg TableConfig, id string) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.Bm25Index_TblCol_Storage_Index_Id, sqlquote.String(id)),
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
			catalog.Bm25Index_TblCol_Metadata_Index_Id, sqlquote.String(id)),
	}
}

// readMetadata fetches an index id's tag=0 metadata (checksum + filesize).
// found=false means no metadata row exists (a tag=0 base was never built — e.g.
// an index created on an empty table, whose corpus is entirely tag=1 CDC deltas).
func readMetadata(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (checksum string, filesize int64, chunkId int64, found bool, err error) {
	metaSQL := fmt.Sprintf("SELECT %s, %s, %s FROM %s WHERE %s = %s",
		catalog.Bm25Index_TblCol_Metadata_Checksum, catalog.Bm25Index_TblCol_Metadata_Filesize,
		catalog.Bm25Index_TblCol_Metadata_Recency,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
		catalog.Bm25Index_TblCol_Metadata_Index_Id, sqlquote.String(id))
	mres, err := sqlexec.RunSql(sqlproc, metaSQL)
	if err != nil {
		return "", 0, 0, false, err
	}
	for _, bat := range mres.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		checksum = bat.Vecs[0].GetStringAt(0)
		filesize = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
		chunkId = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[2], 0)
		found = true
		break
	}
	mres.Close()
	return checksum, filesize, chunkId, found, nil
}

// DeleteAllBasesSqls removes every tag=0 base sub-index — all tag=0 chunk rows (of all
// sub-index ids) from the storage table plus all metadata rows — so the CREATE build is
// idempotent when several sub-indexes exist. The tag=1 CdcTail is untouched.
func DeleteAllBasesSqls(cfg TableConfig) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %d", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.Bm25Index_TblCol_Storage_Tag, int(vectorindex.Tag_ModelChunk)),
		// WHERE TRUE, not a bare DELETE: a bare DELETE takes MO's truncate fast-path
		// (DROP + RECREATE the metadata hidden table object); WHERE TRUE keeps the object.
		fmt.Sprintf("DELETE FROM %s WHERE TRUE", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable)),
	}
}

// DeleteTailSqls removes the entire tag=1 CdcTail (every cdc_tail chunk). Used by a
// REINDEX rebuild, which discards the accumulated delta log and rebuilds tag=0 from
// scratch; the fresh CDC task then starts from the reindex point (startFromNow). The
// tag=0 bases are cleared separately (DeleteAllBasesSqls / the create TVF).
func DeleteTailSqls(cfg TableConfig) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %d", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.Bm25Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents)),
	}
}

// DeleteTailChunksByMaxId removes only the tag=1 CdcTail chunk rows with chunk_id
// <= k — the prefix a merge-compaction folded into the new tag=0 base. Chunks with
// chunk_id > k (appended by the sinker after the compaction's txn snapshot) are
// preserved, so `chunk_id` is never renumbered and concurrent appends survive.
func DeleteTailChunksByMaxId(cfg TableConfig, k int64) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND %s = %d AND %s <= %d",
			sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.Bm25Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
			catalog.Bm25Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents),
			catalog.Bm25Index_TblCol_Storage_Chunk_Id, k),
	}
}

// CountTailChunks returns the number of tag=1 CdcTail chunk rows — a cheap,
// monotonic proxy for accumulated tail size used by the idxcron reindex gate.
// Chunk count, NOT doc count: an oversized frame is split across several chunk
// rows (see Bug 1 / splitFrameChunks), so per-chunk UnframeCdcChunk cannot run on
// continuation chunks; counting rows is robust and sufficient to gate "how much
// tail has piled up since the last reindex".
func CountTailChunks(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = %d",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.Bm25Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	res, err := sqlexec.RunSql(sqlproc, sql)
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

// SumBaseNrow returns SUM(metadata.nrow) — docs physically present in the tag=0 base subs
// (live + deleted-but-not-yet-reclaimed). A cheap metadata aggregate (no postings loaded);
// idxcron compares it to the source table's live row count to estimate the dead-doc fraction
// and decide MERGE (incremental) vs REBUILD (full reclaim).
func SumBaseNrow(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (int64, error) {
	sql := fmt.Sprintf("SELECT COALESCE(SUM(%s), 0) FROM %s",
		catalog.Bm25Index_TblCol_Metadata_Nrow, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	res, err := sqlexec.RunSql(sqlproc, sql)
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

// LoadAllBases loads every tag=0 base sub-index listed in the metadata table. The
// metadata table is per-fulltext-index, so every row names one of this index's bases
// (mirrors HNSW's LoadMetadata). Returns nil when no base was built (empty-table create
// → CDC-only index). Bases are pk-disjoint, so the caller assigns them one shared
// baseRecency. On any error the partially-loaded bases are freed.
func LoadAllBases(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*WandModel, error) {
	idSQL := fmt.Sprintf("SELECT %s FROM %s",
		catalog.Bm25Index_TblCol_Metadata_Index_Id,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
	res, err := sqlexec.RunSql(sqlproc, idSQL)
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

	bases := make([]*WandModel, 0, len(ids))
	for _, id := range ids {
		m, lerr := LoadFromStorage(sqlproc, cfg, id)
		if lerr != nil {
			freeSegs(bases)
			return nil, lerr
		}
		bases = append(bases, m)
	}
	return bases, nil
}

// LoadFromStorage reads an index's metadata + chunks back from the WAND store,
// verifies the checksum, and deserializes it into a model. Chunks are
// downloaded with STREAMING SQL and written by chunk_id offset into a temp file
// (so the mpool only ever holds a chunk or two, never the whole index — mirrors
// HNSW's loadChunk). The model is then deserialized straight from that file,
// with the large postings going off the Go heap (C allocator). The temp file is
// removed before returning. Errors if the tag=0 metadata is absent (callers that
// tolerate a missing base use LoadBaseOptional).
func LoadFromStorage(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (*WandModel, error) {
	checksum, filesize, chunkId, found, err := readMetadata(sqlproc, cfg, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("wand index %s metadata not found", id))
	}
	if filesize <= 0 {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("wand index %s has empty filesize", id))
	}

	// temp file sized to filesize; chunks are written by offset (possibly out of
	// order), so a sparse file via Truncate + WriteAt.
	fp, err := os.CreateTemp("", "wandidx")
	if err != nil {
		return nil, err
	}
	path := fp.Name()
	defer func() {
		fp.Close()
		os.Remove(path)
	}()
	if err = fp.Truncate(filesize); err != nil {
		return nil, err
	}

	if err = streamChunksToFile(sqlproc, cfg, id, filesize, fp); err != nil {
		return nil, err
	}

	// verify md5 over the assembled file
	if got, cerr := vectorindex.CheckSum(path); cerr != nil {
		return nil, cerr
	} else if got != checksum {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("wand index %s checksum mismatch", id))
	}

	if _, err = fp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	m, err := Deserialize(id, fp)
	if err != nil {
		return nil, err
	}
	// The base's recency key comes from SQL (metadata.chunk_id): 0 = oldest
	// full-build base; K = the folded tail chunk_id for a compacted base. Query
	// ComputeLiveness dedups bases + tail uniformly by this.
	m.Recency = chunkId
	return m, nil
}

// loadTailSegments streams the tag=1 CdcTail (index_id = CdcTailId) into a temp
// file — each chunk row placed at (chunk_id - min)*MaxChunkSize, bounded memory,
// exactly like the tag=0 loader (streamChunksToFile) — and decodes it frame-by-frame
// (assembleFramesAt) into the ordered segment list + folded delete map. Empty tail
// (no CDC yet) → (nil, nil).
//
// Two things it deliberately avoids: (1) NO `ORDER BY chunk_id` — a SQL sort would
// add a Sort operator (full materialization / possible spill); placement-by-offset
// orders the chunks instead. (2) NO buffering the whole delta — chunks stream to
// disk one batch at a time and frames are decoded one at a time, so the transient
// footprint is a single frame, not the tail.
func loadTailSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*WandModel, map[any]int64, int32, error) {
	minC, maxC, empty, err := tailChunkBounds(sqlproc, cfg)
	if err != nil {
		return nil, nil, 0, err
	}
	if empty {
		return nil, nil, 0, nil
	}
	span := maxC - minC + 1
	filesize := span * int64(vectorindex.MaxChunkSize)

	fp, err := os.CreateTemp("", "wandtail")
	if err != nil {
		return nil, nil, 0, err
	}
	path := fp.Name()
	defer func() {
		fp.Close()
		os.Remove(path)
	}()
	if err = fp.Truncate(filesize); err != nil {
		return nil, nil, 0, err
	}

	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s AND %s = %d",
		catalog.Bm25Index_TblCol_Storage_Chunk_Id, catalog.Bm25Index_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.Bm25Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.Bm25Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	_, n, err := streamChunkRowsToFile(sqlproc, sql, minC, filesize, fp)
	if err != nil {
		return nil, nil, 0, err
	}
	// tag=1 chunk_ids are a gapless run, so [min..max] must span exactly the row
	// count; a mismatch is a missing/duplicate chunk (corruption).
	if n != span {
		return nil, nil, 0, moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("wand tail: chunk_id range [%d..%d] spans %d but got %d rows (gap or duplicate)", minC, maxC, span, n))
	}
	return assembleFramesAt(fp, minC, span)
}

// tailChunkBounds returns MIN/MAX chunk_id of the tag=1 CdcTail via aggregates (no
// sort). empty=true when there are no tag=1 rows yet (MIN/MAX → NULL).
func tailChunkBounds(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (minC, maxC int64, empty bool, err error) {
	sql := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WHERE %s = %s AND %s = %d",
		catalog.Bm25Index_TblCol_Storage_Chunk_Id, catalog.Bm25Index_TblCol_Storage_Chunk_Id,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.Bm25Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.Bm25Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	res, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return 0, 0, false, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		if bat.Vecs[0].IsNull(0) { // MIN over no rows → NULL → empty tail
			return 0, 0, true, nil
		}
		minC = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], 0)
		maxC = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
		return minC, maxC, false, nil
	}
	return 0, 0, true, nil
}

// streamChunksToFile streams a tag=0 index's chunk rows and writes each at
// chunk_id*MaxChunkSize into fp; the assembled bytes must fill filesize exactly.
func streamChunksToFile(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string, filesize int64, fp *os.File) error {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.Bm25Index_TblCol_Storage_Chunk_Id, catalog.Bm25Index_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.Bm25Index_TblCol_Storage_Index_Id, sqlquote.String(id))
	written, _, err := streamChunkRowsToFile(sqlproc, sql, 0, filesize, fp)
	if err != nil {
		return err
	}
	if written != filesize {
		return moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("wand index %s incomplete: wrote %d of %d bytes", id, written, filesize))
	}
	return nil
}

// streamChunkRowsToFile streams the (chunk_id, data) rows returned by sql and writes
// each at (chunk_id - baseChunk)*MaxChunkSize into fp, bounding the mpool to the
// stream buffer (never the whole index). bound is the file extent for the range
// check. Returns bytes written and the chunk-row count (callers use whichever fits
// their completeness check: tag=0 wants written == filesize; tag=1 — which has
// partial-tail holes — wants count == span).
func streamChunkRowsToFile(sqlproc *sqlexec.SqlProcess, sql string, baseChunk, bound int64, fp *os.File) (written, nchunks int64, err error) {
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
		if _, e := sqlexec.RunStreamingSql(ctx, sqlproc, sql, streamCh, errorCh); e != nil {
			errorCh <- e
		}
	}()

	var loopErr error
	closed := false
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
							fmt.Sprintf("wand chunk_id %d out of range [base %d, bound %d]", cid, baseChunk, bound))
						break
					}
					if _, e := fp.WriteAt(data, off); e != nil {
						loopErr = e
						break
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
	// drain any remaining results so the producer can exit cleanly
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
