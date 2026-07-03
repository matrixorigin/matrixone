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
	IndexTable    string `json:"index"`    // chunk store (FullTextIndex_TblType_Storage)
	MetadataTable string `json:"metadata"` // metadata (FullTextIndex_TblType_Metadata)
	PKey          string `json:"pkey"`
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
	sqls = append(sqls, fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (%s, %d, %s, %d)",
		metaTbl,
		catalog.FullTextIndex_TblCol_Metadata_Index_Id, catalog.FullTextIndex_TblCol_Metadata_Timestamp,
		catalog.FullTextIndex_TblCol_Metadata_Checksum, catalog.FullTextIndex_TblCol_Metadata_Filesize,
		sqlquote.String(m.Id), ts, sqlquote.String(checksum), filesize))
	sqls = append(sqls, FileChunkInsertSqls(cfg, m.Id, 0, path, int(filesize), tag)...)
	return sqls, cleanup, nil
}

// DeleteSqls returns the SQL to remove an index id's chunks + metadata row
// (used before a rebuild so reindex is idempotent).
func DeleteSqls(cfg TableConfig, id string) []string {
	return []string{
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
			catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(id)),
		fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
			catalog.FullTextIndex_TblCol_Metadata_Index_Id, sqlquote.String(id)),
	}
}

// readMetadata fetches an index id's tag=0 metadata (checksum + filesize).
// found=false means no metadata row exists (a tag=0 base was never built — e.g.
// an index created on an empty table, whose corpus is entirely tag=1 CDC deltas).
func readMetadata(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (checksum string, filesize int64, found bool, err error) {
	metaSQL := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullTextIndex_TblCol_Metadata_Checksum, catalog.FullTextIndex_TblCol_Metadata_Filesize,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
		catalog.FullTextIndex_TblCol_Metadata_Index_Id, sqlquote.String(id))
	mres, err := sqlexec.RunSql(sqlproc, metaSQL)
	if err != nil {
		return "", 0, false, err
	}
	for _, bat := range mres.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		checksum = bat.Vecs[0].GetStringAt(0)
		filesize = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], 0)
		found = true
		break
	}
	mres.Close()
	return checksum, filesize, found, nil
}

// LoadBaseOptional loads the tag=0 compacted-main segment if one exists, returning
// (nil, nil) when no tag=0 base was ever built (empty-table create → CDC-only
// index). Used by the search load path, which composes the base (if any) with the
// tag=1 CdcTail delta frames.
func LoadBaseOptional(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (*WandModel, error) {
	_, _, found, err := readMetadata(sqlproc, cfg, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return LoadFromStorage(sqlproc, cfg, id)
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
	checksum, filesize, found, err := readMetadata(sqlproc, cfg, id)
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
	return Deserialize(id, fp)
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
func loadTailSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*WandModel, map[any]int64, error) {
	minC, maxC, empty, err := tailChunkBounds(sqlproc, cfg)
	if err != nil {
		return nil, nil, err
	}
	if empty {
		return nil, nil, nil
	}
	span := maxC - minC + 1
	filesize := span * int64(vectorindex.MaxChunkSize)

	fp, err := os.CreateTemp("", "wandtail")
	if err != nil {
		return nil, nil, err
	}
	path := fp.Name()
	defer func() {
		fp.Close()
		os.Remove(path)
	}()
	if err = fp.Truncate(filesize); err != nil {
		return nil, nil, err
	}

	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s AND %s = %d",
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, catalog.FullTextIndex_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullTextIndex_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	_, n, err := streamChunkRowsToFile(sqlproc, sql, minC, filesize, fp)
	if err != nil {
		return nil, nil, err
	}
	// tag=1 chunk_ids are a gapless run, so [min..max] must span exactly the row
	// count; a mismatch is a missing/duplicate chunk (corruption).
	if n != span {
		return nil, nil, moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("wand tail: chunk_id range [%d..%d] spans %d but got %d rows (gap or duplicate)", minC, maxC, span, n))
	}
	return assembleFramesAt(fp, minC, span)
}

// tailChunkBounds returns MIN/MAX chunk_id of the tag=1 CdcTail via aggregates (no
// sort). empty=true when there are no tag=1 rows yet (MIN/MAX → NULL).
func tailChunkBounds(sqlproc *sqlexec.SqlProcess, cfg TableConfig) (minC, maxC int64, empty bool, err error) {
	sql := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WHERE %s = %s AND %s = %d",
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, catalog.FullTextIndex_TblCol_Storage_Chunk_Id,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullTextIndex_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
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
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, catalog.FullTextIndex_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(id))
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
