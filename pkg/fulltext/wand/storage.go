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
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
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

// insertBatchRows caps how many chunk tuples go into one INSERT statement.
const insertBatchRows = 256

// ToInsertSqls serializes the model and emits the SQL to persist it: one
// metadata row (timestamp, md5 checksum, filesize) plus the index bytes split
// into <= MaxChunkSize chunks stored as (index_id, chunk_id, data, tag) rows.
// Chunk data is embedded as unhex(...) literals so the create TVF needs no
// temp files.
//
// tag selects the storage tier (Phase B): tag=0 = the compacted main index
// (the sync CREATE/REINDEX build and idxcron's merged output); tag=1 = an
// incremental CDC delta segment appended by the ISCP sinker. Both kinds coexist
// in the same ft_index store and are distinguished only by this column.
func (m *WandModel) ToInsertSqls(cfg TableConfig, ts int64, tag int) ([]string, error) {
	buf, err := m.Serialize()
	if err != nil {
		return nil, err
	}
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	sqls := make([]string, 0, 4)

	metaTbl := sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable)
	sqls = append(sqls, fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES (%s, %d, %s, %d)",
		metaTbl,
		catalog.FullTextIndex_TblCol_Metadata_Index_Id, catalog.FullTextIndex_TblCol_Metadata_Timestamp,
		catalog.FullTextIndex_TblCol_Metadata_Checksum, catalog.FullTextIndex_TblCol_Metadata_Filesize,
		sqlquote.String(m.Id), ts, sqlquote.String(checksum), filesize))

	idxTbl := sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable)
	insertPrefix := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES ", idxTbl,
		catalog.FullTextIndex_TblCol_Storage_Index_Id, catalog.FullTextIndex_TblCol_Storage_Chunk_Id,
		catalog.FullTextIndex_TblCol_Storage_Data, catalog.FullTextIndex_TblCol_Storage_Tag)

	values := make([]string, 0, insertBatchRows)
	chunkID := int64(0)
	for offset := 0; offset < len(buf); offset += vectorindex.MaxChunkSize {
		end := offset + vectorindex.MaxChunkSize
		if end > len(buf) {
			end = len(buf)
		}
		values = append(values, fmt.Sprintf("(%s, %d, unhex('%s'), %d)",
			sqlquote.String(m.Id), chunkID, hex.EncodeToString(buf[offset:end]), tag))
		chunkID++
		if len(values) == insertBatchRows {
			sqls = append(sqls, insertPrefix+strings.Join(values, ", "))
			values = values[:0]
		}
	}
	if len(values) > 0 {
		sqls = append(sqls, insertPrefix+strings.Join(values, ", "))
	}
	return sqls, nil
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

// LoadFromStorage reads an index's metadata + chunks back from the WAND store,
// verifies the checksum, and deserializes it into a model. Chunks are
// downloaded with STREAMING SQL and written by chunk_id offset into a temp file
// (so the mpool only ever holds a chunk or two, never the whole index — mirrors
// HNSW's loadChunk). The model is then deserialized straight from that file,
// with the large postings going off the Go heap (C allocator). The temp file is
// removed before returning.
func LoadFromStorage(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (*WandModel, error) {
	// metadata: checksum + filesize
	metaSQL := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullTextIndex_TblCol_Metadata_Checksum, catalog.FullTextIndex_TblCol_Metadata_Filesize,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable),
		catalog.FullTextIndex_TblCol_Metadata_Index_Id, sqlquote.String(id))
	mres, err := sqlexec.RunSql(sqlproc, metaSQL)
	if err != nil {
		return nil, err
	}
	var checksum string
	var filesize int64
	found := false
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

// loadTailFrames reads the tag=1 CdcTail frames (index_id = CdcTailId) — each row
// one complete FrameCdcChunk (an insert segment or a delete batch) — ordered by
// chunk_id, the append order that drives liveness. Empty (no CDC yet) → nil.
// Frame bytes are copied out (the executor reuses the batch memory).
func loadTailFrames(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]TailFrame, error) {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s AND %s = %d ORDER BY %s ASC",
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, catalog.FullTextIndex_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullTextIndex_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents),
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id)
	res, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	// Read the raw chunk rows (ORDER BY chunk_id) and reassemble frames: a large
	// segment frame is split across several MaxChunkSize rows.
	var chunks []TailChunk
	for _, bat := range res.Batches {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		cids := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
		for i, cid := range cids {
			data := bat.Vecs[1].GetRawBytesAt(i)
			chunks = append(chunks, TailChunk{ChunkId: cid, Data: append([]byte(nil), data...)})
		}
	}
	return reassembleFrames(chunks)
}

// streamChunksToFile streams the index's chunk rows from the store and writes
// each at chunk_id*MaxChunkSize into fp, bounding mpool to the stream buffer.
func streamChunksToFile(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string, filesize int64, fp *os.File) error {
	chunkSQL := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullTextIndex_TblCol_Storage_Chunk_Id, catalog.FullTextIndex_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullTextIndex_TblCol_Storage_Index_Id, sqlquote.String(id))

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
		if _, e := sqlexec.RunStreamingSql(ctx, sqlproc, chunkSQL, streamCh, errorCh); e != nil {
			errorCh <- e
		}
	}()

	var loopErr error
	var written int64
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
					off := cid * int64(vectorindex.MaxChunkSize)
					if off < 0 || off+int64(len(data)) > filesize {
						loopErr = moerr.NewInternalError(sqlproc.GetContext(),
							fmt.Sprintf("wand index %s chunk %d out of range", id, cid))
						break
					}
					if _, e := fp.WriteAt(data, off); e != nil {
						loopErr = e
						break
					}
					written += int64(len(data))
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
		return loopErr
	}
	if written != filesize {
		return moerr.NewInternalError(sqlproc.GetContext(),
			fmt.Sprintf("wand index %s incomplete: wrote %d of %d bytes", id, written, filesize))
	}
	return nil
}
