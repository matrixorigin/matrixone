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
	// sub-indexes of at most Capacity docs each (0 => a single unbounded base).
	Capacity   int64 `json:"capacity,omitempty"`
	FromSource bool  `json:"from_source,omitempty"`
}

// SubIndexId is the index_id for the i-th tag=0 base sub-index of a build
// identified by uid (which must carry a per-build-unique component so concurrent
// builds never collide). Load enumerates ids from the metadata table.
func SubIndexId(uid string, i int) string { return fmt.Sprintf("%s:%d", uid, i) }

// maxInsertTuples bounds the (index_id, chunk_id, data, tag) rows per INSERT.
const maxInsertTuples = 100

// ToInsertSqls serializes the segment, spills it to a temp file, and emits the
// SQL to persist it: one metadata row + the bytes split into <= MaxChunkSize
// (index_id, chunk_id, data, tag) rows read via load_file. tag=0 is a compacted
// base (sync CREATE/REINDEX build); tag=1 is a CDC delta. The returned cleanup
// MUST run after the SQLs execute (they read the temp file at execution).
func (s *Segment) ToInsertSqls(cfg TableConfig, ts int64, tag int) (sqls []string, cleanup func(), err error) {
	buf, err := s.Serialize()
	if err != nil {
		return nil, nil, err
	}
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	fp, err := os.CreateTemp("", "ft2build")
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
	sqls = append(sqls, fileChunkInsertSqls(cfg, s.Id, 0, path, int(filesize), tag)...)
	return sqls, cleanup, nil
}

// fileChunkInsertSqls splits a spilled file into <= MaxChunkSize storage rows read
// via load_file (no hex/unhex). Mirrors bm25.wand.FileChunkInsertSqls.
func fileChunkInsertSqls(cfg TableConfig, id string, startChunkId int64, path string, dataLen int, tag int) []string {
	prefix := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s) VALUES ",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, catalog.FullText2Index_TblCol_Storage_Chunk_Id,
		catalog.FullText2Index_TblCol_Storage_Data, catalog.FullText2Index_TblCol_Storage_Tag)
	var sqls, vals []string
	chunkID := startChunkId
	for off := 0; off < dataLen; off += vectorindex.MaxChunkSize {
		sz := vectorindex.MaxChunkSize
		if off+sz > dataLen {
			sz = dataLen - off
		}
		url := fmt.Sprintf("file://%s?offset=%d&size=%d", path, off, sz)
		vals = append(vals, fmt.Sprintf("(%s, %d, load_file(cast(%s as datalink)), %d)",
			sqlquote.String(id), chunkID, sqlquote.String(url), tag))
		chunkID++
		if len(vals) == maxInsertTuples {
			sqls = append(sqls, prefix+strings.Join(vals, ", "))
			vals = vals[:0]
		}
	}
	if len(vals) > 0 {
		sqls = append(sqls, prefix+strings.Join(vals, ", "))
	}
	return sqls
}

// TailFileInsertSqls persists a spilled tag=1 CDC frame file (index_id=CdcTailId).
func TailFileInsertSqls(cfg TableConfig, startChunkId int64, path string, frameLen int) []string {
	return fileChunkInsertSqls(cfg, vectorindex.CdcTailId, startChunkId, path, frameLen, int(vectorindex.Tag_CdcEvents))
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
	res, err := sqlexec.RunSql(sqlproc, sql)
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

// LoadAllBases loads every tag=0 base sub-index listed in the metadata table.
// Returns nil when no base was built (empty-table create → CDC-only index).
func LoadAllBases(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*Segment, error) {
	idSQL := fmt.Sprintf("SELECT %s FROM %s",
		catalog.FullText2Index_TblCol_Metadata_Index_Id, sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable))
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

	bases := make([]*Segment, 0, len(ids))
	for _, id := range ids {
		m, lerr := LoadFromStorage(sqlproc, cfg, id)
		if lerr != nil {
			return nil, lerr
		}
		bases = append(bases, m)
	}
	return bases, nil
}

// LoadFromStorage reads an index id's metadata + chunks, verifies the checksum,
// and deserializes it. Chunks stream by chunk_id offset into a temp file so the
// mpool never holds the whole index.
func LoadFromStorage(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string) (*Segment, error) {
	checksum, filesize, recency, found, err := readMetadata(sqlproc, cfg, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s metadata not found", id))
	}
	if filesize <= 0 {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s has empty filesize", id))
	}

	fp, err := os.CreateTemp("", "ft2idx")
	if err != nil {
		return nil, err
	}
	path := fp.Name()
	defer func() { fp.Close(); os.Remove(path) }()
	if err = fp.Truncate(filesize); err != nil {
		return nil, err
	}
	if err = streamChunksToFile(sqlproc, cfg, id, filesize, fp); err != nil {
		return nil, err
	}
	if got, cerr := vectorindex.CheckSum(path); cerr != nil {
		return nil, cerr
	} else if got != checksum {
		return nil, moerr.NewInternalError(sqlproc.GetContext(), fmt.Sprintf("fulltext2 index %s checksum mismatch", id))
	}
	if _, err = fp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	m, err := Deserialize(id, fp)
	if err != nil {
		return nil, err
	}
	m.Recency = recency
	return m, nil
}

// LoadTailSegments loads the tag=1 CdcTail: the SELECTed chunk rows are ordered,
// reassembled into frames, and each insert frame decoded into a segment (its
// Recency = the frame's first chunk_id). Delete frames are folded into the pk
// tombstone map. Empty tail → (nil, nil, nil).
func LoadTailSegments(sqlproc *sqlexec.SqlProcess, cfg TableConfig) ([]*Segment, map[string]int64, error) {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s AND %s = %d",
		catalog.FullText2Index_TblCol_Storage_Chunk_Id, catalog.FullText2Index_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(vectorindex.CdcTailId),
		catalog.FullText2Index_TblCol_Storage_Tag, int(vectorindex.Tag_CdcEvents))
	res, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return nil, nil, err
	}
	var chunks []TailChunk
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		cids := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
		for i := 0; i < bat.RowCount(); i++ {
			chunks = append(chunks, TailChunk{ChunkId: cids[i], Data: append([]byte(nil), bat.Vecs[1].GetRawBytesAt(i)...)})
		}
	}
	res.Close()
	if len(chunks) == 0 {
		return nil, nil, nil
	}

	ordered, err := orderTailChunks(chunks)
	if err != nil {
		return nil, nil, err
	}
	frames, err := reassembleFrames(ordered)
	if err != nil {
		return nil, nil, err
	}

	var segs []*Segment
	deletes := make(map[string]int64)
	for _, f := range frames {
		records, _, nInserts, nDeletes, _, uerr := cuvscdc.UnframeCdcChunk(f.Data)
		if uerr != nil {
			return nil, nil, uerr
		}
		switch {
		case nInserts > 0:
			seg, derr := Deserialize(fmt.Sprintf("tail-%d", f.ChunkId), bytes.NewReader(records))
			if derr != nil {
				return nil, nil, derr
			}
			seg.Recency = f.ChunkId
			segs = append(segs, seg)
		case nDeletes > 0:
			recs, derr := DecodeDeleteLog(records)
			if derr != nil {
				return nil, nil, derr
			}
			deletes = foldDeleteFrame(deletes, recs, f.ChunkId)
		}
	}
	return segs, deletes, nil
}

// streamChunksToFile streams a tag=0 index's chunk rows, writing each at
// chunk_id*MaxChunkSize into fp; the assembled bytes must fill filesize exactly.
func streamChunksToFile(sqlproc *sqlexec.SqlProcess, cfg TableConfig, id string, filesize int64, fp *os.File) error {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %s",
		catalog.FullText2Index_TblCol_Storage_Chunk_Id, catalog.FullText2Index_TblCol_Storage_Data,
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable),
		catalog.FullText2Index_TblCol_Storage_Index_Id, sqlquote.String(id))
	written, _, err := streamChunkRowsToFile(sqlproc, sql, 0, filesize, fp)
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
							fmt.Sprintf("fulltext2 chunk_id %d out of range [base %d, bound %d]", cid, baseChunk, bound))
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
	res, err := sqlexec.RunSql(sqlproc, NextTailChunkIdSql(cfg))
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
	res, err := sqlexec.RunSql(sqlproc, sql)
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
