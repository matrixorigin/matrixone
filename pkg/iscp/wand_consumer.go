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

package iscp

import (
	"context"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// RunWand is the ISCP consumer loop for the WAND "retrieval" index. Like
// RunHnsw it is model-building (not the generic SQL RunIndex), but leaner: it
// never loads the full index. It STREAMS each flush's CDC blob straight into a
// TailBuilder — tokenizing insert rows into capacity-capped segments and spilling
// each sealed segment to a temp file the moment it fills — so peak memory is one
// open segment, not the whole stream. On channel close it appends the spilled
// segments (+ one delete batch) as tag=1 CdcTail frames at the next chunk_id, in
// one txn, advancing the watermark.
//
// Why streaming: the old path buffered every event (acc.Events) before building,
// so a large initial sync (e.g. 88M rows) OOM'd holding all (pk, text) in RAM.
// This mirrors hnsw's HnswSync (Update rolls/unloads full models to files;
// Save persists), bounding memory to ~one max_index_capacity segment.
//
// NOTE: this consumer is CDC/txn-coupled and is NOT exercised by the package
// unit tests; it needs a live mo_ctl + CDC pipeline to validate end-to-end. The
// WAND-specific build/frame logic it calls (TailBuilder, FrameInsertSqls,
// NextTailChunkIdSql) is unit-tested in pkg/fulltext/wand.
func RunWand(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	w, ok := c.sqlWriter.(*WandSqlWriter)
	if !ok {
		errch <- moerr.NewInternalError(ctx, "wand iscp Run: unexpected writer type")
		return
	}

	tok, err := tokenizer.SharedJiebaTokenizer(false)
	if err != nil {
		errch <- err
		return
	}
	// Same jieba path the search side uses (parsePatternInNLModeJieba), so build
	// and query tokens match.
	tokenize := func(text string) []string {
		var words []string
		for t, e := range tok.Tokenize([]byte(text)) {
			if e != nil {
				break
			}
			slen := t.TokenBytes[0]
			words = append(words, string(t.TokenBytes[1:slen+1]))
		}
		return words
	}

	// w.capacity was resolved at writer construction (flat algo-param > captured
	// fulltext_max_index_capacity session var > default), so no live resolve here.
	tb, err := wand.NewTailBuilder(w.pkType, w.capacity, tokenize)
	if err != nil {
		errch <- err
		return
	}
	defer tb.Cleanup()

	datatype := r.GetDataType()
	nevents := 0

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-errch:
			errch <- e
			return
		case blob, ok := <-c.sqlBufSendCh:
			if !ok {
				// channel closed: seal the final segment and persist all spilled
				// segments + the delete batch as tag=1 frames in one txn.
				segs, deletes, ferr := tb.Finish()
				if ferr != nil {
					errch <- ferr
					return
				}
				changed := false
				err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), time.Hour, nil, nil,
					func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
						startChunk, err := wandNextTailChunkId(sqlproc, w.cfg)
						if err != nil {
							return err
						}
						chunkID := startChunk
						// Delete frame FIRST (lowest chunk_id) so a same-stream UPSERT's
						// new segment (higher chunk_id) supersedes the deleted base copy
						// under ComputeLiveness (kills only chunk_id strictly below it).
						if len(deletes) > 0 {
							frame, e := wand.FrameDeletes(w.pkType, deletes)
							if e != nil {
								return e
							}
							if e := runFrameInserts(sqlproc, w.cfg, chunkID, frame); e != nil {
								return e
							}
							chunkID += wand.FrameChunkCount(len(frame))
						}
						for _, seg := range segs {
							// Read the spilled framed bytes back one segment at a time
							// (bounded memory) and split across MaxChunkSize rows.
							framed, e := os.ReadFile(seg.Path)
							if e != nil {
								return e
							}
							if e := runFrameInserts(sqlproc, w.cfg, chunkID, framed); e != nil {
								return e
							}
							chunkID += wand.FrameChunkCount(len(framed))
						}
						changed = len(segs) > 0 || len(deletes) > 0
						logutil.Infof("[wand-sink] db=%s index=%s type=%d events=%d segs=%d deletes=%d chunk_id=%d..%d",
							w.cfg.DbName, w.cfg.IndexTable, datatype, nevents, len(segs), len(deletes), startChunk, chunkID)
						// advance the CDC watermark only on the tail stream.
						if datatype == ISCPDataType_Tail {
							sqlctx := sqlproc.SqlCtx
							return r.UpdateWatermark(sqlproc.GetContext(), sqlctx.GetService(), sqlctx.Txn())
						}
						return nil
					})
				if err != nil {
					errch <- err
					return
				}
				// Evict the cached search index so the next query reloads tag=0 +
				// the freshly-appended tag=1 frames, instead of serving the warm
				// (stale) cache until its idle TTL. Local to this CN's cache.
				if changed {
					veccache.Cache.Remove(w.cfg.IndexTable)
					logutil.Infof("[wand-sink] evicted search cache for index=%s", w.cfg.IndexTable)
				}
				return
			}

			cdc, derr := wand.DecodeWandCdc(blob)
			if derr != nil {
				errch <- derr
				return
			}
			nevents += len(cdc.Events)
			if aerr := tb.AddBatch(cdc); aerr != nil {
				errch <- aerr
				return
			}
		}
	}
}

// runFrameInserts splits one framed tag=1 blob across MaxChunkSize storage rows
// from startChunkId and runs the INSERTs.
func runFrameInserts(sqlproc *sqlexec.SqlProcess, cfg wand.TableConfig, startChunkId int64, framed []byte) error {
	for _, s := range wand.FrameInsertSqls(cfg, startChunkId, framed) {
		res, e := sqlexec.RunSql(sqlproc, s)
		if e != nil {
			return e
		}
		res.Close()
	}
	return nil
}

// wandNextTailChunkId runs the COALESCE(MAX(chunk_id)+1,0) query for the tag=1
// CdcTail and returns the next append position.
func wandNextTailChunkId(sqlproc *sqlexec.SqlProcess, cfg wand.TableConfig) (int64, error) {
	res, err := sqlexec.RunSql(sqlproc, wand.NextTailChunkIdSql(cfg))
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
