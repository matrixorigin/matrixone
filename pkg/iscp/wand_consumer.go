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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// RunWand is the ISCP consumer loop for the WAND "retrieval" index. Like
// RunHnsw it is model-building (not the generic SQL RunIndex), but leaner: it
// never loads the full index. It accumulates every flush's CDC blob, then on
// channel close builds ONE delta from the batch — tokenized INSERT/UPSERT rows
// into capacity-split segments + a delete batch — and appends them as tag=1
// CdcTail frames at the next chunk_id, in one txn, advancing the watermark.
//
// NOTE: this consumer is CDC/txn-coupled and is NOT exercised by the package
// unit tests; it needs a live mo_ctl + CDC pipeline to validate end-to-end. The
// WAND-specific build/frame logic it calls (BuildTailFrames, FrameInsertSql,
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

	acc := wand.NewWandCdc(w.pkType)
	datatype := r.GetDataType()

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-errch:
			errch <- e
			return
		case blob, ok := <-c.sqlBufSendCh:
			if !ok {
				// channel closed: build the delta + append tag=1 frames in one txn.
				nframes := 0
				err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), time.Hour, nil, nil,
					func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
						startChunk, err := wandNextTailChunkId(sqlproc, w.cfg)
						if err != nil {
							return err
						}
						frames, nextChunk, err := wand.BuildTailFrames(acc, w.capacity, startChunk, tokenize)
						if err != nil {
							return err
						}
						for _, f := range frames {
							// a large segment frame is split across several
							// MaxChunkSize storage rows (data column cap).
							for _, s := range wand.FrameInsertSqls(w.cfg, f.ChunkId, f.Data) {
								res, e := sqlexec.RunSql(sqlproc, s)
								if e != nil {
									return e
								}
								res.Close()
							}
						}
						nframes = len(frames)
						logutil.Infof("[wand-sink] db=%s index=%s type=%d events=%d frames=%d chunk_id=%d..%d",
							w.cfg.DbName, w.cfg.IndexTable, datatype, len(acc.Events), nframes, startChunk, nextChunk)
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
				if nframes > 0 {
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
			acc.Events = append(acc.Events, cdc.Events...)
		}
	}
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
