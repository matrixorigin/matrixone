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
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// RunFulltext2 is the ISCP consumer loop for the fulltext2 positional index — the
// direct analogue of RunWand. It STREAMS each flush's CDC blob into a fulltext2
// TailBuilder (tokenizing insert rows per the index parser into capacity-capped
// segments, spilling each sealed segment to a temp file as it fills), so peak
// memory is one open segment, not the whole stream. On channel close it appends
// the spilled segments (+ one delete batch, delete-first) as tag=1 CdcTail frames
// at the next chunk_id, in one txn, advancing the watermark.
//
// CDC/txn-coupled: not exercised by package unit tests (needs a live mo_ctl + CDC
// pipeline); the engine primitives it calls (Cdc, TailBuilder, NextTailChunkId,
// TailFileInsertSqls) are unit-tested in pkg/fulltext2.
func RunFulltext2(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	w, ok := c.sqlWriter.(*Fulltext2SqlWriter)
	if !ok {
		errch <- moerr.NewInternalError(ctx, "fulltext2 iscp Run: unexpected writer type")
		return
	}

	// Parser-aware tokenize (ngram/gojieba/json) so build and query tokens match.
	tokenize, err := fulltext2.CdcTokenizer(w.cfg.Parser)
	if err != nil {
		errch <- err
		return
	}
	var bopts []fulltext2.BuildOpt
	if w.cfg.PositionFree {
		bopts = append(bopts, fulltext2.WithPositionFree())
	}
	// The sync build spills its segments onto the LOCAL (SSD) fileservice's __fulltext2
	// dir, but the ISCP consumer context has no LOCAL fileservice handle to do the same
	// for the CDC tail: the engine exposes only SHARED, the persist-txn sqlproc carries
	// no FileService, and the ctx has no ParameterUnit. So the tail spills to the OS temp
	// dir (spillDir=""). NewTailBuilder already takes a spillDir; routing the tail to
	// __fulltext2 is a follow-up that threads the container FileService through the ISCP
	// framework (ISCPTaskExecutorFactory -> Worker -> IndexConsumer).
	tb, err := fulltext2.NewTailBuilder(w.pkType, w.capacity, w.postingCap, "", tokenize, bopts...)
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
				segs, ferr := tb.Finish()
				if ferr != nil {
					errch <- ferr
					return
				}
				err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), time.Hour, nil, nil,
					func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
						startChunk, err := fulltext2.NextTailChunkId(sqlproc, w.cfg)
						if err != nil {
							return err
						}
						chunkID := startChunk
						for _, seg := range segs {
							// The frame is already on disk (TailBuilder spilled it): INSERT it
							// via load_file straight from the file, split across MaxChunkSize rows.
							for _, s := range fulltext2.TailFileInsertSqls(w.cfg, chunkID, seg.Path, seg.FrameLen) {
								res, e := sqlexec.RunSql(sqlproc, s)
								if e != nil {
									return e
								}
								res.Close()
							}
							chunkID += fulltext2.FrameChunkCount(seg.FrameLen)
						}
						logutil.Infof("[ftv2-sink] db=%s index=%s type=%d events=%d frames=%d chunk_id=%d..%d",
							w.cfg.DbName, w.cfg.IndexTable, datatype, nevents, len(segs), startChunk, chunkID)
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
				// Evict the cached search index so the next query reloads tag=0 + the
				// freshly-appended tag=1 frames, instead of serving the warm (stale)
				// cache until its idle TTL. Only when frames were actually written.
				// Local to this CN's cache.
				if len(segs) > 0 {
					veccache.Cache.Remove(w.cfg.IndexTable)
					logutil.Infof("[ftv2-sink] evicted search cache for index=%s", w.cfg.IndexTable)
				}
				return
			}

			cdc, derr := fulltext2.DecodeCdc(blob)
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
