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
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const fulltext2AfterTailCommitBeforeFenceFault = "fulltext2_after_tail_commit_before_fence"

func waitAfterFulltext2TailCommitBeforeFence(ctx context.Context, hasDurableSegment bool) {
	if !hasDurableSegment {
		return
	}
	fault.TriggerFaultWithContext(ctx, fulltext2AfterTailCommitBeforeFenceFault)
}

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
	// The json term shape travels with the config so this path and the CREATE
	// build agree on the terms they emit.
	tokenize, err := fulltext2.CdcTokenizerWithJSONOptions(w.cfg.Parser, w.cfg.JSONTermOptions())
	if err != nil {
		errch <- err
		return
	}
	var bopts []fulltext2.BuildOpt
	if w.cfg.PositionFree {
		bopts = append(bopts, fulltext2.WithPositionFree())
	}
	// Route the CDC tail's segment spills onto the LOCAL (SSD) fileservice's __fulltext2
	// dir — the same fast mount the sync build uses — so tail mmap page faults come off
	// the SSD, not /tmp. The CN root FileService is published on the ISCP executor by
	// ISCPTaskExecutorFactory; resolve it by this CN's UUID. spillDir="" (no LOCAL
	// attached / executor not found) falls back to the OS temp dir, unchanged behavior.
	var spillDir string
	if exec, ok := GetExecutorRuntime(c.cnUUID); ok {
		spillDir = fulltext2.LocalSpillDir(ctx, exec.rootFS)
	}
	tb, err := fulltext2.NewTailBuilder(w.pkType, w.capacity, w.postingCap, spillDir, tokenize, bopts...)
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
				var durableGeneration fulltext2.Generation
				err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), time.Hour, nil, nil,
					func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
						startChunk, err := fulltext2.NextTailChunkId(sqlproc, w.cfg)
						if err != nil {
							return err
						}
						// The frames are already on disk (TailBuilder spilled them into packed spool
						// files): INSERT them via load_file, batched at maxInsertTuples rows per
						// statement ACROSS frames — so a burst of tiny frames costs ~totalChunks/maxInsertTuples
						// RunSql round-trips in this one txn, not one INSERT per frame. chunk_ids stay
						// contiguous in frame order, so recency is unchanged.
						sqls, chunkID := fulltext2.TailFramesInsertSqls(w.cfg, startChunk, segs)
						for _, s := range sqls {
							res, e := sqlexec.RunSql(sqlproc, s)
							if e != nil {
								return e
							}
							res.Close()
						}
						// Per-flush-cycle sink summary: Debug, not Info — a continuously-ingesting index
						// flushes often, so this would flood production logs at Info (see the 46 GB
						// stdout incident). Available at Debug when diagnosing CDC ingest.
						logutil.Debugf("[ftv2-sink] db=%s index=%s type=%d events=%d frames=%d chunk_id=%d..%d",
							w.cfg.DbName, w.cfg.IndexTable, datatype, nevents, len(segs), startChunk, chunkID)
						if datatype == ISCPDataType_Tail {
							sqlctx := sqlproc.SqlCtx
							if err := r.UpdateWatermark(sqlproc.GetContext(), sqlctx.GetService(), sqlctx.Txn()); err != nil {
								return err
							}
						}
						if len(segs) > 0 {
							ts, tail, err := fulltext2.LoadGeneration(sqlproc, w.cfg)
							if err == nil {
								durableGeneration = fulltext2.Generation{BaseTimestamp: ts, TailChunk: tail}
							}
							return err
						}
						return nil
					})
				if err != nil {
					errch <- err
					return
				}
				waitAfterFulltext2TailCommitBeforeFence(ctx, len(segs) > 0)
				// The tail transaction is already durable. Install the local generation
				// fence first, then enqueue best-effort cross-CN delivery. Neither action
				// can make this consumer retry and persist the tail a second time.
				if len(segs) > 0 {
					identity := w.cfg.CacheIdentity(r.GetAccountID())
					fulltext2.InstallGenerationFence(identity, durableGeneration)
					if exec, ok := GetExecutorRuntime(c.cnUUID); ok && exec.fulltext2FencePublisher != nil {
						exec.fulltext2FencePublisher.Enqueue(identity, durableGeneration)
					}
					logutil.Debugf("[ftv2-sink] installed cache generation fence for index=%s", w.cfg.IndexTable) // per-flush: Debug, not Info
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
