// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	countstar_sql = "SELECT COUNT(*) from %s where word = '%s'"
	// BM25 consumes AvgDocLen as float64. Keep the internal query on the
	// floating-point AVG path because exact AVG over integer pos now returns a
	// DECIMAL vector.
	countstar_avg_sql = "SELECT COUNT(*), AVG(CAST(pos AS DOUBLE)) from (SELECT doc_id, MAX(pos) AS pos from %s where word = '%s' GROUP BY doc_id) doc_len"
)

var ft_runSql = sqlexec.RunSql
var ft_runSql_streaming = sqlexec.RunStreamingSql

type fulltextState struct {
	inited           bool
	errCh            chan error
	streamCh         chan executor.Result
	streamingStarted bool
	n_result         uint64
	sacc             *fulltext.SearchAccum
	limit            uint64
	nrows            int
	idx2word         map[int]string
	agghtab          map[any]uint64
	aggcnt           []int64
	mpool            *fulltext.FixedBytePool
	param            fulltext.FullTextParserParam
	docLenMap        map[any]int32
	docIDMap         map[any]any
	minheap          vectorindex.SearchResultHeap
	resbuf           []*vectorindex.SearchResultAnyKey
	ranking          bool
	publisherAccount *uint32
	publisherDB      string

	// Partition-ordered traversal of agghtab for the zero-LIMIT scoring path.
	// Built ONCE per scoring phase (aggregation is complete before the first
	// evaluate call) and drained across evaluate batches; rebuilding it per 8K
	// output batch would cost O(N) workspace per batch and O(N^2/8192)
	// traversal work overall (#25638 review). Consumed slots are nil'd so
	// variable-width doc IDs can be reclaimed incrementally.
	scoreKeys    []any
	scorePos     int
	scoreOrdered bool

	// Serialized membership-filter (docfilter) bytes for reader-level doc_id filtering
	fulltextMembershipFilter []byte

	// holding output batch
	batch *batch.Batch
}

func (u *fulltextState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (u *fulltextState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltextState) resetRowState(proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	if u.mpool != nil {
		u.mpool.Close()
		u.mpool = nil
	}
	u.errCh = make(chan error, 2)
	u.streamCh = make(chan executor.Result, 8)
	u.streamingStarted = false
	u.n_result = 0
	u.sacc = nil
	u.nrows = 0
	u.idx2word = make(map[int]string)
	u.agghtab = nil
	u.aggcnt = nil
	u.docLenMap = make(map[any]int32)
	u.docIDMap = make(map[any]any)
	u.minheap = nil
	u.resbuf = nil
	u.scoreKeys = nil
	u.scorePos = 0
	u.scoreOrdered = false
	u.publisherAccount = nil
	u.publisherDB = ""
}

func (u *fulltextState) sqlProcess(proc *process.Process) *sqlexec.SqlProcess {
	sqlProc := sqlexec.NewSqlProcess(proc)
	if u.publisherAccount != nil {
		sqlProc.WithExecutionIdentity(*u.publisherAccount, u.publisherDB)
	}
	return sqlProc
}

func (u *fulltextState) resolveExecutionTarget(
	proc *process.Process,
	tf *TableFunction,
	sourceTable string,
	indexTable string,
) (string, string, error) {
	sourceRef, indexRef := tf.FulltextSourceRef, tf.FulltextIndexRef
	if sourceRef == nil && indexRef == nil {
		return sourceTable, indexTable, nil
	}
	if sourceRef == nil || indexRef == nil {
		return "", "", moerr.NewInternalError(proc.Ctx, "incomplete trusted fulltext table references")
	}
	if sourceRef.PubInfo == nil || indexRef.PubInfo == nil {
		return "", "", moerr.NewInternalError(proc.Ctx, "trusted fulltext table references require publisher identity")
	}
	if sourceRef.PubInfo.TenantId < 0 ||
		sourceRef.PubInfo.TenantId != indexRef.PubInfo.TenantId ||
		sourceRef.SchemaName == "" || sourceRef.SchemaName != indexRef.SchemaName ||
		sourceRef.ObjName == "" || indexRef.ObjName == "" ||
		sourceRef.SubscriptionName == "" || sourceRef.SubscriptionName != indexRef.SubscriptionName {
		return "", "", moerr.NewInternalError(proc.Ctx, "inconsistent trusted fulltext table references")
	}

	accountID := uint32(sourceRef.PubInfo.TenantId)
	u.publisherAccount = &accountID
	u.publisherDB = sourceRef.SchemaName
	return sqlquote.QualifiedIdent(sourceRef.SchemaName, sourceRef.ObjName),
		sqlquote.QualifiedIdent(indexRef.SchemaName, indexRef.ObjName), nil
}

func (u *fulltextState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}

	if u.mpool != nil {
		u.mpool.Close()
	}

	if !u.streamingStarted || u.streamCh == nil {
		return
	}

	for {
		select {
		case res, ok := <-u.streamCh:
			if !ok {
				return
			}
			res.Close()
		case <-proc.Ctx.Done():
			return
		}
	}
}

func (u *fulltextState) normalizeDocID(docID any) any {
	if bytes, ok := docID.([]byte); ok {
		key := string(bytes)
		if _, exists := u.docIDMap[key]; !exists {
			u.docIDMap[key] = append([]byte(nil), bytes...)
			// A varchar/composite doc ID retains a string key AND a []byte copy in
			// the non-spillable side maps. Charge the actual retained size toward
			// the pool's fast-path budget re-check: the item-count interval alone
			// assumes small fixed-size IDs and would admit ~2 GiB of key bytes
			// between checks at the 65,535-byte varchar maximum (#25638).
			if u.mpool != nil {
				u.mpool.ChargeSideBytes(2 * uint64(len(bytes)))
			}
		}
		return key
	}
	return docID
}

func (u *fulltextState) outputDocID(docID any) any {
	if output, ok := u.docIDMap[docID]; ok {
		return output
	}
	if key, ok := docID.(string); ok {
		return []byte(key)
	}
	return docID
}

// return (doc_id, score) as result
// when scoremap is empty, return result end.
func (u *fulltextState) returnResult(proc *process.Process, scoremap map[any]float32) (vm.CallResult, error) {
	// return result
	if u.batch.VectorCount() == 1 {
		// only doc_id returned

		// write the batch
		for key := range scoremap {
			doc_id := u.outputDocID(key)
			// type of id follow primary key column
			vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())
			delete(u.docIDMap, key)
		}
	} else {
		// doc_id and score returned
		for key := range scoremap {
			doc_id := u.outputDocID(key)
			// type of id follow primary key column
			vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())

			// score
			vector.AppendFixed[float32](u.batch.Vecs[1], scoremap[key], false, proc.Mp())
			delete(u.docIDMap, key)
		}
	}

	u.batch.SetRowCount(len(scoremap))
	u.n_result += uint64(len(scoremap))

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil

}

func (u *fulltextState) returnResultFromBuffer(proc *process.Process, limit uint64) (vm.CallResult, error) {

	blocksz := 8192
	nres := len(u.resbuf)
	n := nres
	if uint64(n) > limit {
		n = int(limit)
	}
	if n > blocksz {
		n = blocksz
	}

	for i := range n {
		// get result in reversed order
		sr := u.resbuf[nres-i-1]
		doc_id := u.outputDocID(sr.Id)
		vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())

		if u.batch.VectorCount() > 1 {
			vector.AppendFixed[float32](u.batch.Vecs[1], float32(sr.GetDistance()), false, proc.Mp())
		}
		delete(u.docIDMap, sr.Id)
	}

	// remove the retrieved results from buffer
	u.resbuf = u.resbuf[:nres-n]

	u.batch.SetRowCount(n)
	u.n_result += uint64(n)
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil

}

// return (doc_id, score) as result
// when scoremap is empty, return result end.
func (u *fulltextState) returnResultFromHeap(proc *process.Process, limit uint64) (vm.CallResult, error) {

	if len(u.resbuf) > 0 {
		return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "result buffer is not empty.")
	}

	if u.minheap == nil {
		return vm.CancelResult, nil
	}

	// minheap is in reversed order so pop everything out
	for range u.minheap.Len() {
		sr := heap.Pop(&u.minheap).(*vectorindex.SearchResultAnyKey)
		u.resbuf = append(u.resbuf, sr)
	}

	return u.returnResultFromBuffer(proc, limit)
}

func (u *fulltextState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	var err error
	u.batch.CleanOnlyData()
	limit := u.limit
	topk := fulltextTopKLimit(limit, u.ranking)

	if !u.ranking {

		// number of result more than pushdown limit and exit
		if limit > 0 && u.n_result >= limit {
			return vm.CancelResult, nil
		}
	}

	if limit == 0 {
		// array is empty, try to get batch from SQL executor
		scoremap, err := evaluate(u, proc, u.sacc)
		if err != nil {
			return vm.CancelResult, err
		}

		if scoremap != nil {
			return u.returnResult(proc, scoremap)
		}
		return vm.CancelResult, nil

	} else {
		if len(u.resbuf) > 0 {
			return u.returnResultFromBuffer(proc, limit)
		}

		// build minheap
		if len(u.minheap) == 0 {
			err = sort_topk(u, proc, u.sacc, topk)
			if err != nil {
				return vm.CancelResult, err
			}
		}

		if u.minheap != nil {
			return u.returnResultFromHeap(proc, limit)
		}
		return vm.CancelResult, nil
	}
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current unnest impl will
// always return one batch per nthRow.
func (u *fulltextState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		if len(tf.Params) > 0 {
			err := sonic.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}
		u.batch = tf.createResultBatch()
		u.inited = true
	}
	u.resetRowState(proc)

	v := tf.ctr.argVecs[0]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("First argument (source table name) must be string, but got %s", v.GetType().String()))
	}
	source_table := v.GetStringAt(nthRow)

	v = tf.ctr.argVecs[1]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Second argument (index table name) must be string, but got %s", v.GetType().String()))
	}
	index_table := v.GetStringAt(nthRow)

	source_table, index_table, err := u.resolveExecutionTarget(proc, tf, source_table, index_table)
	if err != nil {
		return err
	}

	// Optional 5th argument: the zero-relevance guard for a MATCH score threshold that
	// was only known at EXECUTE (a prepared '?'). See QueryBuilder.fulltextRuntimeScoreGuard.
	//
	// This runs before the pattern is validated, so the guard decides the outcome
	// whatever the search term binds to. The refusal it restates is a plan-time
	// property of the threshold alone; letting a NULL or empty pattern report its own
	// error first would answer `AGAINST(NULL) > ?` differently from the identical
	// literal, which the planner refuses outright.
	if err := checkFulltextZeroRelevanceGuard(proc, tf.ctr.argVecs, 4, nthRow); err != nil {
		return err
	}

	v = tf.ctr.argVecs[2]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Third argument (pattern) must be string, but got %s", v.GetType().String()))
	}
	if v.IsConstNull() || v.GetNulls().Contains(uint64(nthRow)) {
		return moerr.NewInvalidInput(proc.Ctx, "fulltext search pattern must not be NULL")
	}
	pattern := v.GetStringAt(nthRow)
	if len(pattern) == 0 {
		return moerr.NewInvalidInput(proc.Ctx, "fulltext search pattern must not be empty")
	}

	v = tf.ctr.argVecs[3]
	if v.GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Fourth argument (mode) must be int64, but got %s", v.GetType().String()))
	}
	mode := vector.GetFixedAtNoTypeCheck[int64](v, nthRow)

	scoreAlgo, err := fulltext.GetScoreAlgo(proc)
	if err != nil {
		return err
	}

	return fulltextIndexMatch(u, proc, tf, source_table, index_table, pattern, mode, scoreAlgo, u.batch)
}

// prepare
func fulltextIndexScanPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextState{}
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	if err != nil {
		return nil, err
	}

	st.limit, err = evalLimitExpression(proc, tableFunction.Limit, 0)
	if err != nil {
		return nil, err
	}

	// TODO: LIMIT BY RANK should set ranking to true
	st.ranking = false
	return st, err
}

// run SQL to get the (doc_id, word_index) of all patterns (words) in the search string
func runWordStats(
	ctx context.Context,
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
) (result executor.Result, err error) {

	var sql string
	if sql, err = fulltext.PatternToSql(
		s.Pattern, s.Mode, s.TblName, u.param.Parser, s.ScoreAlgo,
	); err != nil {
		return
	}

	sqlProc := u.sqlProcess(proc)
	// Attach the membership filter for reader-level doc_id filtering on the fulltext index table.
	if len(u.fulltextMembershipFilter) > 0 {
		sqlProc.FulltextMembershipFilter = u.fulltextMembershipFilter
	}

	result, err = ft_runSql_streaming(ctx, sqlProc, sql, u.streamCh, u.errCh)

	return
}

// traversalKeySize is the workspace cost per key of the partition-ordered
// traversal: one interface header in the flat key buffer.
const traversalKeySize = 16

// scoreTraversalEstimate is the EXACT workspace partitionOrderedKeys allocates
// for nkeys keys over npart partitions: one flat interface buffer plus one
// per-partition offset array (reused between the counting and placement passes)
// and slice headers. Kept as a function so the admission estimate and the
// regression that measures the real allocation share one definition.
func scoreTraversalEstimate(nkeys, npart int) uint64 {
	return uint64(nkeys)*traversalKeySize + uint64(npart)*8 + 64
}

// partitionOrderedKeys returns agghtab's keys ordered by ascending pool-partition id.
// Go map iteration order is randomized and has no relation to the partition an
// address lives in; when partitions have spilled, scoring in map order makes GetItem
// evict and re-materialize WHOLE partitions per document (a diagnostic showed one
// partition reload per item read). Ordering by partition first guarantees each
// spilled partition is unspilled at most once per scoring pass (#25638).
//
// Workspace shape: ONE flat []any of exactly len(agghtab) plus one per-partition
// offset array — no append growth, no per-bucket slack — so the heap-budget
// admission estimate (scoreTraversalEstimate) matches the peak allocation.
// Construction is two O(n) map passes (count, then place), both honoring
// cancellation. Callers must build the traversal at most once per scoring phase
// and drain it incrementally, never rebuild it per output batch.
func partitionOrderedKeys(
	proc *process.Process,
	agghtab map[any]uint64,
	pool *fulltext.FixedBytePool,
) ([]any, error) {
	npart := pool.NumPartitions()
	if npart < 1 {
		npart = 1
	}
	if err := pool.CheckBudget(scoreTraversalEstimate(len(agghtab), npart)); err != nil {
		return nil, err
	}
	pidOf := func(addr uint64) uint64 {
		pid := fulltext.GetPartitionId(addr)
		if pid >= uint64(npart) {
			pid = uint64(npart - 1)
		}
		return pid
	}
	offsets := make([]int, npart)
	n := 0
	for _, addr := range agghtab {
		if n%cancelCheckInterval == 0 {
			if err := proc.Ctx.Err(); err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "fulltext scoring cancelled")
			}
		}
		n++
		offsets[pidOf(addr)]++
	}
	// per-partition counts -> start offsets
	sum := 0
	for i, c := range offsets {
		offsets[i] = sum
		sum += c
	}
	keys := make([]any, len(agghtab))
	n = 0
	for k, addr := range agghtab {
		if n%cancelCheckInterval == 0 {
			if err := proc.Ctx.Err(); err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "fulltext scoring cancelled")
			}
		}
		n++
		pid := pidOf(addr)
		keys[offsets[pid]] = k
		offsets[pid]++
	}
	return keys, nil
}

// cancelCheckInterval: how many scored documents between context-cancellation checks in
// the scoring loops. Scoring a large agghtab can involve partition-sized disk I/O, so a
// KILL / timeout must be able to stop the loop promptly.
const cancelCheckInterval = 1024

// evaluate the score for all document vectors in Agg hashtable.
// whenever there is 8192 results, return it immediately.
func evaluate(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (scoremap map[any]float32, err error) {

	// Build the partition-ordered traversal ONCE: aggregation is complete before
	// the first evaluate call, and the zero-LIMIT path re-enters evaluate for every
	// 8K output batch, so the ordering must be drained across batches — not rebuilt
	// per batch (#25638 review).
	if !u.scoreOrdered {
		if u.scoreKeys, err = partitionOrderedKeys(proc, u.agghtab, u.mpool); err != nil {
			return nil, err
		}
		u.scorePos = 0
		u.scoreOrdered = true
	}

	scoremap = make(map[any]float32, 8192)

	aggcnt := u.aggcnt

	// Consume the traversal in partition order so spilled partitions are
	// materialized at most once across ALL batches, honoring cancellation between
	// documents. Every candidate is freed and deleted from the side maps the
	// moment it is scored: a sparse result (e.g. a boolean query whose required
	// words filter most candidates) must not accumulate per-candidate state, so
	// per-call memory is bounded by the returned scoremap, not by the number of
	// candidates processed (#25638 review).
	n := 0
	for u.scorePos < len(u.scoreKeys) && len(scoremap) < 8192 {
		if n%cancelCheckInterval == 0 {
			if err := proc.Ctx.Err(); err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "fulltext evaluate cancelled")
			}
		}
		n++
		doc_id := u.scoreKeys[u.scorePos]
		u.scoreKeys[u.scorePos] = nil // let the (possibly wide) ID be reclaimed
		u.scorePos++

		addr, ok := u.agghtab[doc_id]
		if !ok {
			continue
		}
		docvec, err := u.mpool.GetItem(addr)
		if err != nil {
			return nil, err
		}

		docLen := int64(0)
		if l, ok := u.docLenMap[doc_id]; ok {
			docLen = int64(l)
		}

		score, err := s.Eval(docvec, docLen, aggcnt)
		if err != nil {
			return nil, err
		}

		// consumed: release the pooled item and side-map entries immediately
		if err := u.mpool.FreeItem(addr); err != nil {
			return nil, err
		}
		delete(u.agghtab, doc_id)
		delete(u.docLenMap, doc_id)

		if len(score) > 0 {
			scoremap[doc_id] = score[0]
		} else {
			delete(u.docIDMap, doc_id)
		}
	}
	if u.scorePos >= len(u.scoreKeys) {
		u.scoreKeys = nil // fully drained; release the flat buffer
	}

	return scoremap, nil
}

func sort_topk(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum, limit uint64) (err error) {
	if limit == 0 {
		return nil
	}
	aggcnt := u.aggcnt
	if u.minheap == nil {
		capacity := vectorindex.SearchResultPreallocate(limit)
		u.minheap = make(vectorindex.SearchResultHeap, 0, capacity)
		u.resbuf = make([]*vectorindex.SearchResultAnyKey, 0, capacity)
	}
	heap.Init(&u.minheap)

	// score in partition order so spilled partitions are materialized at most once
	// per pass (map order would thrash whole-partition I/O per document), and honor
	// cancellation between documents so KILL/timeout can stop the I/O loop promptly.
	// sort_topk runs as a single pass, so the traversal is local and released on return.
	keys, err := partitionOrderedKeys(proc, u.agghtab, u.mpool)
	if err != nil {
		return err
	}
	n := 0
	{
		for i, doc_id := range keys {
			keys[i] = nil // let the (possibly wide) ID be reclaimed
			if n%cancelCheckInterval == 0 {
				if err := proc.Ctx.Err(); err != nil {
					return moerr.NewInternalError(proc.Ctx, "fulltext sort_topk cancelled")
				}
			}
			n++
			addr, ok := u.agghtab[doc_id]
			if !ok {
				continue
			}
			docvec, err := u.mpool.GetItem(addr)
			if err != nil {
				return err
			}

			docLen := int64(0)
			if len, ok := u.docLenMap[doc_id]; ok {
				docLen = int64(len)
			}

			score, err := s.Eval(docvec, docLen, aggcnt)
			if err != nil {
				return err
			}

			if len(score) > 0 {
				scoref64 := float64(score[0])
				if uint64(len(u.minheap)) >= limit {
					if u.minheap[0].GetDistance() < scoref64 {
						if u.ranking {
							// In ranking mode, free the evicted document's resources immediately
							// so they are not orphaned in agghtab after sort_topk returns.
							evictedID := u.minheap[0].(*vectorindex.SearchResultAnyKey).Id
							if evictedAddr, exists := u.agghtab[evictedID]; exists {
								err = u.mpool.FreeItem(evictedAddr)
								if err != nil {
									return err
								}
								delete(u.agghtab, evictedID)
								delete(u.docLenMap, evictedID)
								delete(u.docIDMap, evictedID)
							}
						}
						u.minheap[0] = &vectorindex.SearchResultAnyKey{Id: doc_id, Distance: scoref64}
						heap.Fix(&u.minheap, 0)
					}
				} else {
					heap.Push(&u.minheap, &vectorindex.SearchResultAnyKey{Id: doc_id, Distance: scoref64})
				}
			} else if u.ranking {
				err = u.mpool.FreeItem(addr)
				if err != nil {
					return err
				}
				delete(u.agghtab, doc_id)
				delete(u.docLenMap, doc_id)
				delete(u.docIDMap, doc_id)
			}
		}
	}

	if u.ranking {
		for _, it := range u.minheap {
			sr := it.(*vectorindex.SearchResultAnyKey)
			err = u.mpool.FreeItem(u.agghtab[sr.Id])
			if err != nil {
				return err
			}
			delete(u.agghtab, sr.Id)
			delete(u.docLenMap, sr.Id)
		}
	} else {
		survivors := make(map[any]struct{}, len(u.minheap))
		for _, it := range u.minheap {
			survivors[it.(*vectorindex.SearchResultAnyKey).Id] = struct{}{}
		}
		for docID, addr := range u.agghtab {
			err = u.mpool.FreeItem(addr)
			if err != nil {
				return err
			}
			delete(u.agghtab, docID)
			delete(u.docLenMap, docID)
			if _, ok := survivors[docID]; !ok {
				delete(u.docIDMap, docID)
			}
		}
	}

	return nil
}

func fulltextTopKLimit(limit uint64, ranking bool) uint64 {
	if !ranking {
		return limit
	}
	if limit > ^uint64(0)/3 {
		return ^uint64(0)
	}
	return 3 * limit
}

// result from SQL is (doc_id, index constant (refer to Pattern.Index))
// Two group by happens here
// 1. Group by the result into []uint8 which is DocCount[Pattern.Index].
// 2. Aggregate the total number of documents contain the word index (Pattern.Index). AggCnt[Pattern.Index].
func groupby(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (stream_closed bool, err error) {

	// first receive the batch and calculate the scoremap
	// We don't need to calculate mini-batch?????
	var res executor.Result
	var ok bool

	select {
	case res, ok = <-u.streamCh:
		if !ok {
			// channel closed and evaluate the rest of result
			return true, nil
		}
	case err = <-u.errCh:
		return false, err
	case <-proc.Ctx.Done():
		return false, moerr.NewInternalError(proc.Ctx, "context cancelled")
	}

	defer res.Close()

	if len(res.Batches) == 0 {
		return false, nil
	}
	bat := res.Batches[0]

	if len(bat.Vecs) > 3 {
		return false, moerr.NewInternalError(proc.Ctx, "output vector columns not match")
	}
	needSetDocLen := len(bat.Vecs) == 3

	u.nrows += bat.RowCount()

	for i := 0; i < bat.RowCount(); i++ {
		// doc_id any
		doc_id := u.normalizeDocID(vector.GetAny(bat.Vecs[0], i, false))

		if needSetDocLen {
			docLen := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[2], i)
			u.docLenMap[doc_id] = docLen
		}

		// word string
		widx := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[1], i)

		var docvec []uint8
		if s.Mode == int64(tree.FULLTEXT_NL) || s.Pattern[0].Operator == fulltext.PHRASE {
			// phrase search widx is dummy and fill in value 1 for all keywords
			nwords := s.Nkeywords
			addr, ok := u.agghtab[doc_id]
			if ok {
				docvec, err = u.mpool.GetItem(addr)
				if err != nil {
					return false, err
				}
				for i := 0; i < nwords; i++ {
					if docvec[i] < 255 {
						docvec[i]++
					}
				}
			} else {
				//docvec = make([]uint8, s.Nkeywords)
				addr, docvec, err = u.mpool.NewItem()
				if err != nil {
					return false, err
				}

				for i := 0; i < nwords; i++ {
					docvec[i] = 1
					u.aggcnt[i]++
				}
				u.agghtab[doc_id] = addr
			}
		} else {

			addr, ok := u.agghtab[doc_id]
			if ok {
				docvec, err = u.mpool.GetItem(addr)
				if err != nil {
					return false, err
				}
				if docvec[widx] < 255 {
					// limit doc count to 255 to fit uint8
					docvec[widx]++
				}
			} else {
				//docvec = make([]uint8, s.Nkeywords)
				addr, docvec, err = u.mpool.NewItem()
				if err != nil {
					return false, err
				}
				docvec[widx] = 1
				u.agghtab[doc_id] = addr
			}

			// update only once per doc_id
			if docvec[widx] == 1 {
				u.aggcnt[widx]++
			}

		}
		//logutil.Infof("ROW widx=%d, docid = %v", widx, doc_id)

	}

	return false, nil
}

// Run SQL to get number of records in source table
func runCountStar(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (executor.Result, error) {
	sqlFmt := countstar_sql
	if s.ScoreAlgo == fulltext.ALGO_BM25 {
		sqlFmt = countstar_avg_sql
	}
	sql := fmt.Sprintf(sqlFmt, s.TblName, fulltext.DOC_LEN_WORD)

	res, err := ft_runSql(u.sqlProcess(proc), sql)
	if err != nil {
		return executor.Result{}, err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return res, nil
	}

	bat := res.Batches[0]
	if bat.RowCount() == 1 {
		nrow := vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0)
		s.Nrow = nrow

		if bat.VectorCount() > 1 {
			avgDocLen := vector.GetFixedAtWithTypeCheck[float64](bat.Vecs[1], 0)
			s.AvgDocLen = avgDocLen
		} else {
			s.AvgDocLen = 0
		}
		//logutil.Infof("NROW = %d", nrow)
	}
	// downgrade BM25 to TF-IDF if AvgDocLen is zro
	if s.ScoreAlgo == fulltext.ALGO_BM25 && s.AvgDocLen == 0 {
		s.ScoreAlgo = fulltext.ALGO_TFIDF
	}

	return res, nil
}

func fulltextIndexMatch(
	u *fulltextState,
	proc *process.Process,
	tableFunction *TableFunction,
	srctbl, tblname, pattern string,
	mode int64,
	scoreAlgo fulltext.FullTextScoreAlgo,
	bat *batch.Batch,
) (err error) {

	opStats := tableFunction.OpAnalyzer.GetOpStats()

	// Wait for the unique-join-keys runtime filter if configured (pre-filter pushdown)
	if u.fulltextMembershipFilter == nil && len(tableFunction.RuntimeFilterSpecs) > 0 {
		bfResult, bfErr := waitFulltextMembershipFilter(proc, tableFunction.RuntimeFilterSpecs)
		if bfErr != nil {
			return bfErr
		}
		if bfResult != nil {
			u.fulltextMembershipFilter = bfResult.membershipFilterBytes
		}
	}

	if u.sacc == nil {
		// parse the search string to []Pattern and create SearchAccum
		s, err := fulltext.NewSearchAccum(srctbl, tblname, pattern, mode, string(tableFunction.Params), scoreAlgo)
		if err != nil {
			return err
		}

		u.mpool = fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0)
		u.agghtab = make(map[any]uint64, 1024)
		u.aggcnt = make([]int64, s.Nkeywords)

		// count(*) to get number of records in source table
		res, err := runCountStar(u, proc, s)
		if err != nil {
			return err
		}

		u.sacc = s

		opStats.BackgroundQueries = append(opStats.BackgroundQueries, res.LogicalPlan)

	}

	//t1 := time.Now()

	// we should wait the goroutine exit completely here,
	// even the SQL stream is done inside the `runWordStats`.
	// or will be resulting in data race on the tableFunction.
	var (
		waiter      sync.WaitGroup
		ctx, cancel = context.WithCancelCause(proc.GetTopContext())
	)
	defer cancel(nil)

	u.streamingStarted = true
	waiter.Add(1)
	go func() {
		defer func() {
			close(u.streamCh)
			waiter.Done()
		}()

		// get the statistic of search string ([]Pattern) and store in SearchAccum
		res, err2 := runWordStats(ctx, u, proc, u.sacc)
		if err2 != nil {
			u.errCh <- err2
			return
		}
		opStats.BackgroundQueries = append(opStats.BackgroundQueries, res.LogicalPlan)
	}()

	// get batch from SQL executor
	sql_closed := false
	for !sql_closed {
		if sql_closed, err = groupby(u, proc, u.sacc); err != nil {
			// notify the producer to stop the sql streaming
			cancel(err)
			break
		}
	}

	// wait for the sql streaming to be closed. make sure all the remaining
	// results in stream_chan are closed.
	if !sql_closed {
		for res := range u.streamCh {
			res.Close()
		}
	}

	waiter.Wait()

	if err == nil {
		// fetch potential remaining errors from error_chan
		select {
		case err = <-u.errCh:
		default:
		}
	}

	/*
		t2 := time.Now()
		diff := t2.Sub(t1)
		os.Stderr.WriteString(fmt.Sprintf("FULLTEXT: diff %v\n", diff))
		os.Stderr.WriteString(u.mpool.String())
	*/
	return
}

type fulltextMembershipFilterStatus uint8

const (
	fulltextMembershipFilterReady fulltextMembershipFilterStatus = iota
	fulltextMembershipFilterPass
	fulltextMembershipFilterDrop
)

// fulltextMembershipFilterResult holds the result from waiting for a unique-join-keys runtime filter.
type fulltextMembershipFilterResult struct {
	membershipFilterBytes []byte // serialized membership-filter payload for reader-level filtering
	status                fulltextMembershipFilterStatus
}

// waitFulltextMembershipFilter waits for a unique-join-keys runtime filter message,
// deserializes the PK vector, and builds the doc_id membership filter for reader-level filtering.
func waitFulltextMembershipFilter(proc *process.Process, specs []*plan.RuntimeFilterSpec) (*fulltextMembershipFilterResult, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	spec := specs[0]
	if !spec.UseMembershipFilter {
		return nil, nil
	}

	sqlProc := sqlexec.NewSqlProcess(proc)
	sqlProc.RuntimeFilterSpecs = specs

	vecbytes, err := sqlexec.WaitUniqueJoinKeys(sqlProc)
	if err != nil || len(vecbytes) == 0 {
		return nil, err
	}

	payload, err := buildFulltextMembershipFilter(proc, vecbytes)
	if err != nil {
		return nil, err
	}
	return &fulltextMembershipFilterResult{
		membershipFilterBytes: payload,
		status:                fulltextMembershipFilterReady,
	}, nil
}

// waitFulltext2MembershipFilter preserves PASS and DROP terminal states. A
// FULLTEXT2 candidate LIMIT is only safe while an exact membership filter is
// available; PASS must therefore fall back to an unbounded search, while DROP
// can terminate the search with an empty result.
func waitFulltext2MembershipFilter(proc *process.Process, specs []*plan.RuntimeFilterSpec) (*fulltextMembershipFilterResult, error) {
	if !hasFulltextMembershipFilterSpec(specs) {
		return nil, nil
	}

	sqlProc := sqlexec.NewSqlProcess(proc)
	sqlProc.RuntimeFilterSpecs = specs
	vecbytes, status, err := sqlexec.WaitUniqueJoinKeysWithStatus(sqlProc)
	if err != nil {
		return nil, err
	}
	switch status {
	case sqlexec.UniqueJoinKeysPass, sqlexec.UniqueJoinKeysNone:
		return &fulltextMembershipFilterResult{status: fulltextMembershipFilterPass}, nil
	case sqlexec.UniqueJoinKeysDrop:
		return &fulltextMembershipFilterResult{status: fulltextMembershipFilterDrop}, nil
	case sqlexec.UniqueJoinKeysAvailable:
		if len(vecbytes) == 0 {
			return &fulltextMembershipFilterResult{status: fulltextMembershipFilterPass}, nil
		}
		payload, err := buildFulltextMembershipFilter(proc, vecbytes)
		if err != nil {
			return nil, err
		}
		if len(payload) == 0 {
			return &fulltextMembershipFilterResult{status: fulltextMembershipFilterPass}, nil
		}
		return &fulltextMembershipFilterResult{
			membershipFilterBytes: payload,
			status:                fulltextMembershipFilterReady,
		}, nil
	default:
		return &fulltextMembershipFilterResult{status: fulltextMembershipFilterPass}, nil
	}
}

func hasFulltextMembershipFilterSpec(specs []*plan.RuntimeFilterSpec) bool {
	return len(specs) > 0 && specs[0].UseMembershipFilter
}

func buildFulltextMembershipFilter(proc *process.Process, vecbytes []byte) ([]byte, error) {
	if len(vecbytes) == 0 {
		return nil, nil
	}
	keyvec := new(vector.Vector)
	if err := keyvec.UnmarshalBinary(vecbytes); err != nil {
		return nil, err
	}
	// No keyvec.Free here on purpose: UnmarshalBinary aliases vecbytes (it sets
	// cantFreeData/cantFreeArea), so keyvec owns no mpool memory — the struct and
	// the aliased bytes are reclaimed by GC. Calling Free(mp) would be a no-op for
	// this zero-copy path, and tying its release to a specific mpool would be a
	// cross-pool free hazard if the deserialization ever became owning.

	// docfilter picks and tags the doc_id filter structure (exact set for integer
	// PKs, CBloomFilter otherwise); the reader reconstructs it at the allocation
	// site. The caller need not know which structure is used.
	payload, err := docfilter.BuildWithMemoryAdmission(
		keyvec,
		docfilter.AdmissionForService(proc.GetService()),
	)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// checkFulltextZeroRelevanceGuard evaluates the optional zero-relevance guard argument
// a driving fulltext table function carries when a MATCH score threshold is only known
// at execution.
//
// The planner cannot test `MATCH(...) <op> ?` at plan time, so it emits `0 <op> ?` as a
// boolean argument instead. True means a document with relevance 0 -- one this index
// never returns -- would satisfy the predicate, so answering from the index would drop
// exactly those rows. That is the same condition the planner rejects for a literal
// threshold, and it raises the same error, so `> ?` and `>= ?` behave identically to
// the literals they stand for at every bound value.
//
// A missing argument (the threshold was a literal, so the planner already checked it)
// or a NULL bound is not a violation: a NULL threshold makes the comparison NULL and
// the query returns no rows either way.
func checkFulltextZeroRelevanceGuard(proc *process.Process, argVecs []*vector.Vector, pos int, nthRow int) error {
	if pos >= len(argVecs) {
		return nil
	}
	v := argVecs[pos]
	if v == nil || v.Length() == 0 {
		return nil
	}
	if v.GetType().Oid != types.T_bool {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf(
			"fulltext score-threshold guard must be bool, but got %s", v.GetType().String()))
	}
	row := nthRow
	if v.IsConst() {
		row = 0
	}
	if v.IsConstNull() || v.GetNulls().Contains(uint64(row)) {
		return nil
	}
	if vector.GetFixedAtNoTypeCheck[bool](v, row) {
		return moerr.NewNotSupported(proc.Ctx,
			"MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is not supported yet.")
	}
	return nil
}
