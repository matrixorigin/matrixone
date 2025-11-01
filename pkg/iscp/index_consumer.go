// Copyright 2024 Matrix Origin
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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

/* IndexConsumer */
type IndexEntry struct {
	algo    string
	indexes []*plan.IndexDef
}

/* IndexConsumer */
type IndexConsumer struct {
	cnUUID       string
	cnEngine     engine.Engine
	cnTxnClient  client.TxnClient
	jobID        JobID
	info         *ConsumerInfo
	tableDef     *plan.TableDef
	sqlWriter    IndexSqlWriter
	rowdata      []any
	rowdelete    []any
	sqlBufSendCh chan []byte
	algo         string
}

var _ Consumer = new(IndexConsumer)

func NewIndexConsumer(cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	tableDef *plan.TableDef,
	jobID JobID,
	info *ConsumerInfo) (Consumer, error) {

	ie := &IndexEntry{indexes: make([]*plan.IndexDef, 0, 3)}

	for _, idx := range tableDef.Indexes {
		if idx.TableExist && (catalog.IsHnswIndexAlgo(idx.IndexAlgo) || catalog.IsIvfIndexAlgo(idx.IndexAlgo) || catalog.IsFullTextIndexAlgo(idx.IndexAlgo)) {
			key := idx.IndexName
			if key == info.IndexName {
				if len(ie.algo) == 0 {
					ie.algo = idx.IndexAlgo
				}
				ie.indexes = append(ie.indexes, idx)
			}
		}

	}

	sqlwriter, err := NewIndexSqlWriter(ie.algo, jobID, info, tableDef, ie.indexes)
	if err != nil {
		return nil, err
	}

	c := &IndexConsumer{cnUUID: cnUUID,
		cnEngine:    cnEngine,
		cnTxnClient: cnTxnClient,
		jobID:       jobID,
		info:        info,
		tableDef:    tableDef,
		sqlWriter:   sqlwriter,
		rowdata:     make([]any, len(tableDef.Cols)),
		rowdelete:   make([]any, 1),
		algo:        ie.algo,
		//sqlBufSendCh: make(chan []byte),
	}

	return c, nil
}

func runIndex(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	datatype := r.GetDataType()

	if datatype == ISCPDataType_Snapshot {
		// SNAPSHOT
		for {
			select {
			case <-ctx.Done():
				return
			case e2 := <-errch:
				errch <- e2
				return
			case sql, ok := <-c.sqlBufSendCh:
				if !ok {
					return
				}

				// no transaction required and commit every time.
				err := sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), 5*time.Minute, nil, nil,
					func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
						sqlctx := sqlproc.SqlCtx

						res, err := ExecWithResult(sqlproc.GetContext(), string(sql), sqlctx.GetService(), sqlctx.Txn())
						if err != nil {
							logutil.Errorf("cdc indexConsumer(%v) send sql failed, err: %v, sql: %s", c.info, err, string(sql))
							os.Stderr.WriteString(fmt.Sprintf("sql  executor run failed. %s\n", string(sql)))
							os.Stderr.WriteString(fmt.Sprintf("err :%v\n", err))
							return err
						}
						res.Close()
						return nil
					})

				if err != nil {
					errch <- err
					return
				}
			}
		}

	} else {

		// all updates under same transaction and transaction can last very long so set timeout to 24 hours
		err := sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), 24*time.Hour, nil, nil,
			func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
				sqlctx := sqlproc.SqlCtx

				// TAIL
				for {
					select {
					case <-ctx.Done():
						return
					case e2 := <-errch:
						errch <- e2
						return
					case sql, ok := <-c.sqlBufSendCh:
						if !ok {
							// channel closed
							return r.UpdateWatermark(sqlproc.GetContext(), sqlctx.GetService(), sqlctx.Txn())
						}

						// update SQL
						var res executor.Result
						res, err = ExecWithResult(sqlproc.GetContext(), string(sql), sqlctx.GetService(), sqlctx.Txn())
						if err != nil {
							return err
						}
						res.Close()
					}
				}
			})

		if err != nil {
			errch <- err
			return
		}
	}
}

func runHnsw[T types.RealNumbers](c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {

	datatype := r.GetDataType()

	// Suppose we shoult not use transaction here for Snapshot type and commit every time a new batch comes.
	// However, HNSW only run in local without save to database until Sync.Save().
	// HNSW is okay to have similar implementation to TAIL

	var err error
	var sync *hnsw.HnswSync[T]

	// read-only sql so no need transaction here.  All models are loaded at startup.
	err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), 30*time.Minute, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {

			w := c.sqlWriter.(*HnswSqlWriter[T])
			sync, err = w.NewSync(sqlproc)
			return err
		})

	if err != nil {
		errch <- err
		return
	}

	if sync == nil {
		errch <- moerr.NewInternalErrorNoCtx("failed create HnswSync")
		return
	}

	defer func() {
		if sync != nil {
			sync.Destroy()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case e2 := <-errch:
			errch <- e2
			return
		case sql, ok := <-c.sqlBufSendCh:
			if !ok {
				// channel closed

				// we need a transaction here to save model files and update watermark
				err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), time.Hour, nil, nil,
					func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
						sqlctx := sqlproc.SqlCtx

						// save model to db
						err = sync.Save(sqlproc)
						if err != nil {
							return
						}

						// update watermark
						if datatype == ISCPDataType_Tail {
							err = r.UpdateWatermark(sqlproc.GetContext(), sqlctx.GetService(), sqlctx.Txn())
							if err != nil {
								return
							}
						}
						return

					})

				if err != nil {
					errch <- err
					return
				}
				return
			}

			// sql -> cdc
			var cdc vectorindex.VectorIndexCdc[T]
			err = sonic.Unmarshal(sql, &cdc)
			if err != nil {
				errch <- err
				return
			}

			// HNSW models are already in local so hnsw Update should not require executing SQL or should be read-only. No transaction required.
			err = sqlexec.RunTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), 30*time.Minute, nil, nil,
				func(sqlproc *sqlexec.SqlProcess, cbdata any) (err error) {
					return sync.Update(sqlproc, &cdc)
				})

			if err != nil {
				errch <- err
				return
			}

		}
	}

}

func (c *IndexConsumer) run(ctx context.Context, errch chan error, r DataRetriever) {

	switch c.sqlWriter.(type) {
	case *HnswSqlWriter[float32]:
		// init HnswSync[float32]
		runHnsw[float32](c, ctx, errch, r)
	case *HnswSqlWriter[float64]:
		// init HnswSync[float64]
		runHnsw[float64](c, ctx, errch, r)
	default:
		// run fulltext/ivfflat index
		runIndex(c, ctx, errch, r)
	}
}

func (c *IndexConsumer) processISCPData(ctx context.Context, data *ISCPData, datatype int8, errch chan error) bool {
	// release the data

	if data == nil {
		err := c.flushCdc()
		if err != nil {
			errch <- err
		}
		close(c.sqlBufSendCh)
		return true
	}

	defer data.Done()

	insertBatch := data.insertBatch
	deleteBatch := data.deleteBatch
	noMoreData := data.noMoreData
	err := data.err
	if err != nil {
		errch <- err
		return true
	}

	if noMoreData {
		err := c.flushCdc()
		if err != nil {
			errch <- err
		}
		close(c.sqlBufSendCh)
		return noMoreData
	}

	// update index

	if datatype == ISCPDataType_Snapshot {
		// SNAPSHOT
		err := c.sinkSnapshot(ctx, insertBatch)
		if err != nil {
			// error out
			errch <- err
			noMoreData = true
		}

	} else {
		// sinkTail will save sql to the slice
		err := c.sinkTail(ctx, insertBatch, deleteBatch)
		if err != nil {
			// error out
			errch <- err
			noMoreData = true
		}
	}

	return noMoreData

}

func (c *IndexConsumer) Consume(ctx context.Context, r DataRetriever) error {
	noMoreData := false
	errch := make(chan error, 2)
	c.sqlBufSendCh = make(chan []byte)
	defer func() {
		c.sqlBufSendCh = nil
		c.sqlWriter.Reset()
	}()

	datatype := r.GetDataType()

	// create thread to poll sql
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.run(ctx, errch, r)
	}()

	// read data
	for !noMoreData {
		data := r.Next()
		noMoreData = c.processISCPData(ctx, data, datatype, errch)
	}

	wg.Wait()

	if len(errch) > 0 {
		return <-errch
	}

	return nil
}

func (c *IndexConsumer) sinkSnapshot(ctx context.Context, upsertBatch *AtomicBatch) error {
	var err error

	for _, bat := range upsertBatch.Batches {
		for i := 0; i < batchRowCount(bat); i++ {
			if err = extractRowFromEveryVector(ctx, bat, i, c.rowdata); err != nil {
				return err
			}

			err = c.sqlWriter.Upsert(ctx, c.rowdata)
			if err != nil {
				return err
			}

			if c.sqlWriter.Full() {
				err = c.sendSql(c.sqlWriter)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// upsertBatch and deleteBatch is sorted by ts
// for the same ts, delete first, then upsert
func (c *IndexConsumer) sinkTail(ctx context.Context, upsertBatch, deleteBatch *AtomicBatch) error {
	var err error

	upsertIter := upsertBatch.GetRowIterator().(*atomicBatchRowIter)
	deleteIter := deleteBatch.GetRowIterator().(*atomicBatchRowIter)
	defer func() {
		upsertIter.Close()
		deleteIter.Close()
	}()

	// output sql until one iterator reach the end
	upsertIterHasNext, deleteIterHasNext := upsertIter.Next(), deleteIter.Next()
	for upsertIterHasNext && deleteIterHasNext {
		upsertItem, deleteItem := upsertIter.Item(), deleteIter.Item()
		// compare ts, ignore pk
		if upsertItem.Ts.LT(&deleteItem.Ts) {
			if err = c.sinkInsert(ctx, upsertIter); err != nil {
				return err
			}
			// get next item
			upsertIterHasNext = upsertIter.Next()
		} else {
			if err = c.sinkDelete(ctx, deleteIter); err != nil {
				return err
			}
			// get next item
			deleteIterHasNext = deleteIter.Next()
		}
	}

	// output the rest of upsert iterator
	for upsertIterHasNext {
		if err = c.sinkInsert(ctx, upsertIter); err != nil {
			return err
		}
		// get next item
		upsertIterHasNext = upsertIter.Next()
	}

	// output the rest of delete iterator
	for deleteIterHasNext {
		if err = c.sinkDelete(ctx, deleteIter); err != nil {
			return err
		}
		// get next item
		deleteIterHasNext = deleteIter.Next()
	}
	c.flushCdc()
	return nil
}

func (c *IndexConsumer) sinkInsert(ctx context.Context, upsertIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	if err = upsertIter.Row(ctx, c.rowdata); err != nil {
		return err
	}

	if !c.sqlWriter.CheckLastOp(vectorindex.CDC_INSERT) {
		// last op is not INSERT, sendSql first
		// send SQL
		err = c.sendSql(c.sqlWriter)
		if err != nil {
			return err
		}

	}

	err = c.sqlWriter.Insert(ctx, c.rowdata)
	if err != nil {
		return err
	}

	if c.sqlWriter.Full() {
		// send SQL
		err = c.sendSql(c.sqlWriter)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *IndexConsumer) sinkDelete(ctx context.Context, deleteIter *atomicBatchRowIter) (err error) {

	// get row from the batch
	if err = deleteIter.Row(ctx, c.rowdelete); err != nil {
		return err
	}

	if !c.sqlWriter.CheckLastOp(vectorindex.CDC_DELETE) {
		// last op is not DELETE, sendSql first
		// send SQL
		err = c.sendSql(c.sqlWriter)
		if err != nil {
			return err
		}

	}

	err = c.sqlWriter.Delete(ctx, c.rowdelete)
	if err != nil {
		return err
	}

	if c.sqlWriter.Full() {
		// send SQL
		err = c.sendSql(c.sqlWriter)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *IndexConsumer) sendSql(writer IndexSqlWriter) error {
	if writer.Empty() {
		return nil
	}

	// generate sql from cdc
	sql, err := writer.ToSql()
	if err != nil {
		return err
	}

	c.sqlBufSendCh <- sql
	//os.Stderr.WriteString(string(sql))
	//os.Stderr.WriteString("\n")

	// reset
	writer.Reset()

	return nil
}

func (c *IndexConsumer) flushCdc() error {
	return c.sendSql(c.sqlWriter)
}
