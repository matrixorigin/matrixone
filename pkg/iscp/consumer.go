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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

/* IndexConsumer */
type IndexEntry struct {
	algo    string
	indexes []*plan.IndexDef
}

var sqlExecutorFactory = _sqlExecutorFactory

func _sqlExecutorFactory(cnUUID string) (executor.SQLExecutor, error) {
	// sql executor
	v, ok := runtime.ServiceRuntime(cnUUID).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		//os.Stderr.WriteString(fmt.Sprintf("sql executor create failed. cnUUID = %s\n", cnUUID))
		return nil, moerr.NewNotSupportedNoCtx("no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)
	return exec, nil
}

/* IndexConsumer */
type IndexConsumer struct {
	cnUUID       string
	info         *ConsumerInfo
	tableDef     *plan.TableDef
	sqlWriter    IndexSqlWriter
	exec         executor.SQLExecutor
	rowdata      []any
	rowdelete    []any
	sqlBufSendCh chan []byte
}

var _ Consumer = new(IndexConsumer)

func NewIndexConsumer(cnUUID string,
	tableDef *plan.TableDef,
	info *ConsumerInfo) (Consumer, error) {

	exec, err := sqlExecutorFactory(cnUUID)
	if err != nil {
		return nil, err
	}

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

	sqlwriter, err := NewIndexSqlWriter(ie.algo, info, tableDef, ie.indexes)
	if err != nil {
		return nil, err
	}

	c := &IndexConsumer{cnUUID: cnUUID,
		info:      info,
		tableDef:  tableDef,
		sqlWriter: sqlwriter,
		exec:      exec,
		rowdata:   make([]any, len(tableDef.Cols)),
		rowdelete: make([]any, 1),
		//sqlBufSendCh: make(chan []byte),
	}

	return c, nil
}

func (c *IndexConsumer) run(ctx context.Context, errch chan error, r DataRetriever) {

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
				func() {
					newctx, cancel := context.WithTimeout(context.Background(), time.Hour)
					defer cancel()
					//os.Stderr.WriteString("Wait for BEGIN but sql. execute anyway\n")
					opts := executor.Options{}
					res, err := c.exec.Exec(newctx, string(sql), opts)
					if err != nil {
						logutil.Errorf("cdc indexConsumer(%v) send sql failed, err: %v, sql: %s", c.info, err, string(sql))
						os.Stderr.WriteString(fmt.Sprintf("sql  executor run failed. %s\n", string(sql)))
						os.Stderr.WriteString(fmt.Sprintf("err :%v\n", err))
						errch <- err
					}
					res.Close()
				}()
			}
		}

	} else {
		// TAIL
		newctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		opts := executor.Options{}
		err := c.exec.ExecTxn(newctx,
			func(exec executor.TxnExecutor) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case e2 := <-errch:
						return e2
					case sql, ok := <-c.sqlBufSendCh:
						if !ok {
							// channel closed
							return r.UpdateWatermark(exec, opts.StatementOption())
						}

						// update SQL
						res, err := exec.Exec(string(sql), opts.StatementOption())
						if err != nil {
							return err
						}
						res.Close()
					}
				}
			}, opts)
		if err != nil {
			errch <- err
			return
		}
	}

}

func (c *IndexConsumer) processISCPData(ctx context.Context, data *ISCPData, datatype int8, errch chan error) bool {
	// release the data
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

	c.sqlWriter.Insert(ctx, c.rowdata)

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

	c.sqlWriter.Delete(ctx, c.rowdelete)

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
	os.Stderr.WriteString(string(sql))
	os.Stderr.WriteString("\n")

	// reset
	writer.Reset()

	return nil
}

func (c *IndexConsumer) flushCdc() error {
	return c.sendSql(c.sqlWriter)
}
