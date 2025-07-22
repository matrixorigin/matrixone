// Copyright 2022 Matrix Origin
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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type ISCPData struct {
	refcnt atomic.Int32

	insertBatch *AtomicBatch
	deleteBatch *AtomicBatch
	noMoreData  bool
	err         error
}

func NewISCPData(
	noMoreData bool,
	insertBatch *AtomicBatch,
	deleteBatch *AtomicBatch,
	err error,
) *ISCPData {
	d := &ISCPData{
		noMoreData:  noMoreData,
		insertBatch: insertBatch,
		deleteBatch: deleteBatch,
		err:         err,
	}
	return d
}

func (d *ISCPData) Set(cnt int) {
	d.refcnt.Add(int32(cnt))
}

func (d *ISCPData) Done() {
	newRefcnt := d.refcnt.Add(-1)
	if newRefcnt == 0 {
		if d.insertBatch != nil {
			d.insertBatch.Close()
			d.insertBatch = nil
		}
		if d.deleteBatch != nil {
			d.deleteBatch.Close()
			d.deleteBatch = nil
		}
	}
}

const (
	ISCPDataType_Snapshot int8 = iota
	ISCPDataType_Tail
)

type DataRetrieverImpl struct {
	*JobEntry
	*Iteration
	typ int8

	insertDataCh chan *ISCPData
	ctx          context.Context
	cancel       context.CancelFunc
	err          error

	mu sync.Mutex
}

func NewDataRetriever(
	consumer *JobEntry,
	iteration *Iteration,
	dataType int8,
) *DataRetrieverImpl {
	ctx, cancel := context.WithCancel(context.Background())
	return &DataRetrieverImpl{
		JobEntry:     consumer,
		Iteration:    iteration,
		insertDataCh: make(chan *ISCPData, 1),
		typ:          dataType,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (r *DataRetrieverImpl) Next() *ISCPData {
	data := <-r.insertDataCh
	return data
}

func (r *DataRetrieverImpl) UpdateWatermark(exec executor.TxnExecutor, opts executor.StatementOption) error {
	if r.typ == ISCPDataType_Snapshot {
		return nil
	}
	updateWatermarkSQL := cdc.CDCSQLBuilder.AsyncIndexLogUpdateResultSQL(
		r.tableInfo.accountID,
		r.tableInfo.tableID,
		r.indexName,
		r.to,
		0,
		"",
	)
	_, err := exec.Exec(updateWatermarkSQL, opts)
	return err
}

func (r *DataRetrieverImpl) GetDataType() int8 {
	return r.typ
}

func (r *DataRetrieverImpl) SetNextBatch(data *ISCPData) {
	if r.hasError() {
		return
	}
	select {
	case r.insertDataCh <- data:
		return
	case <-r.ctx.Done():
		return
	}
}

func (r *DataRetrieverImpl) hasError() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err != nil
}

// after error occurs, the data retriever won't consume any more data
func (r *DataRetrieverImpl) SetError(err error) {
	if r.hasError() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.cancel()
	r.err = err
}

func (r *DataRetrieverImpl) Close() {
	close(r.insertDataCh)
}
