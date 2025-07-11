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

package idxcdc

import (
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type CDCData struct {
	insertBatch *AtomicBatch
	deleteBatch *AtomicBatch
	noMoreData  bool
	err         error
}

const (
	CDCDataType_Snapshot int8 = iota
	CDCDataType_Tail
)

type DataRetrieverImpl struct {
	*SinkerEntry
	*Iteration
	txn          client.TxnOperator
	insertDataCh <-chan *CDCData
	ackChan      chan<- struct{}
	typ          int8
}

func NewDataRetriever(
	consumer *SinkerEntry,
	iteration *Iteration,
	txn client.TxnOperator,
	insertDataCh <-chan *CDCData,
	ackChan chan<- struct{},
	dataType int8,
) *DataRetrieverImpl {
	return &DataRetrieverImpl{
		SinkerEntry:  consumer,
		Iteration:    iteration,
		txn:          txn,
		insertDataCh: insertDataCh,
		ackChan:      ackChan,
		typ:          dataType,
	}
}

func (r *DataRetrieverImpl) Next() (insertBatch, deleteBatch *AtomicBatch, noMoreDate bool, err error) {
	data := <-r.insertDataCh
	defer func() {
		r.ackChan <- struct{}{}
	}()
	return data.insertBatch, data.deleteBatch, data.noMoreData, data.err
}

func (r *DataRetrieverImpl) UpdateWatermark(exec executor.TxnExecutor, opts executor.StatementOption) error {
	if r.typ == CDCDataType_Snapshot {
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
