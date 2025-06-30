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

package cdc

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type DataRetriever interface {
	Next() (insertBatch *AtomicBatch, deleteBatch *AtomicBatch, noMoreData bool, err error)
	UpdateWatermark() error
}

type TxnRetriever struct {
	Txn *client.TxnOperator
}

func (r *TxnRetriever) Next() (insertBatch *AtomicBatch, deleteBatch *AtomicBatch, noMoreData bool, err error) {
	logutil.Infof("TxRetriever Next()")
	return nil, nil, true, nil
}

func (r *TxnRetriever) UpdateWatermark() error {
	logutil.Infof("TxnRetriever.UpdateWatermark()")
	return nil
}

var _ DataRetriever = new(TxnRetriever)

func NewTxnRetriever(txn *client.TxnOperator) DataRetriever {
	return &TxnRetriever{Txn: txn}
}

type ConsumerInfo struct {
	ConsumerType int8
	TableName    string
	DbName       string
	IndexName    string
}

type Consumer interface {
	Consume(DataRetriever) error
	Reset()
	Close()
}

type IndexConsumer struct {
	cnUUID    string
	info      *ConsumerInfo
	dbTblInfo *DbTableInfo
	tableDef  *plan.TableDef
	sqlWriter IndexSqlWriter
	exec      executor.SQLExecutor
	rowdata   []any
	rowdelete []any
}

var _ Consumer = new(IndexConsumer)

func NewIndexConsumer(cnUUID string,
	tableDef *plan.TableDef,
	info *ConsumerInfo) (Consumer, error) {

	exec, err := sqlExecutorFactory(cnUUID)
	if err != nil {
		return nil, err
	}

	dbTblInfo := &DbTableInfo{SinkDbName: info.DbName, SinkTblName: info.TableName}

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

	sqlwriter, err := NewIndexSqlWriter(ie.algo, dbTblInfo, tableDef, ie.indexes)

	c := &IndexConsumer{cnUUID: cnUUID,
		info:      info,
		dbTblInfo: dbTblInfo,
		tableDef:  tableDef,
		sqlWriter: sqlwriter,
		exec:      exec,
		rowdata:   make([]any, len(tableDef.Cols)),
		rowdelete: make([]any, 1),
	}

	return c, nil
}

func (c *IndexConsumer) Consume(r DataRetriever) error {
	noMoreData := false
	var insertBatch, deleteBatch *AtomicBatch
	var err error

	for !noMoreData {

		insertBatch, deleteBatch, noMoreData, err = r.Next()
		if err != nil {
			return err
		}

		if noMoreData {
			return nil
		}

		// update index
		var _ = insertBatch
		var _ = deleteBatch

	}
	return nil
}

func (c *IndexConsumer) Reset() {
	logutil.Infof("IndexConsumer.Reset")
}

func (c *IndexConsumer) Close() {
	logutil.Infof("IndexConsumer.Close")
}
