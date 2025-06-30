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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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

type Consumer interface {
	Consume(DataRetriever) error
	Reset()
	Close()
}

type IndexConsumer struct {
}

var _ Consumer = new(IndexConsumer)

func NewIndexConsumer() (Consumer, error) {

	return &IndexConsumer{}, nil
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
