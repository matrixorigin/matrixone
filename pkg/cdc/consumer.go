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
