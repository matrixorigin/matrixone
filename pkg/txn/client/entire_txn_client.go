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

package client

// EntireClient is a wrapper to support txn for temp engine.
type EntireClient struct {
	txnClient  TxnClient
	TempClient TxnClient
}

// tc1 should be txnClient
// tc2 should be memorystorage.StorageTxnClient
func NewEntireClient(tc1 TxnClient, tc2 TxnClient) TxnClient {
	return &EntireClient{
		txnClient:  tc1,
		TempClient: tc2,
	}
}

// New returns a TxnOperator to handle read and write operation for a
// transaction.
func (ec *EntireClient) New(options ...TxnOption) (TxnOperator, error) {
	var txnOperator TxnOperator
	var tempOperator TxnOperator
	var err error
	if txnOperator, err = ec.txnClient.New(options...); err != nil {
		return nil, err
	}
	if ec.TempClient != nil {
		if tempOperator, err = ec.TempClient.New(options...); err != nil {
			return nil, err
		}
	}
	return &EntireTxnOperator{
		txnOperator:  txnOperator,
		tempOperator: tempOperator,
	}, nil
}

// NewWithSnapshot create a txn operator from a snapshot. The snapshot must
// be from a CN coordinator txn operator.
func (ec *EntireClient) NewWithSnapshot(snapshot []byte) (TxnOperator, error) {
	var txnOperator TxnOperator
	var tempOperator TxnOperator
	var err error
	if txnOperator, err = ec.txnClient.NewWithSnapshot(snapshot); err != nil {
		return nil, err
	}
	if tempOperator, err = ec.TempClient.NewWithSnapshot(snapshot); err != nil {
		return nil, err
	}
	return &EntireTxnOperator{
		txnOperator:  txnOperator,
		tempOperator: tempOperator,
	}, nil
}

func (ec *EntireClient) Close() error {
	return ec.txnClient.Close()
}
