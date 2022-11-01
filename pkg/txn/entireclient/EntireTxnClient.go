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

package entireclient

import (
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type EntireClient struct {
	txnClient  client.TxnClient
	TempClient client.TxnClient
}

// tc1 should be client.txnClient
// tc2 should be memorystorage.StorageTxnClient
func NewEntireClient(tc1 client.TxnClient, tc2 client.TxnClient) client.TxnClient {
	return &EntireClient{
		txnClient:  tc1,
		TempClient: tc2,
	}
}

// New returns a TxnOperator to handle read and write operation for a
// transaction.
func (ec *EntireClient) New(options ...client.TxnOption) (client.TxnOperator, error) {
	var txnOperator client.TxnOperator
	var tempOperator client.TxnOperator
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
func (ec *EntireClient) NewWithSnapshot(snapshot []byte) (client.TxnOperator, error) {
	var txnOperator client.TxnOperator
	var tempOperator client.TxnOperator
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
