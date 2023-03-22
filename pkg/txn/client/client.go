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

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

// WithTxnIDGenerator setup txn id generator
func WithTxnIDGenerator(generator TxnIDGenerator) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.generator = generator
	}
}

// WithLockService setup lock service
func WithLockService(lockService lockservice.LockService) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.lockService = lockService
	}
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	rt          runtime.Runtime
	sender      rpc.TxnSender
	generator   TxnIDGenerator
	lockService lockservice.LockService
}

// NewTxnClient create a txn client with TxnSender and Options
func NewTxnClient(
	rt runtime.Runtime,
	sender rpc.TxnSender,
	options ...TxnClientCreateOption) TxnClient {
	c := &txnClient{rt: rt, sender: sender}
	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	return c
}

func (client *txnClient) adjust() {
	if client.generator == nil {
		client.generator = newUUIDTxnIDGenerator()
	}
	if client.rt.Clock() == nil {
		panic("txn clock not set")
	}
}

func (client *txnClient) New(options ...TxnOption) (TxnOperator, error) {
	txnMeta := txn.TxnMeta{}
	txnMeta.ID = client.generator.Generate()
	now, _ := client.rt.Clock().Now()
	// TODO: Consider how to handle clock offsets. If use Clock-SI, can use the current
	// time minus the maximum clock offset as the transaction's snapshotTimestamp to avoid
	// conflicts due to clock uncertainty.
	txnMeta.SnapshotTS = now
	txnMeta.Mode = client.getTxnMode()
	txnMeta.Isolation = client.getTxnIsolation()
	options = append(options,
		WithTxnCNCoordinator(),
		WithTxnLockService(client.lockService))
	return newTxnOperator(
		client.rt,
		client.sender,
		txnMeta,
		options...), nil
}

func (client *txnClient) NewWithSnapshot(snapshot []byte) (TxnOperator, error) {
	return newTxnOperatorWithSnapshot(client.rt, client.sender, snapshot)
}

func (client *txnClient) Close() error {
	return client.sender.Close()
}

func (client *txnClient) getTxnIsolation() txn.TxnIsolation {
	if v, ok := client.rt.GetGlobalVariables(runtime.TxnIsolation); ok {
		return v.(txn.TxnIsolation)
	}
	return txn.TxnIsolation_RC
}

func (client *txnClient) getTxnMode() txn.TxnMode {
	if v, ok := client.rt.GetGlobalVariables(runtime.TxnMode); ok {
		return v.(txn.TxnMode)
	}
	return txn.TxnMode_Pessimistic
}
