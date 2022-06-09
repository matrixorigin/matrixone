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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/pb"
	"go.uber.org/zap"
)

// WithLogger setup zap logger for TxnCoordinator
func WithLogger(logger *zap.Logger) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.logger = logger
	}
}

// WithTxnIDGenerator setup txn id generator
func WithTxnIDGenerator(generator TxnIDGenerator) TxnClientCreateOption {
	return func(tc *txnClient) {
		tc.generator = generator
	}
}

var _ TxnClient = (*txnClient)(nil)

type txnClient struct {
	logger    *zap.Logger
	sender    TxnSender
	clock     clock.Clock
	generator TxnIDGenerator
}

// NewTxnClient
func NewTxnClient(sender TxnSender, options ...TxnClientCreateOption) TxnClient {
	c := &txnClient{sender: sender}
	for _, opt := range options {
		opt(c)
	}
	c.adjust()
	return c
}

func (client *txnClient) adjust() {
	client.logger = logutil.Adjust(client.logger).Named("txn")

	if client.generator == nil {
		client.generator = newUUIDTxnIDGenerator()
	}

	if client.clock == nil {
		clock.NewHLCClock(func() int64 {
			return time.Now().Unix()
		}, 0)
	}
}

func (client *txnClient) New(options ...TxnOption) TxnCoordinator {
	txn := pb.TxnMeta{}
	txn.ID = client.generator.Generate()

	now, _ := client.clock.Now()
	// TODO: Consider how to handle clock offsets. If use Clock-SI, can use the current
	// time minus the maximum clock offset as the transaction's snapshotTimestamp to avoid
	// conflicts due to clock uncertainty.
	txn.SnapshotTimestamp = now
	return newTxnCoordinator(client.sender, txn, options...)
}
