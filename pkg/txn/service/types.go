// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnService is a transaction service that runs on the TNStore and receives
// transaction requests from the CN.
//
// The TxnService is managed by the DNStore and a TxnService serves only one DNShard.
//
// The txn service uses Clock-SI for transaction processing.
type TxnService interface {
	// Shard returns the metadata of DNShard
	Shard() metadata.TNShard
	// Start start the txn service
	Start() error
	// Close close the txn service. Destroy TxnStorage if destroy is true.
	Close(destroy bool) error

	// Read handle txn read request from CN. For reuse, the response is provided by the
	// TODO: only read log tail.
	Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// Write handle txn write request from CN. For reuse, the response is provided by the caller
	Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// Commit handle txn commit request from CN. For reuse, the response is provided by the caller
	Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
	// Rollback handle txn rollback request from CN. For reuse, the response is provided by the caller
	Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error

	// Debug handle txn debug request from CN. For reuse, the response is provided by the caller
	Debug(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error
}
