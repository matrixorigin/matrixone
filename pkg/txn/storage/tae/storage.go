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

package taestorage

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
)

type Storage struct {
	shard     metadata.DNShard
	logClient logservice.Client
	s3FS      fileservice.FileService
	localFS   fileservice.FileService
	clock     clock.Clock
}

func New(
	shard metadata.DNShard,
	logClient logservice.Client,
	s3FS fileservice.FileService,
	localFS fileservice.FileService,
	clock clock.Clock,
) (*Storage, error) {

	return &Storage{
		shard:     shard,
		logClient: logClient,
		s3FS:      s3FS,
		localFS:   localFS,
		clock:     clock,
	}, nil
}

var _ storage.TxnStorage = new(Storage)

// Close implements storage.TxnStorage
func (*Storage) Close(ctx context.Context) error {
	panic("unimplemented")
}

// Commit implements storage.TxnStorage
func (*Storage) Commit(ctx context.Context, txnMeta txn.TxnMeta) error {
	panic("unimplemented")
}

// Committing implements storage.TxnStorage
func (*Storage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	panic("unimplemented")
}

// Destroy implements storage.TxnStorage
func (*Storage) Destroy(ctx context.Context) error {
	panic("unimplemented")
}

// Prepare implements storage.TxnStorage
func (*Storage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	panic("unimplemented")
}

// Read implements storage.TxnStorage
func (*Storage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (storage.ReadResult, error) {
	panic("unimplemented")
}

// Rollback implements storage.TxnStorage
func (*Storage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error {
	panic("unimplemented")
}

// StartRecovery implements storage.TxnStorage
func (*Storage) StartRecovery(context.Context, chan txn.TxnMeta) {
	panic("unimplemented")
}

// Write implements storage.TxnStorage
func (*Storage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
	panic("unimplemented")
}
