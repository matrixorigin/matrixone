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

package memorystorage

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
)

type Storage struct {
	handler Handler
}

func New(
	handler Handler,
) (*Storage, error) {
	s := &Storage{
		handler: handler,
	}
	return s, nil
}

var _ storage.TxnStorage = new(Storage)

func (s *Storage) Commit(ctx context.Context, txnMeta txn.TxnMeta) error {
	return s.handler.HandleCommit(ctx, txnMeta)
}

func (s *Storage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	return s.handler.HandleCommitting(ctx, txnMeta)
}

func (s *Storage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	return s.handler.HandlePrepare(ctx, txnMeta)
}

func (s *Storage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error {
	return s.handler.HandleRollback(ctx, txnMeta)
}

func (s *Storage) StartRecovery(ctx context.Context, ch chan txn.TxnMeta) {
	s.handler.HandleStartRecovery(ctx, ch)
}

func (s *Storage) Close(ctx context.Context) error {
	return s.handler.HandleClose(ctx)
}

func (s *Storage) Destroy(ctx context.Context) error {
	return s.handler.HandleDestroy(ctx)
}
