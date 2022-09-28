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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
)

type DynamicStorage struct {
	newStorage func() (*Storage, error)
	keyFunc    func(context.Context) (any, error)
	storages   struct {
		sync.Mutex
		Map map[any]*Storage
	}
}

func NewDynamicStorage(
	newStorage func() (*Storage, error),
	keyFunc func(context.Context) (any, error),
) *DynamicStorage {
	s := &DynamicStorage{
		newStorage: newStorage,
		keyFunc:    keyFunc,
	}
	s.storages.Map = make(map[any]*Storage)
	return s
}

var _ storage.TxnStorage = new(DynamicStorage)

func (d *DynamicStorage) Close(ctx context.Context) error {
	storage, err := d.get(ctx)
	if err != nil {
		return err
	}
	return storage.Close(ctx)
}

func (d *DynamicStorage) Commit(ctx context.Context, txnMeta txn.TxnMeta) error {
	storage, err := d.get(ctx)
	if err != nil {
		return err
	}
	return storage.Commit(ctx, txnMeta)
}

func (d *DynamicStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	storage, err := d.get(ctx)
	if err != nil {
		return err
	}
	return storage.Committing(ctx, txnMeta)
}

func (d *DynamicStorage) Destroy(ctx context.Context) error {
	storage, err := d.get(ctx)
	if err != nil {
		return err
	}
	return storage.Destroy(ctx)
}

func (d *DynamicStorage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (ts timestamp.Timestamp, err error) {
	storage, err := d.get(ctx)
	if err != nil {
		return
	}
	return storage.Prepare(ctx, txnMeta)
}

func (d *DynamicStorage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (res storage.ReadResult, err error) {
	storage, err := d.get(ctx)
	if err != nil {
		return
	}
	return storage.Read(ctx, txnMeta, op, payload)
}

func (d *DynamicStorage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error {
	storage, err := d.get(ctx)
	if err != nil {
		return nil
	}
	return storage.Rollback(ctx, txnMeta)
}

func (d *DynamicStorage) StartRecovery(ctx context.Context, ch chan txn.TxnMeta) {
	storage, err := d.get(ctx)
	if err != nil {
		return
	}
	storage.StartRecovery(ctx, ch)
}

func (d *DynamicStorage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
	storage, err := d.get(ctx)
	if err != nil {
		return nil, err
	}
	return storage.Write(ctx, txnMeta, op, payload)
}

func (d *DynamicStorage) get(ctx context.Context) (*Storage, error) {
	key, err := d.keyFunc(ctx)
	if err != nil {
		return nil, err
	}
	d.storages.Lock()
	defer d.storages.Unlock()
	storage, ok := d.storages.Map[key]
	if !ok {
		var err error
		storage, err = d.newStorage()
		if err != nil {
			return nil, err
		}
		d.storages.Map[key] = storage
	}
	return storage, nil
}
