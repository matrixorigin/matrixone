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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"os"
	"sync"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
)

type TAEStorage struct {
	shard     metadata.DNShard
	logClient logservice.Client
	fs        fileservice.FileService
	clock     clock.Clock
	tae       *db.DB
	txns      struct {
		sync.Mutex
		idMap map[string]txnif.AsyncTxn
	}
}

func openTAE(targetDir string) (*db.DB, error) {
	mask := syscall.Umask(0)
	if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
		syscall.Umask(mask)
		logutil.Infof("Recreate dir error:%v\n", err)
		return nil, err
	}
	syscall.Umask(mask)
	tae, err := db.Open(targetDir+"/tae", nil)
	if err != nil {
		logutil.Infof("Open tae failed. error:%v", err)
		return nil, err
	}
	return tae, nil
}

func NewTAEStorage(
	shard metadata.DNShard,
	logClient logservice.Client,
	fs fileservice.FileService,
	clock clock.Clock,
) (*TAEStorage, error) {
	//open tae, just for test.
	dir := "./store"
	engine, err := openTAE(dir)
	if err != nil {
		return nil, err
	}
	storage := &TAEStorage{
		shard:     shard,
		logClient: logClient,
		fs:        fs,
		clock:     clock,
		tae:       engine,
	}
	storage.txns.idMap = make(map[string]txnif.AsyncTxn)
	return storage, nil
}

var _ storage.TxnStorage = new(TAEStorage)

func (s *TAEStorage) getOrCreate(meta txn.TxnMeta) (txn txnif.AsyncTxn, err error) {
	s.txns.Lock()
	defer s.txns.Unlock()
	txn, ok := s.txns.idMap[string(meta.ID)]
	if !ok {
		//TODO::use StartTxnWithMeta, instead of StartTxn.
		txn, err = s.tae.StartTxn(nil)
	}
	return
}

func (s *TAEStorage) get(meta txn.TxnMeta) (txn txnif.AsyncTxn, err error) {
	s.txns.Lock()
	defer s.txns.Unlock()
	txn, ok := s.txns.idMap[string(meta.ID)]
	if !ok {
		return nil, storage.ErrMissingTxn
	}
	return
}

// Close implements storage.TxnTAEStorage
func (s *TAEStorage) Close(ctx context.Context) error {
	panic("unimplemented")
}

// Commit implements storage.TxnTAEStorage
func (s *TAEStorage) Commit(ctx context.Context, txnMeta txn.TxnMeta) error {
	txnHandle, err := s.get(txnMeta)
	if err != nil {
		return err
	}
	return txnHandle.Commit()

}

// Committing implements storage.TxnTAEStorage
func (s *TAEStorage) Committing(ctx context.Context, txnMeta txn.TxnMeta) error {
	panic("unimplemented")
}

// Destroy implements storage.TxnTAEStorage
func (s *TAEStorage) Destroy(ctx context.Context) error {
	panic("unimplemented")
}

// Prepare implements storage.TxnTAEStorage
func (s *TAEStorage) Prepare(ctx context.Context, txnMeta txn.TxnMeta) (timestamp.Timestamp, error) {
	panic("unimplemented")
}

// Read implements storage.TxnTAEStorage
func (s *TAEStorage) Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (storage.ReadResult, error) {
	panic("unimplemented")
}

// Rollback implements storage.TxnTAEStorage
func (s *TAEStorage) Rollback(ctx context.Context, txnMeta txn.TxnMeta) error {
	txnHandle, err := s.get(txnMeta)
	if err != nil {
		return err
	}
	return txnHandle.Rollback()
}

// StartRecovery implements storage.TxnTAEStorage
func (s *TAEStorage) StartRecovery(context.Context, chan txn.TxnMeta) {
	panic("unimplemented")
}

// Write implements storage.TxnTAEStorage
func (s *TAEStorage) Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error) {
	txnHandle, err := s.getOrCreate(txnMeta)
	if err != nil {
		return nil, err
	}

	precommitW := &api.PrecommitWriteCmd{}
	err = types.Decode(payload, precommitW)
	if err != nil {
		return nil, err
	}
	for _, entry := range precommitW.EntryList {
		//err = doHandleCmd(txnHandle, entry)
		err = txnHandle.HandleCmd(entry)
	}
	return nil, err
}
