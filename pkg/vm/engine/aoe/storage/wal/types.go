// Copyright 2021 Matrix Origin
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

package wal

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
)

type Role uint8

const (
	HolderRole Role = iota
	BrokerRole
)

type Payload interface {
	Marshal() ([]byte, error)
}

type Wal interface {
	io.Closer
	Log(Payload) (*Entry, error)
	SyncLog(Payload) error
	Checkpoint(interface{})
	String() string
	GetRole() Role
}

type ShardAwareWal interface {
	Wal
	InitShard(uint64, uint64) error
	GetShardCheckpointId(uint64) uint64
	GetShardCurrSeqNum(uint64) uint64
	GetShardPendingCnt(uint64) int
	GetAllPendingEntries() []*shard.ItemsToCheckpointStat
}

type ShardWal struct {
	Wal     ShardAwareWal
	ShardId uint64
}

func NewWalShard(shardId uint64, wal ShardAwareWal) *ShardWal {
	return &ShardWal{
		Wal:     wal,
		ShardId: shardId,
	}
}

func (wal *ShardWal) GetPendingCnt() int {
	if wal.Wal == nil {
		return 0
	}
	return wal.Wal.GetShardPendingCnt(wal.ShardId)
}

func (wal *ShardWal) GetShardId() uint64 {
	return wal.ShardId
}

func (wal *ShardWal) InitWal(index uint64) error {
	if wal.Wal == nil {
		return nil
	}
	return wal.Wal.InitShard(wal.ShardId, index)
}

func (wal *ShardWal) WalEnabled() bool {
	return wal.Wal != nil
}

func (wal *ShardWal) GetCheckpointId() uint64 {
	if wal.Wal == nil {
		return 0
	}
	return wal.Wal.GetShardCheckpointId(wal.ShardId)
}

func (wal *ShardWal) GetCurrSeqNum() uint64 {
	if wal.Wal == nil {
		return 0
	}
	return wal.Wal.GetShardCurrSeqNum(wal.ShardId)
}

func (wal *ShardWal) Log(payload Payload) (*Entry, error) {
	if wal.Wal == nil {
		entry := GetEntry(uint64(0))
		entry.SetDone()
		return entry, nil
	}
	return wal.Wal.Log(payload)
}

func (wal *ShardWal) SyncLog(payload Payload) error {
	if wal.Wal == nil {
		return nil
	}
	return wal.Wal.SyncLog(payload)
}

func (wal *ShardWal) Checkpoint(v interface{}) {
	if wal.Wal == nil {
		return
	}
	wal.Wal.Checkpoint(v)
}
