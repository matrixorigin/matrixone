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

package store

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	GroupCKP      = entry.GTCKp
	GroupInternal = entry.GTInternal
	GroupFiles    = entry.GTFiles
)

type Store interface {
	Append(gid uint32, entry entry.Entry) (lsn uint64, err error)
	RangeCheckpoint(gid uint32, start, end uint64, files ...string) (ckpEntry entry.Entry, err error)
	Load(gid uint32, lsn uint64) (entry.Entry, error)

	GetCurrSeqNum(gid uint32) (lsn uint64)
	GetSynced(gid uint32) (lsn uint64)
	GetPendding(gid uint32) (cnt uint64)
	GetCheckpointed(gid uint32) (lsn uint64)
	GetTruncated() uint64

	Replay(h ApplyHandle) error
	Close() error
}

type ApplyHandle = func(group uint32, commitId uint64, payload []byte, typ uint16, info any)
