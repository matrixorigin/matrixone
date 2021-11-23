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

package metadata

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

type Wal = wal.ShardAwareWal
type ShardWal = wal.ShardWal

const (
	MinUncommitId = ^uint64(0) / 2
)

var uncommitId = MinUncommitId

func nextUncommitId() uint64 {
	return atomic.AddUint64(&uncommitId, uint64(1)) - 1
}

func IsTransientCommitId(id uint64) bool {
	return id >= MinUncommitId
}

type State = uint8

const (
	STInited State = iota
	STFull
	STClosed
	STSorted
)

type OpT uint8

const (
	OpReserved OpT = iota
	OpCreate
	OpUpgradeFull
	OpUpgradeClose
	OpUpgradeSorted
	OpSoftDelete
	OpReplaced
	OpHardDelete
	OpCreateIndex
	OpDropIndex
)

var OpNames = map[OpT]string{
	OpCreate:        "Create",
	OpUpgradeFull:   "UpgradeFull",
	OpUpgradeClose:  "UpgradeClose",
	OpUpgradeSorted: "UpgradeSorted",
	OpSoftDelete:    "SoftDelete",
	OpReplaced:      "Replaced",
	OpHardDelete:    "HardDelete",
}

func OpName(op OpT) string {
	return OpNames[op]
}

type LogRange struct {
	ShardId uint64       `json:"sid"`
	Range   common.Range `json:"range"`
}

func (r *LogRange) String() string {
	if r == nil {
		return "nil"
	}
	return fmt.Sprintf("LogRange<%d>%s", r.ShardId, r.Range.String())
}

func (r *LogRange) Marshal() ([]byte, error) {
	if r == nil {
		buf := make([]byte, 24)
		return buf, nil
	}
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(r.ShardId))
	buf.Write(encoding.EncodeUint64(r.Range.Left))
	buf.Write(encoding.EncodeUint64(r.Range.Right))
	return buf.Bytes(), nil
}

func (r *LogRange) Unmarshal(data []byte) error {
	buf := data
	r.ShardId = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	r.Range.Left = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	r.Range.Right = encoding.DecodeUint64(buf[:8])
	return nil
}

type CommitInfo struct {
	common.SSLLNode `json:"-"`
	CommitId        uint64    `json:"cid"`
	TranId          uint64    `json:"tid"`
	Op              OpT       `json:"op"`
	Size            int64     `json:"size"`
	LogIndex        *LogIndex `json:"idx"`
	PrevIndex       *LogIndex `json:"pidx"`
	LogRange        *LogRange `json:"range"`
}

func (info *CommitInfo) Clone() *CommitInfo {
	cloned := *info
	if cloned.LogIndex != nil {
		cloned.LogIndex = &(*info.LogIndex)
	}
	if cloned.LogRange != nil {
		cloned.LogRange = &(*info.LogRange)
	}
	return &cloned
}

func (info *CommitInfo) HasCommitted() bool {
	return !IsTransientCommitId(info.CommitId)
}

func (info *CommitInfo) CanUseTxn(tranId uint64) bool {
	if info.HasCommitted() {
		return true
	}
	return tranId == info.TranId
}

func (info *CommitInfo) SameTran(o *CommitInfo) bool {
	return info.TranId == o.TranId
}

func (info *CommitInfo) GetShardId() uint64 {
	if info == nil {
		return 0
	}
	if info.LogIndex == nil {
		return 0
	}
	return info.LogIndex.ShardId
}

func (info *CommitInfo) GetIndex() uint64 {
	if info == nil {
		return 0
	}
	if info.LogIndex == nil {
		return 0
	}
	return info.LogIndex.Id.Id
}

func (info *CommitInfo) IsHardDeleted() bool {
	return info.Op == OpHardDelete
}

func (info *CommitInfo) IsSoftDeleted() bool {
	return info.Op == OpSoftDelete
}

func (info *CommitInfo) IsDeleted() bool {
	return info.Op >= OpSoftDelete
}

func (info *CommitInfo) IsReplaced() bool {
	return info.Op == OpReplaced
}

func (info *CommitInfo) SetSize(size int64) {
	info.Size = size
}

func (info *CommitInfo) GetSize() int64 {
	return info.Size
}

func (info *CommitInfo) PString(level PPLevel) string {
	s := ""
	if info.LogRange != nil {
		s = fmt.Sprintf("%s%s: ", s, info.LogRange.String())
	}
	var curr, prev common.ISSLLNode
	curr = info
	for curr != nil {
		if prev != nil {
			s = fmt.Sprintf("%s => ", s)
		}
		cInfo := curr.(*CommitInfo)
		if cInfo.LogIndex != nil {
			s = fmt.Sprintf("%s[%s]", s, cInfo.LogIndex.String())
		}
		s = fmt.Sprintf("%s<%s,T-%d,C-%d>", s, OpName(cInfo.Op), cInfo.TranId-MinUncommitId, cInfo.CommitId)
		prev = curr
		curr = curr.GetNext()
	}
	return s
}

func (info *CommitInfo) SetIndex(idx LogIndex) error {
	info.LogIndex = &idx
	if info.LogRange == nil {
		info.LogRange = &LogRange{}
		info.LogRange.ShardId = idx.ShardId
	}
	info.LogRange.Range.Append(idx.Id.Id)
	return nil
}

type Sequence struct {
	nextDatabaseId uint64
	nextTableId    uint64
	nextSegmentId  uint64
	nextBlockId    uint64
	nextCommitId   uint64
	nextIndexId    uint64
}

func (s *Sequence) NextDatabaseId() uint64 {
	return atomic.AddUint64(&s.nextDatabaseId, uint64(1))
}

func (s *Sequence) NextTableId() uint64 {
	return atomic.AddUint64(&s.nextTableId, uint64(1))
}

func (s *Sequence) NextSegmentId() uint64 {
	return atomic.AddUint64(&s.nextSegmentId, uint64(1))
}

func (s *Sequence) NextBlockId() uint64 {
	return atomic.AddUint64(&s.nextBlockId, uint64(1))
}

func (s *Sequence) NextCommitId() uint64 {
	return atomic.AddUint64(&s.nextCommitId, uint64(1))
}

func (s *Sequence) NextIndexId() uint64 {
	return atomic.AddUint64(&s.nextIndexId, uint64(1))
}

func (s *Sequence) NextUncommitId() uint64 {
	return nextUncommitId()
}

func (s *Sequence) TryUpdateDatabaseId(id uint64) bool {
	if s.nextDatabaseId < id {
		s.nextDatabaseId = id
		return true
	}
	return false
}

func (s *Sequence) TryUpdateTableId(id uint64) bool {
	if s.nextTableId < id {
		s.nextTableId = id
		return true
	}
	return false
}

func (s *Sequence) TryUpdateCommitId(id uint64) bool {
	if s.nextCommitId < id {
		s.nextCommitId = id
		return true
	}
	return false
}

func (s *Sequence) TryUpdateSegmentId(id uint64) bool {
	if s.nextSegmentId < id {
		s.nextSegmentId = id
		return true
	}
	return false
}

func (s *Sequence) TryUpdateBlockId(id uint64) bool {
	if s.nextBlockId < id {
		s.nextBlockId = id
		return true
	}
	return false
}

func (s *Sequence) TryUpdateIndexId(id uint64) bool {
	if s.nextIndexId < id {
		s.nextIndexId = id
		return true
	}
	return false
}

func EstimateColumnBlockSize(colIdx int, meta *Block) uint64 {
	switch meta.Segment.Table.Schema.ColDefs[colIdx].Type.Oid {
	case types.T_json, types.T_char, types.T_varchar:
		return meta.Segment.Table.Schema.BlockMaxRows * 2 * 4
	default:
		return meta.Segment.Table.Schema.BlockMaxRows * uint64(meta.Segment.Table.Schema.ColDefs[colIdx].Type.Size)
	}
}

func EstimateBlockSize(meta *Block) uint64 {
	size := uint64(0)
	for colIdx, _ := range meta.Segment.Table.Schema.ColDefs {
		size += EstimateColumnBlockSize(colIdx, meta)
	}
	return size
}
