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

package hakeeper

import (
	"encoding/binary"
	"encoding/gob"
	"io"

	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	plog = logger.GetLogger("hakeeper")
)

var (
	binaryEnc = binary.BigEndian
)

const (
	headerSize = 2
)

const (
	// DefaultHAKeeperShardID is the shard ID assigned to the special HAKeeper
	// shard.
	DefaultHAKeeperShardID uint64 = 0
	queryLogShardIDTag     uint16 = 0xAE01
	createLogShardTag      uint16 = 0xAE02
)

type logShardIDQuery struct {
	name string
}

type logShardIDQueryResult struct {
	id    uint64
	found bool
}

type stateMachine struct {
	replicaID uint64
	NextID    uint64
	LogShards map[string]uint64
}

func parseCmdTag(cmd []byte) uint16 {
	return binaryEnc.Uint16(cmd)
}

func getCreateLogShardCmd(name string) []byte {
	return getLogShardCmd(name, createLogShardTag)
}

func getLogShardCmd(name string, tag uint16) []byte {
	cmd := make([]byte, headerSize+len(name))
	binaryEnc.PutUint16(cmd, tag)
	copy(cmd[headerSize:], []byte(name))
	return cmd
}

func isCreateLogShardCmd(cmd []byte) (string, bool) {
	return isLogShardCmd(cmd, createLogShardTag)
}

func isLogShardCmd(cmd []byte, tag uint16) (string, bool) {
	if len(cmd) <= headerSize {
		return "", false
	}
	if parseCmdTag(cmd) == tag {
		return string(cmd[headerSize:]), true
	}
	return "", false
}

func NewStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	if shardID != DefaultHAKeeperShardID {
		panic(moerr.NewError(moerr.INVALID_INPUT, "invalid HAKeeper shard ID"))
	}
	return &stateMachine{
		replicaID: replicaID,
		LogShards: make(map[string]uint64),
	}
}

func (s *stateMachine) assignID() uint64 {
	s.NextID++
	return s.NextID
}

func (s *stateMachine) handleCreateLogShardCmd(cmd []byte) (sm.Result, error) {
	name, ok := isCreateLogShardCmd(cmd)
	if !ok {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not create log shard cmd"))
	}
	if shardID, ok := s.LogShards[name]; ok {
		data := make([]byte, 8)
		binaryEnc.PutUint64(data, shardID)
		return sm.Result{Value: 0, Data: data}, nil
	}
	s.LogShards[name] = s.assignID()
	return sm.Result{Value: s.NextID}, nil
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	cmd := e.Cmd
	if _, ok := isCreateLogShardCmd(cmd); ok {
		return s.handleCreateLogShardCmd(cmd)
	}
	panic(moerr.NewError(moerr.INVALID_INPUT, "unexpected haKeeper cmd"))
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if q, ok := query.(*logShardIDQuery); ok {
		id, ok := s.LogShards[q.name]
		if ok {
			return &logShardIDQueryResult{found: true, id: id}, nil
		}
		return &logShardIDQueryResult{found: false}, nil
	}
	panic("unknown query type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	enc := gob.NewEncoder(w)
	return enc.Encode(s)
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(s)
}
