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
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
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

	createLogShardTag uint16 = 0xAE01
	dnHeartbeatTag    uint16 = 0xAE02
	logHeartbeatTag   uint16 = 0xAE03
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
	Tick      uint64
	NextID    uint64
	LogShards map[string]uint64
	DNState   DNState
	LogState  LogState
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

func isDNHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, dnHeartbeatTag)
}

func isLogHeartbeatCmd(cmd []byte) bool {
	return isHeartbeatCmd(cmd, logHeartbeatTag)
}

func isHeartbeatCmd(cmd []byte, tag uint16) bool {
	if len(cmd) <= headerSize {
		return false
	}
	return parseCmdTag(cmd) == tag
}

func getHeartbeatCmd(cmd []byte) []byte {
	return cmd[headerSize:]
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
		DNState:   NewDNState(),
		LogState:  NewLogState(),
	}
}

func (s *stateMachine) Close() error {
	return nil
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

func (s *stateMachine) handleDNHeartbeat(cmd []byte) (sm.Result, error) {
	data := getHeartbeatCmd(cmd)
	var hb logservice.DNStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.DNState.Update(hb, s.Tick)
	return sm.Result{}, nil
}

func (s *stateMachine) handleLogHeartbeat(cmd []byte) (sm.Result, error) {
	data := getHeartbeatCmd(cmd)
	var hb logservice.LogStoreHeartbeat
	if err := hb.Unmarshal(data); err != nil {
		panic(err)
	}
	s.LogState.Update(hb, s.Tick)
	return sm.Result{}, nil
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	cmd := e.Cmd
	if _, ok := isCreateLogShardCmd(cmd); ok {
		return s.handleCreateLogShardCmd(cmd)
	} else if isDNHeartbeatCmd(cmd) {
		return s.handleDNHeartbeat(cmd)
	} else if isLogHeartbeatCmd(cmd) {
		return s.handleLogHeartbeat(cmd)
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
