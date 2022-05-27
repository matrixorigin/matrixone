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

package logservice

import (
	"io"

	sm "github.com/lni/dragonboat/v3/statemachine"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	defaultHAKeeperShardID uint64 = 0
	queryLogShardIDTag     uint16 = 0xAE01
	createLogShardTag      uint16 = 0xAE02
)

type haKeeperSM struct {
	replicaID uint64
	GlobalID  uint64
	LogShards map[string]uint64
}

func getQueryLogShardIDCmd(name string) []byte {
	return getLogShardCmd(name, queryLogShardIDTag)
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

func isQueryLogShardIDCmd(cmd []byte) (string, bool) {
	return isLogShardCmd(cmd, queryLogShardIDTag)
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

func newHAKeeperStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	if shardID != defaultHAKeeperShardID {
		panic(moerr.NewError(moerr.INVALID_INPUT, "invalid HAKeeper shard ID"))
	}
	return &haKeeperSM{
		replicaID: replicaID,
		LogShards: make(map[string]uint64),
	}
}

func (h *haKeeperSM) handleCreateLogShardCmd(cmd []byte) (sm.Result, error) {
	name, ok := isCreateLogShardCmd(cmd)
	if !ok {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not create log shard cmd"))
	}
	if shardID, ok := h.LogShards[name]; ok {
		data := make([]byte, 8)
		binaryEnc.PutUint64(data, shardID)
		return sm.Result{Value: 0, Data: data}, nil
	}
	h.GlobalID++
	h.LogShards[name] = h.GlobalID
	return sm.Result{Value: h.GlobalID}, nil
}

func (h *haKeeperSM) handleQueryLogShardIDCmd(cmd []byte) (sm.Result, error) {
	name, ok := isQueryLogShardIDCmd(cmd)
	if !ok {
		panic(moerr.NewError(moerr.INVALID_INPUT, "not query log shard id cmd"))
	}
	if shardID, ok := h.LogShards[name]; ok {
		return sm.Result{Value: shardID}, nil
	}
	return sm.Result{}, nil
}

func (h *haKeeperSM) Close() error {
	return nil
}

func (h *haKeeperSM) Update(cmd []byte) (sm.Result, error) {
	if _, ok := isCreateLogShardCmd(cmd); ok {
		return h.handleCreateLogShardCmd(cmd)
	}
	if _, ok := isQueryLogShardIDCmd(cmd); ok {
		return h.handleQueryLogShardIDCmd(cmd)
	}
	panic(moerr.NewError(moerr.INVALID_INPUT, "unexpected haKeeper cmd"))
}

func (h *haKeeperSM) Lookup(query interface{}) (interface{}, error) {
	panic("not implemented")
}

func (h *haKeeperSM) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	return gobMarshalTo(w, h)
}

func (h *haKeeperSM) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	return gobUnmarshalFrom(r, h)
}
