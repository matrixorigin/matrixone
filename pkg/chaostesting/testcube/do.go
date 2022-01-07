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

package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixcube/raftstore"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Do(
	newKV NewKV,
	nodes fz.Nodes,
	log LogPorcupineOp,
	closeNode fz.CloseNode,
) fz.Do {

	var kvs []*KV
	for i := 0; i < len(nodes); i++ {
		kvs = append(kvs, newKV(nodes[i].(*Node)))
	}
	stopped := make(map[fz.NodeID]bool)

	return func(threadID int64, action fz.Action) error {

		switch action := action.(type) {

		case ActionSet:
			if stopped[fz.NodeID(action.ClientID)] {
				return nil
			}
			return log(
				func() (int, any, any, error) {
					if err := kvs[action.ClientID].Set(action.Key, action.Value, time.Minute*2); err != nil {
						if errors.Is(err, raftstore.ErrTimeout) {
							// timeout
							return int(threadID), [2]any{"set", action.Key}, fz.KVResultTimeout, nil
						}
						// error
						return int(threadID), nil, nil, err
					}
					// ok
					return int(threadID), [2]any{"set", action.Key}, action.Value, nil
				},
			)

		case ActionGet:
			if stopped[fz.NodeID(action.ClientID)] {
				return nil
			}
			return log(
				func() (int, any, any, error) {
					var res int
					ok, err := kvs[action.ClientID].Get(action.Key, &res, time.Minute*2)
					if err != nil {
						if errors.Is(err, raftstore.ErrTimeout) {
							// timeout
							return int(threadID), [2]any{"get", action.Key}, fz.KVResultTimeout, nil
						}
						// error
						return int(threadID), nil, nil, err
					}
					if !ok {
						// not found
						return int(threadID), [2]any{"get", action.Key}, fz.KVResultNotFound, nil
					}
					// ok
					return int(threadID), [2]any{"get", action.Key}, res, nil
				},
			)

		case ActionStopNode:
			stopped[action.NodeID] = true
			return closeNode(fz.NodeID(action.NodeID))

		default:
			panic(fmt.Errorf("unknown action: %#v", action))

		}

	}
}
