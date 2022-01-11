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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixcube/raftstore"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Do(
	newKV NewKV,
	nodes fz.Nodes,
	log LogPorcupineOp,
	closeNode fz.CloseNode,
	timeoutCounter TimeoutCounter,
) fz.Do {

	var kvs []*KV
	for i := 0; i < len(nodes); i++ {
		kvs = append(kvs, newKV(nodes[i].(*Node)))
	}
	stopped := make(map[fz.NodeID]bool)

	// see https://drive.google.com/file/d/1nPdvhB0PutEJzdCq5ms6UI58dp50fcAN/view, p70
	waitChan := make(chan chan struct{}, 1)
	waitChan <- nil
	wait := func() {
		c := <-waitChan
		waitChan <- c
		if c != nil {
			<-c
		}
	}
	lock := func() {
		c := <-waitChan
		if c == nil {
			c = make(chan struct{})
		}
		waitChan <- c
	}
	unlock := func() {
		c := <-waitChan
		if c != nil {
			close(c)
		}
		waitChan <- nil
	}

	return func(threadID int64, action fz.Action) error {

		switch action := action.(type) {

		case ActionSet:
			if stopped[fz.NodeID(action.ClientID)] {
				return nil
			}
			wait()
			return log(
				func() (int, any, any, error) {
					if err := kvs[action.ClientID].Set(action.Key, action.Value, time.Minute*10); err != nil {
						if errors.Is(err, raftstore.ErrTimeout) {
							// timeout
							atomic.AddInt64(timeoutCounter, 1)
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
			wait()
			return log(
				func() (int, any, any, error) {
					var res int
					ok, err := kvs[action.ClientID].Get(action.Key, &res, time.Minute*10)
					if err != nil {
						if errors.Is(err, raftstore.ErrTimeout) {
							// timeout
							atomic.AddInt64(timeoutCounter, 1)
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
			lock()
			defer unlock()
			stopped[action.NodeID] = true
			return closeNode(fz.NodeID(action.NodeID))

		default:
			panic(fmt.Errorf("unknown action: %#v", action))

		}

	}
}

type TimeoutCounter *int64

func (_ Def) TimeoutCounter() TimeoutCounter {
	var n int64
	return &n
}

func (_ Def) ReportTimeout(
	report fz.AddReport,
	counter TimeoutCounter,
) fz.Operators {
	return fz.Operators{
		{
			AfterDo: func() {
				if *counter >= 5 {
					report(fmt.Sprintf("too many timeout %d", *counter))
				}
			},
		},
	}
}
