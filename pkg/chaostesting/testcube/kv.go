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
	"bytes"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"github.com/reusee/sb"
)

type KV struct {
	Set func(key any, value any, timeout time.Duration) error
	Get func(key any, target any, timeout time.Duration) error
}

type KVs = []*KV

const (
	OpGet = iota
	OpSet
)

func (_ Def) KVs(
	nodes Nodes,
) (kvs KVs) {

	for _, node := range nodes {

		type ReqInfo struct {
			Result chan []byte
			Error  chan error
		}
		reqInfos := make(map[string]*ReqInfo)
		reqInfosLock := new(sync.Mutex)

		shardsProxy := node.RaftStore.GetShardsProxy()
		shardsProxy.SetCallback(

			func(resp rpc.Response) {
				reqInfosLock.Lock()
				defer reqInfosLock.Unlock()
				defer func() {
					delete(reqInfos, string(resp.ID))
				}()
				info, ok := reqInfos[string(resp.ID)]
				if !ok {
					return
				}
				info.Result <- resp.Value
			},

			func(reqID []byte, err error) {
				reqInfosLock.Lock()
				defer reqInfosLock.Unlock()
				defer func() {
					delete(reqInfos, string(reqID))
				}()
				info, ok := reqInfos[string(reqID)]
				if !ok {
					return
				}
				info.Error <- err
			},
		)
		shardsProxy.SetRetryController(nil) //TODO

		kvs = append(kvs, &KV{

			Set: func(key any, value any, timeout time.Duration) (err error) {
				defer he(&err)

				req := rpc.Request{}
				req.ID = uuid.NewV4().Bytes()
				req.CustomType = OpSet
				req.Type = rpc.CmdType_Write
				keyBuf := new(bytes.Buffer)
				ce(sb.Copy(sb.Marshal(key), sb.Encode(keyBuf)))
				req.Key = keyBuf.Bytes()
				valueBuf := new(bytes.Buffer)
				ce(sb.Copy(sb.Marshal(value), sb.Encode(valueBuf)))
				req.Cmd = valueBuf.Bytes()
				req.StopAt = time.Now().Add(timeout).Unix()

				reqInfosLock.Lock()
				resultChan := make(chan []byte, 1)
				errChan := make(chan error, 1)
				reqInfos[string(req.ID)] = &ReqInfo{
					Result: resultChan,
					Error:  errChan,
				}
				reqInfosLock.Unlock()

				ce(node.RaftStore.GetShardsProxy().Dispatch(req))

				select {
				case result := <-resultChan:
					_ = result
					return nil
				case err := <-errChan:
					return err
				case <-time.After(timeout):
					return raftstore.ErrTimeout
				}

			},

			Get: func(key any, target any, timeout time.Duration) (err error) {
				defer he(&err)

				req := rpc.Request{}
				req.ID = uuid.NewV4().Bytes()
				req.CustomType = OpGet
				req.Type = rpc.CmdType_Read
				keyBuf := new(bytes.Buffer)
				ce(sb.Copy(sb.Marshal(key), sb.Encode(keyBuf)))
				req.Key = keyBuf.Bytes()
				req.StopAt = time.Now().Add(timeout).Unix()

				reqInfosLock.Lock()
				resultChan := make(chan []byte, 1)
				errChan := make(chan error, 1)
				reqInfos[string(req.ID)] = &ReqInfo{
					Result: resultChan,
					Error:  errChan,
				}
				reqInfosLock.Unlock()

				select {
				case result := <-resultChan:
					ce(sb.Copy(
						sb.Decode(bytes.NewReader(result)),
						sb.Unmarshal(target),
					))
					return nil
				case err := <-errChan:
					return err
				case <-time.After(timeout):
					return raftstore.ErrTimeout
				}

			},
		})
	}

	return
}
