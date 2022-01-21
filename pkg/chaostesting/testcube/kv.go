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
	"errors"
	"runtime/trace"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/uuid"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/sb"
)

type KV struct {
	Set func(key any, value any, timeout time.Duration) error
	Get func(key any, target any, timeout time.Duration) (bool, error)
}

const (
	OpSet = iota + 1
	OpGet
)

type NewKV func(node *Node) *KV

func (_ Def) NewKV(
	wt fz.RootWaitTree,
) NewKV {

	return func(node *Node) *KV {

		type ReqInfo struct {
			Req    rpc.Request
			Result chan []byte
			Error  chan error
		}

		reqInfosLock := new(sync.Mutex)
		var reqInfos map[uuid.UUID]*ReqInfo
		var proxy raftstore.ShardsProxy

		// after node restarted, shard proxy need to be updated
		// this function must be called with reqInfosLock being held
		check := func() {
			curProxy := node.RaftStore.GetShardsProxy()
			if curProxy == proxy {
				return
			}
			proxy = curProxy
			reqInfos = make(map[uuid.UUID]*ReqInfo)

			proxy.SetCallback(

				func(resp rpc.Response) {
					reqInfosLock.Lock()
					defer reqInfosLock.Unlock()
					info, ok := reqInfos[uuid.UUID(*(*[16]byte)(resp.ID))]
					if !ok {
						return
					}
					info.Result <- resp.Value
				},

				func(reqID []byte, err error) {
					reqInfosLock.Lock()
					defer reqInfosLock.Unlock()
					info, ok := reqInfos[uuid.UUID(*(*[16]byte)(reqID))]
					if !ok {
						return
					}
					info.Error <- we(err)
				},
			)

			proxy.SetRetryController(RetryFunc(func(id []byte) (req rpc.Request, retry bool) {
				reqInfosLock.Lock()
				defer reqInfosLock.Unlock()
				info, ok := reqInfos[uuid.UUID(*(*[16]byte)(id))]
				if !ok {
					return
				}
				req = info.Req
				retry = true
				return
			}))
		}

		return &KV{

			Set: func(key any, value any, timeout time.Duration) (err error) {
				defer he(&err)

				ctx, task := trace.NewTask(wt.Ctx, "kv set")
				defer task.End()
				trace.Logf(ctx, "kv", "set %v -> %v", key, value)

				req := rpc.Request{}
				id := uuid.NewV4()
				req.ID = id.Bytes()
				req.CustomType = OpSet
				req.Type = rpc.CmdType_Write
				keyBuf := new(bytes.Buffer)
				ce(sb.Copy(sb.Marshal(key), sb.Encode(keyBuf)))
				req.Key = keyBuf.Bytes()
				valueBuf := new(bytes.Buffer)
				ce(sb.Copy(sb.Marshal(value), sb.Encode(valueBuf)))
				req.Cmd = valueBuf.Bytes()

				reqInfosLock.Lock()
				check()
				resultChan := make(chan []byte, 1)
				errChan := make(chan error, 1)
				reqInfos[id] = &ReqInfo{
					Result: resultChan,
					Error:  errChan,
				}
				reqInfosLock.Unlock()
				defer func() {
					reqInfosLock.Lock()
					delete(reqInfos, id)
					reqInfosLock.Unlock()
				}()

				for {
					req.StopAt = time.Now().Add(timeout).Unix()
					err = proxy.Dispatch(req)
					if err != nil {
						var tryAgain *raftstore.ErrTryAgain
						if errors.As(err, &tryAgain) {
							time.Sleep(tryAgain.Wait)
							continue
						}
						ce(err)
					}
					break
				}

				select {
				case result := <-resultChan:
					_ = result
					return nil
				case err := <-errChan:
					return we(err)
				case <-time.After(timeout):
					return we(raftstore.ErrTimeout)
				}

			},

			Get: func(key any, target any, timeout time.Duration) (ok bool, err error) {
				defer he(&err)

				ctx, task := trace.NewTask(wt.Ctx, "kv get")
				defer task.End()
				trace.Logf(ctx, "kv", "get %v", key)

				req := rpc.Request{}
				id := uuid.NewV4()
				req.ID = id.Bytes()
				req.CustomType = OpGet
				req.Type = rpc.CmdType_Read
				keyBuf := new(bytes.Buffer)
				ce(sb.Copy(sb.Marshal(key), sb.Encode(keyBuf)))
				req.Key = keyBuf.Bytes()

				reqInfosLock.Lock()
				check()
				resultChan := make(chan []byte, 1)
				errChan := make(chan error, 1)
				reqInfos[id] = &ReqInfo{
					Result: resultChan,
					Error:  errChan,
				}
				reqInfosLock.Unlock()
				defer func() {
					reqInfosLock.Lock()
					delete(reqInfos, id)
					reqInfosLock.Unlock()
				}()

				for {
					req.StopAt = time.Now().Add(timeout).Unix()
					err = proxy.Dispatch(req)
					if err != nil {
						var tryAgain *raftstore.ErrTryAgain
						if errors.As(err, &tryAgain) {
							time.Sleep(tryAgain.Wait)
							continue
						}
						ce(err)
					}
					break
				}

				select {
				case result := <-resultChan:
					if len(result) > 0 {
						ce(sb.Copy(
							sb.Decode(bytes.NewReader(result)),
							sb.Unmarshal(target),
						))
						return true, nil
					}
					return false, nil
				case err := <-errChan:
					return false, we(err)
				case <-time.After(timeout):
					return false, we(raftstore.ErrTimeout)
				}

			},
		}

	}

}

type RetryFunc func(id []byte) (rpc.Request, bool)

var _ raftstore.RetryController = RetryFunc(nil)

func (r RetryFunc) Retry(reqID []byte) (rpc.Request, bool) {
	return r(reqID)
}
