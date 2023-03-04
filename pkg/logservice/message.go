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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// RPCRequest is request message type used in morpc
type RPCRequest struct {
	pb.Request
	payload []byte
	pool    *sync.Pool
}

var _ morpc.PayloadMessage = (*RPCRequest)(nil)

func (r *RPCRequest) Size() int {
	return r.Request.Size() + len(r.payload)
}

func (r *RPCRequest) Release() {
	if r.pool != nil {
		r.Request = pb.Request{}
		r.payload = nil
		r.pool.Put(r)
	}
}

func (r *RPCRequest) SetID(id uint64) {
	r.RequestID = id
}

func (r *RPCRequest) GetID() uint64 {
	return r.RequestID
}

func (r *RPCRequest) DebugString() string {
	return fmt.Sprintf("%s:%p", r.Request.Method.String(), r.payload)
}

func (r *RPCRequest) GetPayloadField() []byte {
	return r.payload
}

func (r *RPCRequest) SetPayloadField(data []byte) {
	r.payload = data
}

// RPCResponse is response message type used in morpc
type RPCResponse struct {
	pb.Response
	payload []byte
	pool    *sync.Pool
}

var _ morpc.PayloadMessage = (*RPCResponse)(nil)

func (r *RPCResponse) Release() {
	if r.pool != nil {
		r.Response = pb.Response{}
		r.payload = nil
		r.pool.Put(r)
	}
}

func (r *RPCResponse) SetID(id uint64) {
	r.RequestID = id
}

func (r *RPCResponse) GetID() uint64 {
	return r.RequestID
}

func (r *RPCResponse) DebugString() string {
	return r.Response.Method.String()
}

func (r *RPCResponse) GetPayloadField() []byte {
	return r.payload
}

func (r *RPCResponse) SetPayloadField(data []byte) {
	r.payload = data
}
