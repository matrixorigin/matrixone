// Copyright 2021-2024 Matrix Origin
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

package shard

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func (m Request) TypeName() string {
	return "pb.shard.Request"
}

func (m Response) TypeName() string {
	return "pb.shard.Response"
}

// SetID implement morpc Message
func (m *Request) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Message
func (m *Request) GetID() uint64 {
	return m.RequestID
}

// Method implement morpc Message
func (m *Request) Method() uint32 {
	return uint32(m.RPCMethod)
}

// SetMethod implement morpc Message
func (m *Request) SetMethod(v uint32) {
	m.RPCMethod = Method(v)
}

// DebugString returns the debug string
func (m *Request) DebugString() string {
	return ""
}

// WrapError wrapper error to TxnError
func (m *Request) WrapError(err error) {

}

// UnwrapError unwrap the moerr from the error bytes
func (m Request) UnwrapError() error {
	return nil
}

// SetID implement morpc Message
func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Message
func (m *Response) GetID() uint64 {
	return m.RequestID
}

// Method implement morpc Message
func (m *Response) Method() uint32 {
	return uint32(m.RPCMethod)
}

// SetMethod implement morpc Message
func (m *Response) SetMethod(v uint32) {
	m.RPCMethod = Method(v)
}

// DebugString returns the debug string
func (m *Response) DebugString() string {
	return ""
}

// WrapError wrapper error to TxnError
func (m *Response) WrapError(err error) {
	me := moerr.ConvertGoError(context.TODO(), err).(*moerr.Error)
	data, e := me.MarshalBinary()
	if e != nil {
		panic(e)
	}
	m.Error = data
}

// UnwrapError unwrap the moerr from the error bytes
func (m Response) UnwrapError() error {
	if len(m.Error) == 0 {
		return nil
	}

	err := &moerr.Error{}
	if e := err.UnmarshalBinary(m.Error); e != nil {
		panic(e)
	}
	return err
}
