// Copyright 2021 - 2023 Matrix Origin
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

package cache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type CacheResponse struct {
	Response
	ReleaseFunc func()
}

func (r *CacheResponse) Reset() {
	if r.ReleaseFunc != nil {
		r.ReleaseFunc()
	}
	r.Response.Reset()
}

// SetID implements the morpc.Message interface.
func (m *Request) SetID(id uint64) {
	m.RequestID = id
}

// GetID implements the morpc.Message interface.
func (m *Request) GetID() uint64 {
	return m.RequestID
}

// Method implements the morpc.MethodBasedMessage interface.
func (m *Request) Method() uint32 {
	return uint32(m.CmdMethod)
}

// SetMethod implements the morpc.MethodBasedMessage interface.
func (m *Request) SetMethod(v uint32) {
	m.CmdMethod = CmdMethod(v)
}

// DebugString implements the morpc.Message interface.
func (m *Request) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.CmdMethod.String())
	buffer.WriteString("/")
	return buffer.String()
}

// WrapError implements the morpc.MethodBasedMessage interface.
func (m *Request) WrapError(err error) {
	panic("not supported")
}

// UnwrapError implements the morpc.MethodBasedMessage interface.
func (m *Request) UnwrapError() error {
	panic("not supported")
}

// SetID implements the morpc.Message interface.
func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implements the morpc.Message interface.
func (m *Response) GetID() uint64 {
	return m.RequestID
}

// Method implements the morpc.MethodBasedMessage interface.
func (m *Response) Method() uint32 {
	return uint32(m.CmdMethod)
}

// SetMethod implements the morpc.MethodBasedMessage interface.
func (m *Response) SetMethod(v uint32) {
	m.CmdMethod = CmdMethod(v)
}

// DebugString implements the morpc.Message interface.
func (m *Response) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.CmdMethod.String())
	buffer.WriteString("/")
	return buffer.String()
}

// WrapError implements the morpc.MethodBasedMessage interface.
func (m *Response) WrapError(err error) {
	me := moerr.ConvertGoError(context.TODO(), err).(*moerr.Error)
	data, e := me.MarshalBinary()
	if e != nil {
		panic(e)
	}
	m.Error = data
}

// UnwrapError implements the morpc.MethodBasedMessage interface.
func (m *Response) UnwrapError() error {
	if len(m.Error) == 0 {
		return nil
	}
	err := &moerr.Error{}
	if e := err.UnmarshalBinary(m.Error); e != nil {
		panic(e)
	}
	return err
}
