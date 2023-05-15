// Copyright 2022 Matrix Origin
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

package ctl

import (
	"bytes"
	"context"
	fmt "fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// CtlResult ctl result
type CtlResult struct {
	Method string      `json:"method"`
	Data   interface{} `json:"result"`
}

// SetID implement morpc Messgae
func (m *Request) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Request) GetID() uint64 {
	return m.RequestID
}

func (m *Request) Method() uint32 {
	return uint32(m.CMDMethod)
}

func (m *Request) SetMethod(v uint32) {
	m.CMDMethod = CmdMethod(v)
}

// DebugString returns the debug string
func (m *Request) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.CMDMethod.String())
	buffer.WriteString("/")
	return buffer.String()
}

func (m *Request) WrapError(err error) {
	panic("not support")
}

func (m *Request) UnwrapError() error {
	panic("not support")
}

// SetID implement morpc Messgae
func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Response) GetID() uint64 {
	return m.RequestID
}

func (m *Response) WrapError(err error) {
	me := moerr.ConvertGoError(context.TODO(), err).(*moerr.Error)
	data, e := me.MarshalBinary()
	if e != nil {
		panic(e)
	}
	m.Error = data
}

// UnwrapError unwrap the moerr from the error bytes
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

// DebugString returns the debug string
func (m *Response) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.CMDMethod.String())
	buffer.WriteString("/")
	return buffer.String()
}

func (m *Response) Method() uint32 {
	return uint32(m.CMDMethod)
}

func (m *Response) SetMethod(v uint32) {
	m.CMDMethod = CmdMethod(v)
}
