// Copyright 2023 Matrix Origin
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

package lock

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// SetID implement morpc Messgae
func (m *Request) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Request) GetID() uint64 {
	return m.RequestID
}

// DebugString returns the debug string
func (m *Request) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.Method.String())
	buffer.WriteString("/")
	if m.LockTable.Table > 0 {
		buffer.WriteString(m.LockTable.DebugString())
		buffer.WriteString("/")
	}
	switch m.Method {
	case Method_Lock:
		buffer.WriteString(m.Lock.DebugString())
	case Method_Unlock:
		buffer.WriteString(m.Unlock.DebugString())
	case Method_GetBind:
		buffer.WriteString(m.GetBind.DebugString())
	case Method_GetTxnLock:
		buffer.WriteString(m.GetTxnLock.DebugString())
	case Method_GetWaitingList:
		buffer.WriteString(m.GetWaitingList.DebugString())
	case Method_KeepLockTableBind:
		buffer.WriteString(m.KeepLockTableBind.DebugString())
	case Method_KeepRemoteLock:
		buffer.WriteString(m.KeepRemoteLock.DebugString())
	}
	return buffer.String()
}

// SetID implement morpc Messgae
func (m *Response) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *Response) GetID() uint64 {
	return m.RequestID
}

// DebugString returns the debug string
func (m *Response) DebugString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%d: ", m.RequestID))
	buffer.WriteString(m.Method.String())
	buffer.WriteString("/")
	switch m.Method {
	case Method_Lock:
		buffer.WriteString(m.Lock.DebugString())
	case Method_Unlock:
		buffer.WriteString(m.Unlock.DebugString())
	case Method_GetBind:
		buffer.WriteString(m.GetBind.DebugString())
	case Method_GetTxnLock:
		buffer.WriteString(m.GetTxnLock.DebugString())
	case Method_GetWaitingList:
		buffer.WriteString(m.GetWaitingList.DebugString())
	case Method_KeepLockTableBind:
		buffer.WriteString(m.KeepLockTableBind.DebugString())
	case Method_KeepRemoteLock:
		buffer.WriteString(m.KeepRemoteLock.DebugString())
	}
	return buffer.String()
}

// Changed returns true if LockTable bind changed
func (m LockTable) Changed(v LockTable) bool {
	return m.Version != v.Version ||
		m.ServiceID != v.ServiceID
}

// DebugString returns the debug string
func (m LockTable) DebugString() string {
	return fmt.Sprintf("%d-%s-%d", m.Table, m.ServiceID, m.Version)
}

// WithGranularity set rows granularity, the default granularity is Row.
func (m LockOptions) WithGranularity(granularity Granularity) LockOptions {
	m.Granularity = granularity
	return m
}

// WithMode set lock mode, the default mode is Exclusive.
func (m LockOptions) WithMode(mode LockMode) LockOptions {
	m.Mode = mode
	return m
}

// WithWaitPolicy set wait policy, the default policy is Wait.
func (m LockOptions) WithWaitPolicy(policy WaitPolicy) LockOptions {
	m.Policy = policy
	return m
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

// DebugString debug string
func (m LockOptions) DebugString() string {
	return fmt.Sprintf("%s-%s-%s",
		m.Mode.String(),
		m.Granularity.String(),
		m.Policy.String())
}

func (m *LockRequest) DebugString() string {
	return fmt.Sprintf("%s-%s-%s-%s",
		hex.EncodeToString(m.TxnID),
		m.ServiceID,
		bytesArrayString(m.Rows),
		m.Options.DebugString())
}

func (m *LockResponse) DebugString() string {
	return ""
}

func (m *UnlockRequest) DebugString() string {
	return hex.EncodeToString(m.TxnID)
}

func (m *UnlockResponse) DebugString() string {
	return ""
}

func (m *GetBindRequest) DebugString() string {
	return fmt.Sprintf("%s-%d", m.ServiceID, m.Table)
}

func (m *GetBindResponse) DebugString() string {
	return m.LockTable.DebugString()
}

func (m *GetTxnLockRequest) DebugString() string {
	return fmt.Sprintf("%s-%s",
		hex.EncodeToString(m.TxnID),
		hex.EncodeToString(m.Row))
}

func (m *GetTxnLockResponse) DebugString() string {
	return fmt.Sprintf("%d-%s",
		m.Value,
		bytesArrayString(m.WaitingList))
}

func (m *GetWaitingListRequest) DebugString() string {
	return m.Txn.DebugString()
}

func (m *GetWaitingListResponse) DebugString() string {
	return waitTxnArrayString(m.WaitingList)
}

func (m *KeepLockTableBindRequest) DebugString() string {
	return m.ServiceID
}

func (m *KeepLockTableBindResponse) DebugString() string {
	if m.OK {
		return "true"
	}
	return "false"
}

func (m *KeepRemoteLockRequest) DebugString() string {
	return m.ServiceID
}

func (m *KeepRemoteLockResponse) DebugString() string {
	if m.OK {
		return "true"
	}
	return "false"
}

func bytesArrayString(values [][]byte) string {
	var buffer bytes.Buffer
	for idx, v := range values {
		buffer.WriteString(hex.EncodeToString(v))
		if idx != len(values)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

func waitTxnArrayString(values []WaitTxn) string {
	var buffer bytes.Buffer
	for idx, v := range values {
		buffer.WriteString(v.String())
		if idx != len(values)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

func (m *WaitTxn) DebugString() string {
	return fmt.Sprintf("%s(%s)",
		hex.EncodeToString(m.TxnID),
		m.CreatedOn)
}
