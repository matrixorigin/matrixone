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

package txn

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const (
	// SkipResponseFlag skip response.
	SkipResponseFlag uint32 = 1
)

// NewTxnRequest create TxnRequest by CNOpRequest
func NewTxnRequest(request *CNOpRequest) TxnRequest {
	return TxnRequest{CNRequest: request}
}

// GetCNOpResponse returns the CNOpResponse from TxnResponse
func GetCNOpResponse(response TxnResponse) CNOpResponse {
	return *response.CNOpResponse
}

// HasFlag returns true if has the spec flag
func (m TxnResponse) HasFlag(flag uint32) bool {
	return m.Flag&flag > 0
}

// DebugString returns debug string
func (m TxnRequest) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString("<")
	buffer.WriteString(m.Txn.DebugString())
	buffer.WriteString(">/")

	buffer.WriteString(m.Method.String())
	buffer.WriteString("/")
	buffer.WriteString(fmt.Sprintf("F-%d", m.Flag))

	if m.CNRequest != nil {
		buffer.WriteString("/<")
		buffer.WriteString(m.CNRequest.DebugString())
		buffer.WriteString(">")
	}
	buffer.WriteString("/=><")
	buffer.WriteString(m.GetTargetDN().DebugString())
	buffer.WriteString(">")
	return buffer.String()
}

// DebugString returns debug string
func (m TxnError) DebugString() string {
	return fmt.Sprintf("%s-%s", m.Code.String(), m.Message)
}

// DebugString returns debug string
func (m TxnResponse) DebugString() string {
	var buffer bytes.Buffer

	if m.Txn != nil {
		buffer.WriteString("<")
		buffer.WriteString(m.Txn.DebugString())
		buffer.WriteString(">/")
	}

	buffer.WriteString(m.Method.String())
	buffer.WriteString("/")
	buffer.WriteString(fmt.Sprintf("F:%d", m.Flag))

	if m.TxnError != nil {
		buffer.WriteString("/")
		buffer.WriteString(m.TxnError.DebugString())
	}

	if m.CNOpResponse != nil {
		buffer.WriteString("/")
		buffer.WriteString(m.CNOpResponse.DebugString())
	}

	return buffer.String()
}

// DebugString returns debug string
func (m CNOpRequest) DebugString() string {
	return fmt.Sprintf("O:%d-D:%d", m.OpCode, len(m.Payload))
}

// DebugString returns debug string
func (m CNOpResponse) DebugString() string {
	return fmt.Sprintf("D:%d", len(m.Payload))
}

// DebugString returns debug string
func (m TxnMeta) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString(hex.EncodeToString(m.ID))
	buffer.WriteString("/")
	buffer.WriteString(m.Status.String())
	buffer.WriteString("/S:")
	buffer.WriteString(m.SnapshotTS.DebugString())

	if !m.PreparedTS.IsEmpty() {
		buffer.WriteString("/P:")
		buffer.WriteString(m.PreparedTS.DebugString())
	}

	if !m.CommitTS.IsEmpty() {
		buffer.WriteString("/C:")
		buffer.WriteString(m.CommitTS.DebugString())
	}

	n := len(m.DNShards)
	var buf bytes.Buffer
	buf.WriteString("/<")
	for idx, dn := range m.DNShards {
		buf.WriteString(dn.DebugString())
		if idx < n-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString(">")
	return buffer.String()
}

// GetTargetDN return dn shard ID that message need send to.
func (m TxnRequest) GetTargetDN() metadata.DNShard {
	switch m.Method {
	case TxnMethod_Read, TxnMethod_Write:
		return m.CNRequest.Target
	case TxnMethod_Commit:
		return m.Txn.DNShards[0]
	case TxnMethod_Rollback:
		return m.Txn.DNShards[0]
	case TxnMethod_Prepare:
		return m.PrepareRequest.DNShard
	case TxnMethod_GetStatus:
		return m.GetStatusRequest.DNShard
	case TxnMethod_CommitDNShard:
		return m.CommitDNShardRequest.DNShard
	case TxnMethod_RollbackDNShard:
		return m.RollbackDNShardRequest.DNShard
	default:
		panic("unknown txn request method")
	}
}

// SetID implement morpc Messgae
func (m *TxnRequest) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *TxnRequest) GetID() uint64 {
	return m.RequestID
}

// GetPayloadField implement morpc PayloadMessgae
func (m TxnRequest) GetPayloadField() []byte {
	if m.CNRequest != nil {
		return m.CNRequest.Payload
	}
	return nil
}

// SetPayloadField implement morpc PayloadMessgae
func (m *TxnRequest) SetPayloadField(data []byte) {
	if m.CNRequest != nil {
		m.CNRequest.Payload = data
	}
}

// SetID implement morpc Messgae
func (m *TxnResponse) SetID(id uint64) {
	m.RequestID = id
}

// GetID implement morpc Messgae
func (m *TxnResponse) GetID() uint64 {
	return m.RequestID
}

// RequestsDebugString returns requests debug string
func RequestsDebugString(requests []TxnRequest) string {
	n := len(requests)
	var buf bytes.Buffer
	buf.WriteString("[")
	for idx, req := range requests {
		buf.WriteString(req.DebugString())
		if idx < n-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString("]")
	return buf.String()
}

// ResponsesDebugString returns responses debug string
func ResponsesDebugString(responses []TxnResponse) string {
	n := len(responses)
	var buf bytes.Buffer
	buf.WriteString("[")
	for idx, resp := range responses {
		buf.WriteString(resp.DebugString())
		if idx < n-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString("]")
	return buf.String()
}
