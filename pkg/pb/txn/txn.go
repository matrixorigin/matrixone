// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
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

	buffer.WriteString("txn-meta: <")
	buffer.WriteString(m.Txn.DebugString())
	buffer.WriteString(">, ")

	buffer.WriteString("method: ")
	buffer.WriteString(m.Method.String())
	buffer.WriteString(", ")

	buffer.WriteString("flag: ")
	buffer.WriteString(fmt.Sprintf("%d", m.Flag))
	buffer.WriteString(", ")

	if m.CNRequest != nil {
		buffer.WriteString("cn-request: <")
		buffer.WriteString(m.CNRequest.DebugString())
		buffer.WriteString(">, ")
	}

	return buffer.String()
}

// DebugString returns debug string
func (m TxnError) DebugString() string {
	return fmt.Sprintf("%s: %s", m.Code.String(), m.Message)
}

// DebugString returns debug string
func (m TxnResponse) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString("txn-meta: <")
	buffer.WriteString(m.Txn.DebugString())
	buffer.WriteString(">, ")

	buffer.WriteString("method: ")
	buffer.WriteString(m.Method.String())
	buffer.WriteString(", ")

	buffer.WriteString("flag: ")
	buffer.WriteString(fmt.Sprintf("%d", m.Flag))
	buffer.WriteString(", ")

	if m.TxnError != nil {
		buffer.WriteString("error: <")
		buffer.WriteString(m.TxnError.DebugString())
		buffer.WriteString(">, ")
	}

	if m.CNOpResponse != nil {
		buffer.WriteString("cn-response: <")
		buffer.WriteString(m.CNOpResponse.DebugString())
		buffer.WriteString(">, ")
	}

	return buffer.String()
}

// DebugString returns debug string
func (m CNOpRequest) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString("op: ")
	buffer.WriteString(fmt.Sprintf("%d", m.OpCode))
	buffer.WriteString(", ")

	buffer.WriteString("payload: ")
	buffer.WriteString(fmt.Sprintf("%d bytes", m.Payload))
	buffer.WriteString(", ")

	buffer.WriteString("dn: <")
	buffer.WriteString(m.Target.DebugString())
	buffer.WriteString(">")
	return buffer.String()
}

// DebugString returns debug string
func (m CNOpResponse) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString("payload: ")
	buffer.WriteString(fmt.Sprintf("%d bytes", m.Payload))
	return buffer.String()
}

// DebugString returns debug string
func (m TxnMeta) DebugString() string {
	var buffer bytes.Buffer

	buffer.WriteString("txn-id: ")
	buffer.WriteString(hex.EncodeToString(m.ID))
	buffer.WriteString(", ")

	buffer.WriteString("status: ")
	buffer.WriteString(m.Status.String())
	buffer.WriteString(", ")

	buffer.WriteString("snapshot-ts: ")
	buffer.WriteString(m.SnapshotTS.String())
	buffer.WriteString(", ")

	if !m.PreparedTS.IsEmpty() {
		buffer.WriteString("prepared-ts: ")
		buffer.WriteString(m.PreparedTS.String())
		buffer.WriteString(", ")
	}

	if !m.CommitTS.IsEmpty() {
		buffer.WriteString("commit-ts: ")
		buffer.WriteString(m.CommitTS.String())
		buffer.WriteString(", ")
	}
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
