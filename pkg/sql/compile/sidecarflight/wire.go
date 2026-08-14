// Copyright 2026 Matrix Origin
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

// Package sidecarflight contains the deliberately small Arrow Flight wire
// client used by MatrixOne's Sirius offload path. These protobuf definitions
// mirror the Apache Arrow Flight field numbers, but avoid importing an Arrow
// implementation into MatrixOne.
package sidecarflight

import "fmt"

type flightAction struct {
	Type string `protobuf:"bytes,1,opt,name=type,proto3"`
	Body []byte `protobuf:"bytes,2,opt,name=body,proto3"`
}

func (m *flightAction) Reset() { *m = flightAction{} }
func (m *flightAction) String() string {
	return fmt.Sprintf("FlightAction{%q,%d bytes}", m.Type, len(m.Body))
}
func (*flightAction) ProtoMessage() {}

type flightResult struct {
	Body []byte `protobuf:"bytes,1,opt,name=body,proto3"`
}

func (m *flightResult) Reset()         { *m = flightResult{} }
func (m *flightResult) String() string { return fmt.Sprintf("FlightResult{%d bytes}", len(m.Body)) }
func (*flightResult) ProtoMessage()    {}

type flightDescriptor struct {
	Type int32    `protobuf:"varint,1,opt,name=type,proto3"`
	Cmd  []byte   `protobuf:"bytes,2,opt,name=cmd,proto3"`
	Path []string `protobuf:"bytes,3,rep,name=path,proto3"`
}

func (m *flightDescriptor) Reset() { *m = flightDescriptor{} }
func (m *flightDescriptor) String() string {
	return fmt.Sprintf("FlightDescriptor{%d,%d bytes}", m.Type, len(m.Cmd))
}
func (*flightDescriptor) ProtoMessage() {}

type flightTicket struct {
	Ticket []byte `protobuf:"bytes,1,opt,name=ticket,proto3"`
}

func (m *flightTicket) Reset()         { *m = flightTicket{} }
func (m *flightTicket) String() string { return fmt.Sprintf("FlightTicket{%d bytes}", len(m.Ticket)) }
func (*flightTicket) ProtoMessage()    {}

type flightLocation struct {
	URI string `protobuf:"bytes,1,opt,name=uri,proto3"`
}

func (m *flightLocation) Reset()         { *m = flightLocation{} }
func (m *flightLocation) String() string { return fmt.Sprintf("FlightLocation{%q}", m.URI) }
func (*flightLocation) ProtoMessage()    {}

type flightEndpoint struct {
	Ticket      *flightTicket     `protobuf:"bytes,1,opt,name=ticket,proto3"`
	Locations   []*flightLocation `protobuf:"bytes,2,rep,name=location,proto3"`
	AppMetadata []byte            `protobuf:"bytes,4,opt,name=app_metadata,json=appMetadata,proto3"`
}

func (m *flightEndpoint) Reset()         { *m = flightEndpoint{} }
func (m *flightEndpoint) String() string { return "FlightEndpoint" }
func (*flightEndpoint) ProtoMessage()    {}

type flightInfo struct {
	Schema       []byte            `protobuf:"bytes,1,opt,name=schema,proto3"`
	Descriptor   *flightDescriptor `protobuf:"bytes,2,opt,name=flight_descriptor,json=flightDescriptor,proto3"`
	Endpoint     []*flightEndpoint `protobuf:"bytes,3,rep,name=endpoint,proto3"`
	TotalRecords int64             `protobuf:"varint,4,opt,name=total_records,json=totalRecords,proto3"`
	TotalBytes   int64             `protobuf:"varint,5,opt,name=total_bytes,json=totalBytes,proto3"`
	Ordered      bool              `protobuf:"varint,6,opt,name=ordered,proto3"`
	AppMetadata  []byte            `protobuf:"bytes,7,opt,name=app_metadata,json=appMetadata,proto3"`
}

func (m *flightInfo) Reset()         { *m = flightInfo{} }
func (m *flightInfo) String() string { return fmt.Sprintf("FlightInfo{%d endpoints}", len(m.Endpoint)) }
func (*flightInfo) ProtoMessage()    {}

type flightData struct {
	Descriptor  *flightDescriptor `protobuf:"bytes,1,opt,name=flight_descriptor,json=flightDescriptor,proto3"`
	DataHeader  []byte            `protobuf:"bytes,2,opt,name=data_header,json=dataHeader,proto3"`
	AppMetadata []byte            `protobuf:"bytes,3,opt,name=app_metadata,json=appMetadata,proto3"`
	DataBody    []byte            `protobuf:"bytes,1000,opt,name=data_body,json=dataBody,proto3"`
}

func (m *flightData) Reset() { *m = flightData{} }
func (m *flightData) String() string {
	return fmt.Sprintf("FlightData{%d,%d bytes}", len(m.DataHeader), len(m.DataBody))
}
func (*flightData) ProtoMessage() {}

type executeSubstraitRequest struct {
	ProtocolVersion  uint32 `protobuf:"varint,1,opt,name=protocol_version,json=protocolVersion,proto3"`
	SubstraitVersion string `protobuf:"bytes,2,opt,name=substrait_version,json=substraitVersion,proto3"`
	CapabilityHash   []byte `protobuf:"bytes,3,opt,name=capability_hash,json=capabilityHash,proto3"`
	MaxBatchBytes    uint64 `protobuf:"varint,4,opt,name=max_batch_bytes,json=maxBatchBytes,proto3"`
	DeadlineUnixMS   uint64 `protobuf:"varint,5,opt,name=deadline_unix_ms,json=deadlineUnixMs,proto3"`
	Plan             []byte `protobuf:"bytes,6,opt,name=plan,proto3"`
	QueryID          []byte `protobuf:"bytes,7,opt,name=query_id,json=queryId,proto3"`
	IdempotencyKey   []byte `protobuf:"bytes,8,opt,name=idempotency_key,json=idempotencyKey,proto3"`
	AccountID        uint64 `protobuf:"varint,9,opt,name=account_id,json=accountId,proto3"`
}

func (m *executeSubstraitRequest) Reset() { *m = executeSubstraitRequest{} }
func (m *executeSubstraitRequest) String() string {
	return fmt.Sprintf("ExecuteSubstraitRequest{%d bytes}", len(m.Plan))
}
func (*executeSubstraitRequest) ProtoMessage() {}

type cancelExecutionRequest struct {
	Ticket         []byte `protobuf:"bytes,1,opt,name=ticket,proto3"`
	IdempotencyKey []byte `protobuf:"bytes,2,opt,name=idempotency_key,json=idempotencyKey,proto3"`
}

func (m *cancelExecutionRequest) Reset() { *m = cancelExecutionRequest{} }
func (m *cancelExecutionRequest) String() string {
	return fmt.Sprintf("CancelExecutionRequest{%d,%d bytes}", len(m.Ticket), len(m.IdempotencyKey))
}
func (*cancelExecutionRequest) ProtoMessage() {}
