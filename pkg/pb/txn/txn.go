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
