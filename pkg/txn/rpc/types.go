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

package rpc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnSender is used to send transaction requests to the DN nodes.
type TxnSender interface {
	// Send send request to the specified DN node, and wait for response synchronously.
	// For any reason, if no response is received, the internal will keep retrying until
	// the Context times out.
	Send(context.Context, []txn.TxnRequest) ([]txn.TxnResponse, error)
	// Close the txn sender
	Close() error
}

// TxnServer receives and processes txn requests from TxnSender.
type TxnServer interface {
	// Start start the txn server
	Start() error
	// Close the txn server
	Close() error
	// RegisterMethodHandler register txn request handler func
	RegisterMethodHandler(txn.TxnMethod, TxnRequestHandleFunc)
	// SetFilter set filter func. Requests can be modified or filtered out by the filter
	// before they are processed by the handler.
	SetFilter(func(*txn.TxnRequest) bool)
}

// TxnRequestHandleFunc txn request handle func
type TxnRequestHandleFunc func(context.Context, *txn.TxnRequest, *txn.TxnResponse) error

// SenderOption option for create Sender
type SenderOption func(*sender)
