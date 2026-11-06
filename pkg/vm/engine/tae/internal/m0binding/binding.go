// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package m0binding carries a TN-local ADD binding between the transaction
// and catalog lock owners. It is never serialized or sent over RPC.
package m0binding

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Add interface {
	Validate(uint64, uint64, txnif.TxnReader, any) error
	Hydrate(uint64, uint64, txnif.TxnReader, any) error
}

type addKey struct{}

func WithAdd(ctx context.Context, binding Add) context.Context {
	return context.WithValue(ctx, addKey{}, binding)
}

func AddFrom(ctx context.Context) Add {
	if ctx == nil {
		return nil
	}
	binding, _ := ctx.Value(addKey{}).(Add)
	return binding
}
