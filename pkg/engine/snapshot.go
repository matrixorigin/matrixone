// Copyright 2021 Matrix Origin
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

package engine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

func (s Snapshot) Snapshot() ([]byte, error) {
	return nil, nil
}

func (s Snapshot) ApplySnapshot(_ []byte) error {
	return nil
}

func (s Snapshot) Read(_ context.Context, _ []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (s Snapshot) Write(_ context.Context, _ []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (s Snapshot) WriteAndCommit(_ context.Context, _ []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (s Snapshot) Commit(_ context.Context) error {
	return nil
}

func (s Snapshot) Rollback(_ context.Context) error {
	return nil
}
