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

package catalog

import (
	// "bytes"
	// "encoding/binary"
	"errors"
	// "fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type UpdateNodeIf interface {
	UpdateNode(UpdateNodeIf)
	GetTxn() txnif.TxnReader
	GetEnd() types.TS
	GetStart() types.TS
	GetLogIndex() []*wal.Index
	String() string
	AddLogIndex(*wal.Index)

	HasDropped() bool

	IsSameTxn(types.TS) bool
	IsActive() bool
	IsCommitting() bool

	PrepareCommit() error
	Prepare2PCPrepare() error
	ApplyCommit(index *wal.Index) error
	ApplyRollback(index *wal.Index) error
	ReadFrom(io.Reader) (int64, error)
	WriteTo(io.Writer) (int64, error)
}

var ErrTxnActive = errors.New("txn is active")

type INode interface {
	txnif.TxnEntry
	ApplyUpdate(UpdateNodeIf) error
	ApplyDelete() error
	GetUpdateNode() UpdateNodeIf
	String() string
}
