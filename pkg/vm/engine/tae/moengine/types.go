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

package moengine

import (
	"bytes"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var ErrReadOnly = errors.New("tae moengine: read only")

type Txn interface {
	GetCtx() []byte
	GetID() uint64
	Commit() error
	Rollback() error
	String() string
	Repr() string
	GetError() error
}

type TxnEngine interface {
	engine.Engine
	StartTxn(info []byte) (txn Txn, err error)
}

var _ TxnEngine = &txnEngine{}

type txnEngine struct {
	impl *db.DB
}

type txnDatabase struct {
	handle handle.Database
}

type txnRelation struct {
	baseRelation
}

type sysRelation struct {
	baseRelation
}

type baseRelation struct {
	handle handle.Relation
	nodes  engine.Nodes
}

type txnBlock struct {
	handle handle.Block
}

type txnReader struct {
	handle handle.Relation
	it     handle.BlockIt
	buffer []*bytes.Buffer
}
