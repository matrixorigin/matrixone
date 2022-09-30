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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var ErrReadOnly = moerr.NewInternalError("tae moengine: read only")

type Txn interface {
	GetCtx() []byte
	GetID() uint64
	Prepare() (types.TS, error)
	Committing() error
	Commit() error
	Rollback() error
	String() string
	Repr() string
	GetError() error
}

// Relation is only used by taeStorage
type Relation interface {
	Write(context.Context, *batch.Batch) error

	Delete(context.Context, *vector.Vector, string) error

	DeleteByPhyAddrKeys(context.Context, *vector.Vector) error

	Truncate(context.Context) (uint64, error)

	GetRelationID(context.Context) uint64
}

// Database is only used by taeStorage
type Database interface {
	RelationNames(context.Context) ([]string, error)
	GetRelation(context.Context, string) (Relation, error)

	DropRelation(context.Context, string) error
	CreateRelation(context.Context, string, []engine.TableDef) error // Create Table - (name, table define)

	GetDatabaseID(ctx context.Context) uint64
}

// Engine is only used by taeStorage
type Engine interface {
	// Delete deletes a database
	DropDatabase(ctx context.Context, databaseName string, txn Txn) error

	// Create creates a database
	CreateDatabase(ctx context.Context, databaseName string, txn Txn) error

	// Databases returns all database names
	DatabaseNames(ctx context.Context, txn Txn) (databaseNames []string, err error)

	// Database creates a handle for a database
	GetDatabase(ctx context.Context, databaseName string, txn Txn) (Database, error)

	// GetDB returns tae db struct
	GetTAE(ctx context.Context) *db.DB
}

type TxnEngine interface {
	engine.Engine
	Engine
	StartTxn(info []byte) (txn Txn, err error)
	GetOrCreateTxnWithMeta(info []byte, meta txn.TxnMeta) (txn Txn, err error)
	GetTxnByMeta(meta txn.TxnMeta) (txn Txn, err error)
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
