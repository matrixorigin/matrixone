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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var ErrReadOnly = moerr.NewInternalError("tae moengine: read only")

type Txn interface {
	GetCtx() []byte
	GetID() string
	Is2PC() bool
	SetCommitTS(cts types.TS) error
	SetParticipants(ids []uint64) error
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
	GetPrimaryKeys(context.Context) ([]*engine.Attribute, error)
	GetHideKeys(context.Context) ([]*engine.Attribute, error)

	Write(context.Context, *batch.Batch) error

	Delete(context.Context, *batch.Batch, string) error

	DeleteByPhyAddrKeys(context.Context, *vector.Vector) error

	TableDefs(context.Context) ([]engine.TableDef, error)

	GetRelationID(context.Context) uint64
	//just for test
	// second argument is the number of reader, third argument is the filter extend, foruth parameter is the payload required by the engine
	NewReader(context.Context, int, *plan.Expr, [][]byte) ([]engine.Reader, error)
}

// Database is only used by taeStorage
type Database interface {
	RelationNames(context.Context) ([]string, error)
	GetRelation(context.Context, string) (Relation, error)
	GetRelationByID(context.Context, uint64) (Relation, error)

	DropRelation(context.Context, string) error
	DropRelationByID(context.Context, uint64) error
	TruncateRelationWithID(context.Context, string, uint64) error
	TruncateRelationByID(context.Context, uint64, uint64) error

	CreateRelation(context.Context, string, []engine.TableDef) error               // Create Table - (name, table define)
	CreateRelationWithID(context.Context, string, uint64, []engine.TableDef) error // Create Table - (name, table define)

	GetDatabaseID(ctx context.Context) uint64
}

// moengine.Engine is only used by taeStorage
type Engine interface {
	DropDatabase(ctx context.Context, databaseName string, txn Txn) error
	DropDatabaseByID(ctx context.Context, id uint64, txn Txn) error

	CreateDatabase(ctx context.Context, databaseName string, txn Txn) error
	CreateDatabaseWithID(ctx context.Context, databaseName string, id uint64, txn Txn) error

	// DatabaseNames returns all database names
	DatabaseNames(ctx context.Context, txn Txn) (databaseNames []string, err error)

	// GetDatabase returns a handle for a database
	GetDatabase(ctx context.Context, databaseName string, txn Txn) (Database, error)
	GetDatabaseByID(ctx context.Context, id uint64, txn Txn) (Database, error)

	// GetTAE returns tae db struct
	GetTAE(ctx context.Context) *db.DB
}

type TxnEngine interface {
	engine.Engine
	Engine
	StartTxn(info []byte) (txn Txn, err error)
	GetOrCreateTxnWithMeta(info []byte, id []byte, ts types.TS) (txn Txn, err error)
	GetTxnByID(id []byte) (txn Txn, err error)
	Close() error
	Destroy() error
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
