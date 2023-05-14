// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/assert"
)

const (
	origin    = "origin"
	temporary = "temporary"
)

// There is no way to know the control flow of EntireEngine directly through the Engine method
// BUT this is exactly what we want to test.
// So we need sth to mark the transition of states
// The following enumeration shows all the possible states of the EntireEngine
const (
	first_engine_then_tempengine = -2
	only_tempengine              = 0
	only_engine                  = 2
)

type testEntireEngine struct {
	EntireEngine
	step  int
	state int
}

type testEngine struct {
	name   string // origin or temporary
	parent *testEntireEngine
}

var _ Engine = new(testEngine)

type testOperator struct {
}

func TestEntireEngineNew(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.New(ctx, op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.New(ctx, op)
	assert.Equal(t, first_engine_then_tempengine, ee.state)
}

func TestEntireEngineCommit(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.Commit(ctx, op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Commit(ctx, op)
	assert.Equal(t, first_engine_then_tempengine, ee.state)
}

func TestEntireEngineRollback(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.Rollback(ctx, op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Rollback(ctx, op)
	assert.Equal(t, first_engine_then_tempengine, ee.state)
}

func TestEntireEngineDelete(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.Delete(ctx, "bar", op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Delete(ctx, "foo", op)
	assert.Equal(t, only_engine, ee.state)
}

func TestEntireEngineCreate(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.Create(ctx, "bar", op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Create(ctx, "foo", op)
	assert.Equal(t, only_engine, ee.state)
}

func TestEntireEngineDatabases(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.Databases(ctx, op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Databases(ctx, op)
	assert.Equal(t, only_engine, ee.state)
}

func TestEntireEngineDatabase(t *testing.T) {
	ctx := context.TODO()
	op := newtestOperator()
	ee := buildEntireEngineWithoutTempEngine()
	ee.Database(ctx, "foo", op)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Database(ctx, defines.TEMPORARY_DBNAME, op)
	assert.Equal(t, only_tempengine, ee.state)

}

func TestEntireEngineNodes(t *testing.T) {
	ee := buildEntireEngineWithoutTempEngine()
	ee.Nodes(false, "", nil)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Nodes(false, "", nil)
	assert.Equal(t, only_engine, ee.state)
}

func TestEntireEngineHints(t *testing.T) {
	ee := buildEntireEngineWithoutTempEngine()
	ee.Hints()
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.Hints()
	assert.Equal(t, only_engine, ee.state)

}

func TestEntireEngineNewBlockReader(t *testing.T) {
	ctx := context.TODO()
	ee := buildEntireEngineWithoutTempEngine()
	ee.NewBlockReader(ctx, 1, timestamp.Timestamp{}, nil, nil, nil)
	assert.Equal(t, only_engine, ee.state)
	ee = buildEntireEngineWithTempEngine()
	ee.NewBlockReader(ctx, 1, timestamp.Timestamp{}, nil, nil, nil)
	assert.Equal(t, only_engine, ee.state)
}

func buildEntireEngineWithTempEngine() *testEntireEngine {
	ee := new(testEntireEngine)
	ee.state = 1

	e := newtestEngine(origin, ee)
	te := newtestEngine(temporary, ee)

	ee.Engine = e
	ee.TempEngine = te
	return ee
}

func buildEntireEngineWithoutTempEngine() *testEntireEngine {
	ee := new(testEntireEngine)
	ee.state = 1

	e := newtestEngine(origin, ee)
	ee.Engine = e
	return ee
}

func newtestEngine(name string, tee *testEntireEngine) *testEngine {
	return &testEngine{name: name, parent: tee}
}

func (e *testEngine) New(_ context.Context, _ client.TxnOperator) error {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}

	return nil
}

func (e *testEngine) Commit(_ context.Context, _ client.TxnOperator) error {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}

	return nil
}

func (e *testEngine) Rollback(_ context.Context, _ client.TxnOperator) error {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}

	return nil
}

func (e *testEngine) Delete(ctx context.Context, name string, _ client.TxnOperator) error {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}

	return nil
}

func (e *testEngine) Create(ctx context.Context, name string, _ client.TxnOperator) error {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}

	return nil
}

func (e *testEngine) Databases(ctx context.Context, txnOp client.TxnOperator) ([]string, error) {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}

	var a []string
	a = append(a, "foo")
	a = append(a, "bar")
	return a, nil
}

func (e *testEngine) Database(ctx context.Context, name string, txnOp client.TxnOperator) (Database, error) {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}
	return nil, nil
}

func (e *testEngine) Nodes(_ bool, _ string, _ map[string]string) (Nodes, error) {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}
	return nil, nil
}

func (e *testEngine) Hints() (h Hints) {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}
	return
}

func (e *testEngine) NewBlockReader(_ context.Context, _ int, _ timestamp.Timestamp,
	_ *plan.Expr, _ [][]byte, _ *plan.TableDef) ([]Reader, error) {
	e.parent.step = e.parent.step + 1
	if e.name == origin {
		e.parent.state = e.parent.state + e.parent.step*e.parent.state
	} else {
		e.parent.state = e.parent.state - e.parent.step*e.parent.state
	}
	return nil, nil
}

func (e *testEngine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error) {
	return "", "", nil
}

func (e *testEngine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel Relation, err error) {
	return "", "", nil, nil
}

func (e *testEngine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}

func newtestOperator() *testOperator {
	return &testOperator{}
}

func (o *testOperator) AddWorkspace(_ client.Workspace) {
}

func (o *testOperator) GetWorkspace() client.Workspace {
	return nil
}

func (o *testOperator) ApplySnapshot(data []byte) error {
	return nil
}

func (o *testOperator) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (o *testOperator) Commit(ctx context.Context) error {
	return nil
}

func (o *testOperator) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (o *testOperator) Rollback(ctx context.Context) error {
	return nil
}

func (o *testOperator) Snapshot() ([]byte, error) {
	return nil, nil
}

func (o *testOperator) Txn() txn.TxnMeta {
	return txn.TxnMeta{}
}

func (o *testOperator) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (o *testOperator) AddLockTable(lock.LockTable) error {
	return nil
}

func (o *testOperator) UpdateSnapshot(ts timestamp.Timestamp) error {
	panic("should not call")
}
