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

package cache

import (
	"testing"

	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type cuvsDDLWorkspace struct{ client.Workspace }

func (cuvsDDLWorkspace) GetHaveDDL() bool { return true }

type cuvsCacheTxn struct {
	client.TxnOperator
	workspace client.Workspace
	closed    client.TxnEventCallback
}

func (t *cuvsCacheTxn) GetWorkspace() client.Workspace { return t.workspace }
func (t *cuvsCacheTxn) AppendEventCallback(event client.EventType, callbacks ...client.TxnEventCallback) {
	if event != client.ClosedEvent {
		panic("expected a close callback")
	}
	t.closed = callbacks[0]
}

type cuvsTestModel struct {
	MockSearch
	id        int64
	destroyed bool
}

func (m *cuvsTestModel) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{m.id}, []float64{0}, nil
}

func (m *cuvsTestModel) Destroy() { m.destroyed = true }

func TestCuvsCacheIsolationAndPublication(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(c.Destroy)
	const table = "index_storage"
	f32 := CuvsKey(table, metric.Quantization_F32)
	i8 := CuvsKey(table, metric.Quantization_INT8)
	reader := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(t.Context(), "", nil, 0, nil))
	txn := &cuvsCacheTxn{workspace: cuvsDDLWorkspace{}}
	ddl := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(t.Context(), "", txn, 0, nil))

	search := func(proc *sqlexec.SqlProcess, key string, model *cuvsTestModel, id int64) {
		t.Helper()
		got, _, err := c.SearchCuvs(proc, key, model, []float32{0}, vectorindex.RuntimeConfig{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, []int64{id}, got)
	}
	old := &cuvsTestModel{id: 1}
	search(reader, f32, old, 1)
	sameFormat := &cuvsTestModel{id: 7}
	search(ddl, f32, sameFormat, 7)
	require.True(t, sameFormat.destroyed)
	private := &cuvsTestModel{id: 9}
	search(ddl, i8, private, 9)
	require.True(t, private.destroyed)
	_, cached := c.IndexMap.Load(i8)
	require.False(t, cached)

	c.RemoveCuvsOnCommit(txn, table)
	closeTxn := func(status pbtxn.TxnStatus) {
		t.Helper()
		require.NoError(t, txn.closed.Func(t.Context(), txn, client.TxnEvent{Txn: pbtxn.TxnMeta{Status: status}}, txn.closed.Value))
	}
	closeTxn(pbtxn.TxnStatus_Aborted)
	search(reader, f32, &cuvsTestModel{id: 2}, 1)
	newFormat := &cuvsTestModel{id: 3}
	search(reader, i8, newFormat, 3)
	closeTxn(pbtxn.TxnStatus_Committed)
	require.True(t, old.destroyed)
	require.True(t, newFormat.destroyed)
	search(reader, i8, &cuvsTestModel{id: 4}, 4)
}
