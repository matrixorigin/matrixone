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
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CuvsKey keeps readers using different physical model formats apart.
func CuvsKey(indexTable string, format metric.QuantizationType) string {
	return indexTable + ":" + strconv.FormatUint(uint64(format), 10)
}

// SearchCuvs keeps models read from a DDL workspace local to the query.
func (c *VectorIndexCache) SearchCuvs(proc *sqlexec.SqlProcess, key string, algo VectorIndexSearchIf,
	query any, config vectorindex.RuntimeConfig) (any, []float64, error) {
	if txn := proc.Txn(); txn != nil {
		if workspace := txn.GetWorkspace(); workspace != nil && workspace.GetHaveDDL() {
			defer algo.Destroy()
			if err := algo.Load(proc); err != nil {
				return nil, nil, err
			}
			return algo.Search(proc, query, config)
		}
	}
	return c.Search(proc, key, algo, query, config)
}

// RemoveCuvs evicts the supported formats without scanning other cache keys.
func (c *VectorIndexCache) RemoveCuvs(indexTable string) {
	for _, format := range metric.CuvsQuantizationNameToType {
		c.Remove(CuvsKey(indexTable, format))
	}
}

// RemoveCuvsOnCommit evicts any entry loaded before the replacement became
// visible. An unknown commit outcome also needs refresh.
func (c *VectorIndexCache) RemoveCuvsOnCommit(txn client.TxnOperator, indexTable string) {
	txn.AppendEventCallback(client.ClosedEvent, client.NewTxnEventCallback(
		func(_ context.Context, _ client.TxnOperator, event client.TxnEvent, _ any) error {
			if !event.Aborted() {
				c.RemoveCuvs(indexTable)
			}
			return nil
		},
	))
}
