// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hnsw

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

/*
// give metadata [index_id, checksum, timestamp]
func mock_runSql(proc *process.Process, sql string) (executor.Result, error) {
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeMetaBatch(proc)}}, nil
}

// give blob
func mock_runSql_streaming(proc *process.Process, sql string, ch chan executor.Result, err_chan chan error) (executor.Result, error) {
	defer close(ch)
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc)}}
	ch <- res
	return executor.Result{}, nil
}
*/

func TestSync(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool("", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql

	cdc := vectorindex.VectorIndexCdc[float32]{}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}
