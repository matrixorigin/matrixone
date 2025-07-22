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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
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

// give metadata [index_id, checksum, timestamp]
func mock_runSql_empty(proc *process.Process, sql string) (executor.Result, error) {

	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{}}, nil
}

func mock_runTxn(proc *process.Process, fn func(exec executor.TxnExecutor) error) error {
	return nil
}

func TestSyncUpsertWithEmpty(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql_empty
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.01
		}
	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

func TestSyncUpsert(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.01
		}
	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete all items
func TestSyncDelete(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(0)

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncDeleteAndInsert(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 200)}

	key := int64(0)

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	key = 0
	v := []float32{0.1, 0.2, 0.3}
	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_INSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1

	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncUpdate(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 100)}

	key := int64(0)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncDeleteAndUpsert(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 200)}

	key := int64(0)

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	key = 0
	v := []float32{0.1, 0.2, 0.3}
	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1

	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// total 1000100 items and should have two models
func TestSyncAddOneModel(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.001
		}
	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete all items
func TestSyncDelete2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(0)

	for i := 0; i < 200; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete all items
func TestSyncDeleteShuffle2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(0)

	for i := 0; i < 200; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncUpdateShuffle2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 100)}

	key := int64(0)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 200; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}

// should update and insert items
func TestSyncUpdateInsertShuffle2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runCatalogSql = mock_runCatalogSql
	runTxn = mock_runTxn

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 100)}

	key := int64(0)
	v := []float32{0.1, 0.2, 0.3}

	// 0 - 199 key exists, 200 - 399 new insert
	for i := 0; i < 400; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	err := CdcSync(proc, "db", "src", 3, &cdc)
	require.Nil(t, err)
}
