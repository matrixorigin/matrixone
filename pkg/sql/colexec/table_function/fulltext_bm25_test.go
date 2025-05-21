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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCallBM25(t *testing.T) {

	ut := newFTTestCase(mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_BM25)

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 3; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 8192)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCallOneAttrBM25(t *testing.T) {

	ut := newFTTestCase(mpool.MustNewZero(), ftdefaultAttrs[0:1], fulltext.ALGO_BM25)

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 3; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 8192)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextEarlyFreeBM25(t *testing.T) {

	ut := newFTTestCase(mpool.MustNewZero(), ftdefaultAttrs[0:1], fulltext.ALGO_BM25)

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	/*
		var result vm.CallResult
		// first call receive data
		for i := 0; i < 2; i++ {
			result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
			require.Nil(t, err)
			require.Equal(t, result.Status, vm.ExecNext)
			require.Equal(t, result.Batch.RowCount(), 8192)
		}
	*/

	// early free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}
