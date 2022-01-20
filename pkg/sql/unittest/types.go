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

package unittest

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine/kv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// To all the users and the developers:
// 		we use the memory-engine as the instead store-engine at test process
//		Do not test some case with large-size, because test data is stored at memory but not the Disk
//		And memory-engine didn't implement all the interfaces
//		For example:
//			we didn't implement database related interface, and only run SQL at database `test`
//			we didn't implement index related interface
//		So we suggest that only test logic of compute layer
//		In fact, use it to test a query (select col1, col2 ...... and so on) is ok, and please test DDLs carefully

const testEngineIP string = "127.0.0.1"

// How to start a test for compute-layer?
// First Steps: build structure for test case, here are some examples.
// 1. no need to check the result
// 		testCase{
//			sql: "insert into tbl values (1);"
//		}
// 2. expect an error
//		testCase{
//			sql: "create table tbl (col1 int);",
//			err: "tbl already exists",
//		}
// 3. check the result of a query or others
//		testCase{
// 			sql: "select a, b from tbl;"
//			res: executeResult{
//				attr: []string{"a", "b"},
//				data: [][]string{
//					{"1", "24"},
//					{"2", "35"},
//					{"3", "null"},
//				}
//			}
//		}
// Second Steps: call test functions to check the case
//		test(t, testCase)

type testCase struct {
	// sql of a case
	sql string
	// if this sql expect an error return, set error message here
	err string
	// res is an attribute to record expected result of this case, if we don't care result, just set it empty
	res executeResult
	// com records special comments for this case
	com string
}

type executeResult struct {
	null bool		// null is true means expected an empty result
	attr []string	// attr records the expected attribute names, if no care about that, just set it nil
	data [][]string // data records the expected data, if no care about that, just set it nil
}

func newTestEngine() (engine.Engine, *process.Process) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	//e := memEngine.NewTestEngine()
	e := memEngine.New(kv.New(), engine.Node{Id: "0", Addr: testEngineIP})
	return e, proc
}

func executeSQL(sql string, e engine.Engine, proc *process.Process) (*executeResult, error) {
	compile.InitAddress(testEngineIP)
	c := compile.New("test", sql, "", e, proc)
	es, err := c.Build()
	if err != nil {
		return nil, err
	}

	exec := es[0]
	res := &executeResult{}

	err = exec.Compile(nil, func(_ interface{}, bat *batch.Batch) error {
		return fillTestResult(res, bat)
	})
	if err != nil {
		return nil, err
	}
	err = exec.Run(0)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func test(t *testing.T, testCases []testCase) {
	e, proc := newTestEngine()
	for _, tc := range testCases {
		res, err := executeSQL(tc.sql, e, proc)
		switch {
		case len(tc.err) != 0: // we expect receive an error from running this SQL
			require.Equal(t, tc.err, err.Error(), tc.sql)
		case tc.res.null: 	   // we expect receive an empty batch
			require.NoError(t, err, tc.sql)
			if !res.null && (res.attr != nil || res.data != nil) {
				require.Equal(t, "nothing returns", fmt.Sprintf("%v", res.data), tc.sql)
			}
		case tc.res.attr != nil:
			require.NoError(t, err, tc.sql)
			require.Equal(t, tc.res.attr, res.attr, tc.sql)
			if tc.res.data != nil {
				require.NoError(t, err, tc.sql)
				require.Equal(t, tc.res.data, res.data, tc.sql)
			}
		case tc.res.data != nil:
			require.NoError(t, err, tc.sql)
			require.Equal(t, tc.res.data, res.data, tc.sql)
		default: // Does not care about the specific execution result, only expects the statement to be executed successfully
			require.NoError(t, err, tc.sql)
		}
	}
}

func fillTestResult(r *executeResult, bat *batch.Batch) error {
	return convertBatch(r, bat)
}

// convertBatch will convert a batch into an executeResult
// value `null` will record as string "null"
func convertBatch(res *executeResult, b *batch.Batch) error {
	if len(b.Zs) == 0 {
		res.null = true
		return nil
	}
	rows := vector.Length(b.Vecs[0])
	if b.Sels != nil {
		rows = len(b.Sels)
	}

	if rows == 0 {
		res.null = true
		return nil
	}
	cols := len(b.Attrs)

	res.attr = make([]string, cols)
	res.data = make([][]string, rows)

	for i := range res.data {
		res.data[i] = make([]string, cols)
	}

	tempCol := make([]string, rows)
	for i := range res.attr {
		vec := b.Vecs[i]

		res.attr[i] = b.Attrs[i]
		if err := vec.GetColumnData(b.Sels, b.Zs, tempCol); err != nil {
			return err
		}
		for j := 0; j < rows; j++ {
			res.data[j][i] = tempCol[j]
		}
	}
	return nil
}