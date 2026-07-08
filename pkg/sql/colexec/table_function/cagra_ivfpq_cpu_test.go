//go:build !gpu

// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newStubTableFunction(name string) *TableFunction {
	return &TableFunction{
		Attrs: []string{"status"},
		Rets: []*plan.ColDef{{
			Name: "status",
			Typ:  plan.Type{Id: int32(types.T_int32)},
		}},
		FuncName: name,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
}

// runStubLifecycle drives prepare → start → call → end → reset → free
// for the simple CPU stub state machines (cagra_create, cagra_search,
// ivfpq_create, ivfpq_search). All four share the same skeleton.
func runStubLifecycle(t *testing.T, prep func(p *process.Process, tf *TableFunction) (tvfState, error), name string) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	tf := newStubTableFunction(name)
	// retSchema is required by createResultBatch
	retSchema := make([]types.Type, len(tf.Rets))
	for i, r := range tf.Rets {
		retSchema[i] = types.New(types.T(r.Typ.Id), r.Typ.Width, r.Typ.Scale)
	}
	tf.ctr.retSchema = retSchema

	st, err := prep(proc, tf)
	require.NoError(t, err)

	err = st.start(tf, proc, 0, nil)
	require.NoError(t, err)

	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, res.Status)

	err = st.end(tf, proc)
	require.NoError(t, err)

	st.reset(tf, proc)
	st.free(tf, proc, false, nil)
}

func TestCagraCreateCpuLifecycle(t *testing.T) {
	runStubLifecycle(t, cagraCreatePrepare, "cagra_create")
}

func TestCagraSearchCpuLifecycle(t *testing.T) {
	runStubLifecycle(t, cagraSearchPrepare, "cagra_search")
}

func TestIvfpqCreateCpuLifecycle(t *testing.T) {
	runStubLifecycle(t, ivfpqCreatePrepare, "ivfpq_create")
}

func TestIvfpqSearchCpuLifecycle(t *testing.T) {
	runStubLifecycle(t, ivfpqSearchPrepare, "ivfpq_search")
}

// TestTableFunctionPrepareCagraIvfpq exercises the dispatch in Prepare for
// the new cagra_*/ivfpq_* table functions, covering the new switch arms.
func TestTableFunctionPrepareCagraIvfpq(t *testing.T) {
	names := []string{"cagra_create", "cagra_search", "ivfpq_create", "ivfpq_search"}
	for _, n := range names {
		t.Run(n, func(t *testing.T) {
			m := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", m)
			tf := newStubTableFunction(n)
			err := tf.Prepare(proc)
			require.NoError(t, err)
			require.NotNil(t, tf.ctr.state)
			tf.ctr.state.free(tf, proc, false, nil)
		})
	}
}

// TestTableFunctionPrepareUnknown verifies that an unknown table function
// name returns the "not supported" error from the default branch.
func TestTableFunctionPrepareUnknown(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	tf := newStubTableFunction("this_does_not_exist")
	err := tf.Prepare(proc)
	require.Error(t, err)
}
