// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	arg := Argument{FuncName: "unnest"}
	arg.String(bytes.NewBuffer(nil))
}

func TestCall(t *testing.T) {
	arg := Argument{FuncName: "unnest",
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	resetChildren(&arg, nil)
	end, err := arg.Call(testutil.NewProc())
	require.NoError(t, err)
	require.True(t, end.Status == vm.ExecStop)
	// arg.Name = "generate_series"
	// end, err = arg.Call(testutil.NewProc())
	// require.NoError(t, err)
	require.True(t, end.Status == vm.ExecStop)
	arg.FuncName = "metadata_scan"
	end, err = arg.Call(testutil.NewProc())
	require.NoError(t, err)
	require.True(t, end.Status == vm.ExecStop)
	arg.FuncName = "not_exist"
	end, err = arg.Call(testutil.NewProc())
	require.Error(t, err)
	require.True(t, end.Status == vm.ExecStop)
}

func TestPrepare(t *testing.T) {
	arg := Argument{FuncName: "unnest",
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}
	err := arg.Prepare(testutil.NewProc())
	require.Error(t, err)
	arg.FuncName = "generate_series"
	err = arg.Prepare(testutil.NewProc())
	require.NoError(t, err)
	arg.FuncName = "metadata_scan"
	err = arg.Prepare(testutil.NewProc())
	require.NoError(t, err)
	arg.FuncName = "not_exist"
	err = arg.Prepare(testutil.NewProc())
	require.Error(t, err)
}
