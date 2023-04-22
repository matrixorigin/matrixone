// Copyright 2021 - 2022 Matrix Origin
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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_BuiltIn_CurrentSessionInfo(t *testing.T) {
	proc := testutil.NewProcess()
	proc.SessionInfo = process.SessionInfo{
		User:      "test_user1",
		UserId:    135,
		Account:   "test_account2",
		AccountId: 246,
		Role:      "test_role3",
		RoleId:    147,
	}

	{
		tc := tcTemp{
			info:   "select current_user_id()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_uint32.ToType(), false,
				[]uint32{135}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentUserID)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_user_name()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_user1"}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentUserName)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_account_id()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_uint32.ToType(), false,
				[]uint32{246}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentAccountID)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_account_name()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_account2"}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentAccountName)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_role_id()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_uint32.ToType(), false,
				[]uint32{147}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentRoleID)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_role_name()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_role3"}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentRoleName)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_role()",
			inputs: []testutil.FunctionTestInput{},
			expect: testutil.NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_role3"}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentRole)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
