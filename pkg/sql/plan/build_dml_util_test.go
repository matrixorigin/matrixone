// Copyright 2025 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_runSql(t *testing.T) {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()

	ctx := context.Background()
	proc.Ctx = context.Background()
	proc.ReplaceTopCtx(ctx)

	compilerContext := NewMockCompilerContext2(ctrl)
	compilerContext.EXPECT().GetProcess().Return(proc).AnyTimes()

	_, err := runSql(compilerContext, "")
	require.Error(t, err, "internal error: no account id in context")
}

func Test_buildPostDmlFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPostDmlFullTextIndex(nil, nil, nil, nil, nil, nil, 0, idxdef, 0, false, false, false)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPostDmlFullTextIndex(nil, nil, nil, nil, nil, nil, 0, idxdef, 0, false, false, false)
		require.Nil(t, err)
	}

}

func Test_buildPreDeleteFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.Nil(t, err)
	}

}
