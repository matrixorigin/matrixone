// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
func mockIscpRegisterJobSuccess(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
	return true, nil
}

func mockIscpUnregisterJobSuccess(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
	return true, nil
}
*/

func mockIscpRegisterJobError(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
	return false, moerr.NewInternalErrorNoCtx("mock register job error")
}

func mockIscpUnregisterJobError(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
	return false, moerr.NewInternalErrorNoCtx("mock unregister job error")
}

func TestISCPCheckValidIndexCdcByIndexdef(t *testing.T) {
	{

		idx := &plan.IndexDef{
			TableExist:      true,
			IndexAlgo:       "ivfflat",
			IndexAlgoParams: `{"async":"true"}`,
		}
		found, err := checkValidIndexCdcByIndexdef(idx)
		require.Nil(t, err)
		require.Equal(t, found, true)
	}

	{

		idx := &plan.IndexDef{
			TableExist:      true,
			IndexAlgo:       "ivfflat",
			IndexAlgoParams: `{"async":1}`,
		}
		_, err := checkValidIndexCdcByIndexdef(idx)
		require.NotNil(t, err)
	}

	{

		idx := &plan.IndexDef{
			TableExist:      true,
			IndexAlgo:       "ivfflat",
			IndexAlgoParams: `{}`,
		}
		found, err := checkValidIndexCdcByIndexdef(idx)
		require.Nil(t, err)
		require.Equal(t, found, false)
	}
}

func TestISCPCheckValidIndexCdc(t *testing.T) {

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":"true"}`,
				},
			},
		}

		ok, err := checkValidIndexCdc(tbldef, "a")
		require.Nil(t, err)
		require.Equal(t, ok, true)

	}

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":1}`,
				},
			},
		}

		_, err := checkValidIndexCdc(tbldef, "a")
		require.NotNil(t, err)

	}
}

func TestISCPCreateAllIndexCdcTasks(t *testing.T) {

	iscpRegisterJobFunc = mockIscpRegisterJobError
	iscpUnregisterJobFunc = mockIscpUnregisterJobError

	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":"true"}`,
				},
			},
		}

		err := CreateAllIndexCdcTasks(c, tbldef.Indexes, "dbname", "tname", false)
		require.NotNil(t, err)
		fmt.Println(err)

	}

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":1}`,
				},
			},
		}

		err := CreateAllIndexCdcTasks(c, tbldef.Indexes, "dbname", "tname", false)
		require.NotNil(t, err)
		fmt.Println(err)

	}

}

func TestISCPDropAllIndexCdcTasks(t *testing.T) {

	iscpRegisterJobFunc = mockIscpRegisterJobError
	iscpUnregisterJobFunc = mockIscpUnregisterJobError

	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":"true"}`,
				},
			},
		}

		err := DropAllIndexCdcTasks(c, tbldef, "dbname", "tname")
		require.NotNil(t, err)
		fmt.Println(err)

	}

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":1}`,
				},
			},
		}

		err := DropAllIndexCdcTasks(c, tbldef, "dbname", "tname")
		require.NotNil(t, err)
		fmt.Println(err)

	}

}

func TestISCPDropIndexCdcTask(t *testing.T) {

	iscpRegisterJobFunc = mockIscpRegisterJobError
	iscpUnregisterJobFunc = mockIscpUnregisterJobError

	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":"true"}`,
				},
			},
		}

		err := DropIndexCdcTask(c, tbldef, "dbname", "tname", "a")
		require.NotNil(t, err)
		fmt.Println(err)

	}

	{
		tbldef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{
					TableExist:      true,
					IndexName:       "a",
					IndexAlgo:       "ivfflat",
					IndexAlgoParams: `{"async":1}`,
				},
			},
		}

		err := DropIndexCdcTask(c, tbldef, "dbname", "tname", "a")
		require.NotNil(t, err)
		fmt.Println(err)

	}

}

func TestISCPCreateIndexCdcTask(t *testing.T) {

	iscpRegisterJobFunc = mockIscpRegisterJobError
	iscpUnregisterJobFunc = mockIscpUnregisterJobError

	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	{
		err := CreateIndexCdcTask(c, "dbname", "tname", "a", 0, true, "")
		require.NotNil(t, err)
		fmt.Println(err)

	}

}

func TestISCPGetSinkerTypeFromAlgo(t *testing.T) {
	assert.Panics(t, func() { getSinkerTypeFromAlgo("error") }, "getSinkerTypeFromAlgo panic")
}
