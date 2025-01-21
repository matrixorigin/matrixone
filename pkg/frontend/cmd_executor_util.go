// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/schemaversion"
)

func handleVersionInfo(ses FeSession, execCtx *ExecCtx) error {
	versionInfo := schemaversion.NewVersionInfo()
	isFinalVersion, err := defines.GetIsFinalVersion(execCtx.reqCtx)
	if err != nil {
		isFinalVersion = false
	}
	versionInfo.FinalVersionCompleted = isFinalVersion

	if !isFinalVersion {
		bh := execCtx.ses.GetShareTxnBackgroundExec(execCtx.reqCtx, false)
		defer bh.Close()

		clusterVersion, state, isFinal, err := getClusterVersion(execCtx.reqCtx, versionInfo.FinalVersion, bh)
		if err != nil {
			return err
		}
		versionInfo.Cluster.Version = clusterVersion
		versionInfo.Cluster.IsFinalVersion = isFinal

		//-----------------------------------------------------------------------------------------
		finalVersionCompleted := versionInfo.FinalVersion == clusterVersion && state == versions.StateReady

		// set global variables for finalVersionCompleted
		versionInfo.FinalVersionCompleted = finalVersionCompleted
		runtime.ServiceRuntime(ses.GetService()).SetGlobalVariables(runtime.ClusterIsFinalVersion, finalVersionCompleted)

		if !finalVersionCompleted {
			handleOffset := versions.Compare(versionInfo.Cluster.Version, "2.1.0") >= 0
			accVersion, accOffset, err := getAccountVersion(execCtx.reqCtx, execCtx.ses, handleOffset, bh)
			if err != nil {
				return err
			}
			versionInfo.Account.Version = accVersion
			versionInfo.Account.VersionOffset = accOffset
		}
	}

	ses.GetTxnHandler().txnOp.SetVersionInfo(versionInfo)
	execCtx.reqCtx = defines.AttachVersionInfo(execCtx.reqCtx, versionInfo)
	return nil
}

func getAccountVersion(ctx context.Context, ses FeSession, flag bool, bh BackgroundExec) (string, int32, error) {
	offsetCol := ""
	if flag {
		offsetCol = ", version_offset"
	}
	accSql := fmt.Sprintf("SELECT create_version %s FROM mo_catalog.mo_account WHERE account_id = %d", offsetCol, ses.GetAccountId())
	bh.ClearExecResultSet()

	sysTenantCtx := defines.AttachAccount(ctx, uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))
	err := bh.Exec(sysTenantCtx, accSql)
	if err != nil {
		return "", 0, err
	}

	erArray, err := getResultSet(sysTenantCtx, bh)
	if err != nil {
		return "", 0, err
	}

	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		createVersion, err := erArray[0].GetString(sysTenantCtx, i, 0)
		if err != nil {
			return "", 0, err
		}

		versionOffset := int64(-1)
		if flag {
			versionOffset, err = erArray[0].GetInt64(sysTenantCtx, i, 1)
			if err != nil {
				return "", 0, err
			}
		}
		return createVersion, int32(versionOffset), nil
	}
	return "", 0, nil
}

func getClusterVersion(ctx context.Context, finalVersion string, bh BackgroundExec) (string, int32, bool, error) {
	sql := fmt.Sprintf("SELECT to_version, state, final_version = to_version FROM mo_catalog.mo_upgrade WHERE state >= 1 AND (final_version, final_version_offset) IN (SELECT version, version_offset FROM mo_catalog.mo_version ORDER BY create_at DESC LIMIT 1) ORDER BY upgrade_order DESC LIMIT 1")
	bh.ClearExecResultSet()

	sysTenantCtx := defines.AttachAccount(ctx, uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))
	err := bh.Exec(sysTenantCtx, sql)
	if err != nil {
		return "", 0, false, err
	}

	erArray, err := getResultSet(sysTenantCtx, bh)
	if err != nil {
		return "", 0, false, err
	}

	if len(erArray) == 0 {
		// 说明当前为初始化版本，没有任何升级任务
		sql2 := fmt.Sprintf("SELECT version, state, version = '%s' FROM mo_catalog.mo_version where state >= 1 order by create_at desc limit 1", finalVersion)
		err := bh.Exec(sysTenantCtx, sql2)
		if err != nil {
			return "", 0, false, err
		}

		erArray, err = getResultSet(sysTenantCtx, bh)
		if err != nil {
			return "", 0, false, err
		}
	}

	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		clusterVersion, err := erArray[0].GetString(sysTenantCtx, i, 0)
		if err != nil {
			return "", 0, false, err
		}

		state, err := erArray[0].GetInt64(sysTenantCtx, i, 1)
		if err != nil {
			return "", 0, false, err
		}

		isFinalVersion, err := erArray[0].GetInt64(sysTenantCtx, i, 2)
		if err != nil {
			return "", 0, false, err
		}

		return clusterVersion, int32(state), isFinalVersion != 0, nil
	}
	return "", 0, false, err
}
