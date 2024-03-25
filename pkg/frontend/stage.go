// Copyright 2024 Matrix Origin
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
	"math/bits"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func handleCreateStage(ctx context.Context, ses FeSession, cs *tree.CreateStage) error {
	return doCreateStage(ctx, ses.(*Session), cs)
}

func handleAlterStage(ctx context.Context, ses FeSession, as *tree.AlterStage) error {
	return doAlterStage(ctx, ses.(*Session), as)
}

func handleDropStage(ctx context.Context, ses FeSession, ds *tree.DropStage) error {
	return doDropStage(ctx, ses.(*Session), ds)
}

func checkStageExistOrNot(ctx context.Context, bh BackgroundExec, stageName string) (bool, error) {
	var sql string
	var erArray []ExecResult
	var err error
	sql, err = getSqlForCheckStage(ctx, stageName)
	if err != nil {
		return false, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

func formatCredentials(credentials tree.StageCredentials) string {
	var rstr string
	if credentials.Exist {
		for i := 0; i < len(credentials.Credentials)-1; i += 2 {
			rstr += fmt.Sprintf("%s=%s", credentials.Credentials[i], credentials.Credentials[i+1])
			if i != len(credentials.Credentials)-2 {
				rstr += ","
			}
		}
	}
	return rstr
}

func doCreateStage(ctx context.Context, ses *Session, cs *tree.CreateStage) (err error) {
	var sql string
	//var err error
	var stageExist bool
	var credentials string
	var StageStatus string
	var comment string
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create stage priv
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check stage
	stageExist, err = checkStageExistOrNot(ctx, bh, string(cs.Name))
	if err != nil {
		return err
	}

	if stageExist {
		if !cs.IfNotExists {
			return moerr.NewInternalError(ctx, "the stage %s exists", cs.Name)
		} else {
			// do nothing
			return err
		}
	} else {
		// format credentials and hash it
		credentials = HashPassWord(formatCredentials(cs.Credentials))

		if !cs.Status.Exist {
			StageStatus = "disabled"
		} else {
			StageStatus = cs.Status.Option.String()
		}

		if cs.Comment.Exist {
			comment = cs.Comment.Comment
		}

		sql, err = getSqlForInsertIntoMoStages(ctx, string(cs.Name), cs.Url, credentials, StageStatus, types.CurrentTimestamp().String2(time.UTC, 0), comment)
		if err != nil {
			return err
		}

		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}

	return err
}

func doCheckFilePath(ctx context.Context, ses *Session, ep *tree.ExportParam) (err error) {
	//var err error
	var filePath string
	var sql string
	var erArray []ExecResult
	var stageName string
	var stageStatus string
	var url string
	if ep == nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// detect filepath contain stage or not
	filePath = ep.FilePath
	if !strings.Contains(filePath, ":") {
		// the filepath is the target path
		sql = getSqlForCheckStageStatus(ctx, "enabled")
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}

		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			return err
		}

		// if have stage enabled
		if execResultArrayHasData(erArray) {
			return moerr.NewInternalError(ctx, "stage exists, please try to check and use a stage instead")
		} else {
			// use the filepath
			return err
		}
	} else {
		stageName = strings.Split(filePath, ":")[0]
		// check the stage status
		sql, err = getSqlForCheckStageStatusWithStageName(ctx, stageName)
		if err != nil {
			return err
		}
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}

		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			return err
		}
		if execResultArrayHasData(erArray) {
			stageStatus, err = erArray[0].GetString(ctx, 0, 1)
			if err != nil {
				return err
			}

			// is the stage staus is disabled
			if stageStatus == tree.StageStatusDisabled.String() {
				return moerr.NewInternalError(ctx, "stage '%s' is invalid, please check", stageName)
			} else if stageStatus == tree.StageStatusEnabled.String() {
				// replace the filepath using stage url
				url, err = erArray[0].GetString(ctx, 0, 0)
				if err != nil {
					return err
				}

				filePath = strings.Replace(filePath, stageName+":", url, 1)
				ses.ep.userConfig.StageFilePath = filePath
			}
		} else {
			return moerr.NewInternalError(ctx, "stage '%s' is not exists, please check", stageName)
		}
	}
	return err

}

func doAlterStage(ctx context.Context, ses *Session, as *tree.AlterStage) (err error) {
	var sql string
	//var err error
	var stageExist bool
	var credentials string
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create stage priv
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	optionBits := uint8(0)
	if as.UrlOption.Exist {
		optionBits |= 1
	}
	if as.CredentialsOption.Exist {
		optionBits |= 1 << 1
	}
	if as.StatusOption.Exist {
		optionBits |= 1 << 2
	}
	if as.Comment.Exist {
		optionBits |= 1 << 3
	}
	optionCount := bits.OnesCount8(optionBits)
	if optionCount == 0 {
		return moerr.NewInternalError(ctx, "at least one option at a time")
	}
	if optionCount > 1 {
		return moerr.NewInternalError(ctx, "at most one option at a time")
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check stage
	stageExist, err = checkStageExistOrNot(ctx, bh, string(as.Name))
	if err != nil {
		return err
	}

	if !stageExist {
		if !as.IfNotExists {
			return moerr.NewInternalError(ctx, "the stage %s not exists", as.Name)
		} else {
			// do nothing
			return err
		}
	} else {
		if as.UrlOption.Exist {
			sql = getsqlForUpdateStageUrl(string(as.Name), as.UrlOption.Url)
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}

		if as.CredentialsOption.Exist {
			credentials = HashPassWord(formatCredentials(as.CredentialsOption))
			sql = getsqlForUpdateStageCredentials(string(as.Name), credentials)
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}

		if as.StatusOption.Exist {
			sql = getsqlForUpdateStageStatus(string(as.Name), as.StatusOption.Option.String())
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}

		if as.Comment.Exist {
			sql = getsqlForUpdateStageComment(string(as.Name), as.Comment.Comment)
			err = bh.Exec(ctx, sql)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func doDropStage(ctx context.Context, ses *Session, ds *tree.DropStage) (err error) {
	var sql string
	//var err error
	var stageExist bool
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create stage priv
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check stage
	stageExist, err = checkStageExistOrNot(ctx, bh, string(ds.Name))
	if err != nil {
		return err
	}

	if !stageExist {
		if !ds.IfNotExists {
			return moerr.NewInternalError(ctx, "the stage %s not exists", ds.Name)
		} else {
			// do nothing
			return err
		}
	} else {
		sql = getSqlForDropStage(string(ds.Name))
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}
