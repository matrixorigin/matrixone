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
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func executeStatusStmtInBack(backSes *backSession,
	execCtx *ExecCtx) (err error) {
	execCtx.ses.EnterFPrint(FPStatusStmtInBack)
	defer execCtx.ses.ExitFPrint(FPStatusStmtInBack)
	fPrintTxnOp := execCtx.ses.GetTxnHandler().GetTxn()

	err = disttae.CheckTxnIsValid(fPrintTxnOp)
	if err != nil {
		return err
	}

	runBegin := time.Now()
	if _, err = execCtx.runner.Run(0); err != nil {
		return
	}

	// only log if run time is longer than 1s
	if time.Since(runBegin) > time.Second {
		backSes.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
	}

	return
}
