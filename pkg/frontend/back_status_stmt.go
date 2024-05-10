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
	"fmt"
	"time"
)

func executeStatusStmtInBack(backSes *backSession,
	execCtx *ExecCtx) (err error) {
	runBegin := time.Now()
	if _, err = execCtx.runner.Run(0); err != nil {
		return
	}

	// only log if run time is longer than 1s
	if time.Since(runBegin) > time.Second {
		logInfo(backSes, backSes.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
	}

	return
}
