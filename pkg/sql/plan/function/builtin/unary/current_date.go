// Copyright 2022 Matrix Origin
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

package unary

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func CurrentDate(_ []*vector.Vector, proc *process.Process) (rvec *vector.Vector, err error) {
	defer func() {
		if err != nil && rvec != nil {
			rvec.Free(proc.Mp())
		}
	}()
	rtyp := types.T_date.ToType()
	loc := proc.SessionInfo.TimeZone
	if loc == nil {
		logutil.Warn("missing timezone in session info")
		loc = time.Local
	}
	ts := types.UnixNanoToTimestamp(proc.UnixTime)
	dateTimes := make([]types.Datetime, 1)
	dateTimes, err = types.TimestampToDatetime(loc, []types.Timestamp{ts}, dateTimes)
	if err != nil {
		return
	}
	rvec = vector.NewConstFixed(rtyp, dateTimes[0].ToDate(), 1, proc.Mp())
	return
}
