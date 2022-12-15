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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

func CurrentDate(_ []*vector.Vector, proc *process.Process) (resultVec *vector.Vector, err error) {
	defer func() {
		if err != nil && resultVec != nil {
			resultVec.Free(proc.Mp())
		}
	}()
	resultType := types.T_date.ToType()
	resultVec = proc.AllocScalarVector(resultType)
	loc := proc.SessionInfo.TimeZone
	if loc == nil {
		logutil.Warn("missing timezone in session info")
		loc = time.Local
	}
	resultSlice := vector.MustTCols[types.Date](resultVec)
	ts := types.UnixNanoToTimestamp(proc.UnixTime)
	dateTimes := make([]types.Datetime, 1)
	dateTimes, err = types.TimestampToDatetime(loc, []types.Timestamp{ts}, dateTimes)
	if err != nil {
		return
	}
	resultSlice[0] = dateTimes[0].ToDate()
	return
}
