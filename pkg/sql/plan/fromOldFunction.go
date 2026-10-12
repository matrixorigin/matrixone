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

package plan

import "github.com/matrixorigin/matrixone/pkg/container/types"

// From old function code, and they were only used by plan.

const (
	// MaxFsp is the maximum digit of fractional seconds part.
	MaxFsp = 6
)

func ExtractToDateReturnType(format string) (tp types.T, fsp int) {
	isTime, isDate, hasMicroseconds := types.ClassifyStrToDateFormat(format)
	if isTime && !isDate {
		tp = types.T_time
	} else if isTime && isDate {
		tp = types.T_datetime
	} else {
		tp = types.T_date
	}
	if hasMicroseconds {
		fsp = MaxFsp
	}
	return tp, fsp
}
