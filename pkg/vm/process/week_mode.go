// Copyright 2026 Matrix Origin
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

package process

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// ResolveDefaultWeekFormatMode reads the live value on the initiating CN and
// otherwise uses the statement snapshot carried by the remote process.
func ResolveDefaultWeekFormatMode(proc *Process) (mode int, present bool, err error) {
	if proc == nil || proc.Base == nil {
		return 0, false, nil
	}
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		value, err := resolve("default_week_format", true, false)
		if err != nil {
			return 0, false, err
		}
		if value == nil {
			return 0, true, nil
		}
		var raw int64
		switch v := value.(type) {
		case int64:
			raw = v
		case int32:
			raw = int64(v)
		case int:
			raw = int64(v)
		case uint64:
			raw = int64(v % 8)
		case uint32:
			raw = int64(v)
		case uint:
			raw = int64(v % 8)
		default:
			return 0, false, moerr.NewInternalErrorNoCtxf("session variable default_week_format has unexpected type %T", value)
		}
		return int((raw%8 + 8) % 8), true, nil
	}
	if proc.Base.SessionInfo.DefaultWeekFormatSet {
		return int(proc.Base.SessionInfo.DefaultWeekFormat % 8), true, nil
	}
	return 0, false, nil
}
