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

package trace

import "github.com/matrixorigin/matrixone/pkg/util"

var _ HasItemSize = &MOErrorHolder{}

type MOErrorHolder struct {
	Error     error         `json:"error"`
	Timestamp util.TimeNano `json:"timestamp"`
}

func (s MOErrorHolder) GetName() string {
	return MOErrorType
}

func (s MOErrorHolder) Size() int64 {
	return int64(32 * 8)
}

func ReportError(err error) *MOErrorHolder {
	e := &MOErrorHolder{Error: err}
	/*var has bool
	if e.Timestamp, has = GetTimestamp(err); !has {
		e.Timestamp = util.NowNS()
	}*/
	return e
}
