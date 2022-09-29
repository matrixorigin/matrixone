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

package fromunixtime

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	UnixToDatetime         func(*time.Location, []int64, []types.Datetime) []types.Datetime
	UnixToDateTimeWithNsec func(*time.Location, []int64, []int64, []types.Datetime) []types.Datetime
)

func init() {
	UnixToDatetime = unixToDatetime
	UnixToDateTimeWithNsec = unixToDateTimeWithNsec
}

func unixToDatetime(loc *time.Location, xs []int64, rs []types.Datetime) []types.Datetime {
	for i := range xs {
		rs[i] = types.FromUnix(loc, xs[i])
	}
	return rs
}

func unixToDateTimeWithNsec(loc *time.Location, xs []int64, ns []int64, rs []types.Datetime) []types.Datetime {
	for i := range xs {
		rs[i] = types.FromUnixWithNsec(loc, xs[i], ns[i])
	}
	return rs
}
