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
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func wantDatetimeFromUnix(ts int64) types.Datetime {
	s := time.Unix(ts, 0).Local().Format("2006-01-02 15:04:05")
	datetime, err := types.ParseDatetime(s, 6)
	if err != nil {
		panic("bad datetime")
	}
	return datetime
}

func TestFromUnixTimestamp(t *testing.T) {
	xs := []int64{1641046980, 1641133380, 1641219780}
	rs := make([]types.Datetime, 3)
	want := []types.Datetime{
		wantDatetimeFromUnix(xs[0]),
		wantDatetimeFromUnix(xs[1]),
		wantDatetimeFromUnix(xs[2]),
	}

	got := unixToDatetime(xs, rs)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unixtimestamp() want %v but got %v", want, got)
	}
}
