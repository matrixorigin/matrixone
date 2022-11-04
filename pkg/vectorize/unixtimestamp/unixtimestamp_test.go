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

package unixtimestamp

import (
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func mustTimestamp(s string) types.Timestamp {
	timestamp, err := types.ParseTimestamp(time.Local, s, 6)
	if err != nil {
		panic("bad datetime")
	}
	return timestamp
}

func TestUnixTimestamp(t *testing.T) {

	xs := make([]types.Timestamp, 3)
	rs := make([]int64, 3)
	want := make([]int64, 3)
	for i, timestr := range []string{"2022-01-01 22:23:00", "2022-01-02 22:23:00", "2022-01-03 22:23:00"} {
		xs[i] = mustTimestamp(timestr)
		goLcoalTime, _ := time.ParseInLocation("2006-01-02 15:04:05", timestr, time.Local)
		want[i] = goLcoalTime.Unix()
	}

	got := unixTimestampToInt(xs, rs)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unixtimestamp() want %v but got %v", want, got)
	}
}
