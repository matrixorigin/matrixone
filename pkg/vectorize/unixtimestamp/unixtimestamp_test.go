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

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func mustDatetime(s string) types.Datetime {
	datetime, err := types.ParseDatetime(s)
	if err != nil {
		panic("bad datetime")
	}
	return datetime
}

func TestUnixTimestamp(t *testing.T) {

	xs := []types.Datetime{
		mustDatetime("2022-01-01 22:23:00"),
		mustDatetime("2022-01-02 22:23:00"),
		mustDatetime("2022-01-03 22:23:00"),
	}
	rs := make([]int64, 3)

	// UTC-8
	want := []int64{1641075780, 1641162180, 1641248580}

	got := unixTimestamp(xs, rs)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unixtimestamp() want %v but got %v", want, got)
	}
}
