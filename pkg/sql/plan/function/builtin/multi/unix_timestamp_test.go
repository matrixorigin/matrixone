// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi

import (
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestUnixTimestamp(t *testing.T) {
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-01-01 22:23:00"), 1641075780, false)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-01-02 22:23:00"), 1641162180, false)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-01-03 22:23:00"), 1641248580, false)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2022-02-29 22:23:00"), 0, true)
	UnixtimeCase(t, types.T_int64, MustTimestamp(time.UTC, "2050-02-29 22:23:00"), 0, true)
}

// func FromUnixTime(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error)
func UnixtimeCase(t *testing.T, typ types.T, src types.Timestamp, res int64, isNull bool) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantScalar bool
		wantNull   bool
	}{
		{
			name:       "TEST01",
			vecs:       makeVector2(src, true, typ),
			proc:       procs,
			wantBytes:  []int64{res},
			wantScalar: true,
			wantNull:   isNull,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := UnixTimestamp(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.wantBytes, plus.Col) {
				t.Errorf("unixtimestamp() want %v but got %v", c.wantBytes, plus.Col)
			}
			require.Equal(t, c.wantNull, plus.ConstVectorIsNull())
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

func makeVector2(src types.Timestamp, srcScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 1)
	vectors[0] = vector.NewConstFixed(types.T_timestamp.ToType(), 1, src, testutil.TestUtilMp)
	return vectors
}
