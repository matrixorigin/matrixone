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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestLength(t *testing.T) {
	makeTempVector := func(src string, t types.T, srcIsScalar bool) []*vector.Vector {
		vectors := make([]*vector.Vector, 1)
		if srcIsScalar {
			vectors[0] = vector.NewConstString(t.ToType(), 1, src, testutil.TestUtilMp)
		} else {
			vectors[0] = vector.NewWithStrings(t.ToType(), []string{src}, nil, testutil.TestUtilMp)
		}
		return vectors
	}

	procs := testutil.NewProcess()

	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []int64
		wantScalar bool
	}{
		{
			name:       "Test01",
			vecs:       makeTempVector("abcdefghijklm", types.T_varchar, true),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: true,
		},
		{
			name:       "Test02",
			vecs:       makeTempVector("abcdefghijklm", types.T_char, true),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: true,
		},
		{
			name:       "Test03",
			vecs:       makeTempVector("abcdefghijklm", types.T_varchar, false),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: false,
		},
		{
			name:       "Test04",
			vecs:       makeTempVector("abcdefghijklm", types.T_char, false),
			proc:       procs,
			wantBytes:  []int64{13},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lengthRes, err := Length(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, lengthRes.Col)
			require.Equal(t, c.wantScalar, lengthRes.IsScalar())

		})
	}
}

func TestBlobLength(t *testing.T) {
	makeBlobVector := func(src []byte, srcIsScalar bool, procs *process.Process) *vector.Vector {
		inputType := types.New(types.T_blob, 0, 0)
		var inputVector *vector.Vector
		if srcIsScalar {
			inputVector = vector.NewConst(inputType, 1)
		} else {
			inputVector = vector.New(inputType)
		}
		err := inputVector.Append(src, false, procs.Mp())
		if err != nil {
			t.Fatal(err)
		}
		return inputVector
	}

	procs := testutil.NewProcess()
	cases := []struct {
		name     string
		ctx      []byte
		want     []int64
		isScalar bool
	}{
		{
			name:     "Normal Case",
			ctx:      []byte("12345678"),
			want:     []int64{8},
			isScalar: false,
		},
		{
			name:     "Empty Case",
			ctx:      []byte(""),
			want:     []int64{0},
			isScalar: false,
		},
		{
			name:     "Scalar Case",
			ctx:      []byte("12345678"),
			want:     []int64{8},
			isScalar: true,
		},
	}
	for _, c := range cases {
		convey.Convey(c.name, t, func() {
			res, err := Length([]*vector.Vector{makeBlobVector(c.ctx, c.isScalar, procs)}, procs)
			convey.So(err, convey.ShouldBeNil)
			convey.So(res.Col, convey.ShouldResemble, c.want)
			convey.So(res.IsScalar(), convey.ShouldEqual, c.isScalar)
		})
	}

}
