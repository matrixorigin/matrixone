// Copyright 2024 Matrix Origin
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

package hashbuild

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type intersectVectorsTestCase struct {
	vecs   []*vector.Vector
	result *vector.Vector
	proc   *process.Process
}

var (
	ivtcs []intersectVectorsTestCase
)

func init() {
	ivtcs = []intersectVectorsTestCase{
		newivTestCase(
			[][]int64{{7, 2, 4, 8, 10000000, -1}, {-5, -1, 5, 2, 100000, -111}, {3, 2, 0, -1, -100}},
			[]int64{-1, 2},
			types.T_int64.ToType(),
		),
		newivstrTestCase(
			[][]string{{"7", "2", "4", "8", "10000000", "-1"}, {"-5", "-1", "5", "2", "100000", "-111"}, {"3", "2", "0", "-1", "-100"}},
			[]string{"-1", "2"},
			types.T_varchar.ToType(),
		),
		newivTestCase(
			[][]float64{{7.2345, 2.1, 1234, 78.9, -10000000.00001, -1.999}, {-543.21, -1.999, 5.01, 2.1, 100000, -111}, {-543.21, 2.1, 0, -1.999, -100.01}},
			[]float64{-1.999, 2.1},
			types.T_float64.ToType(),
		),
	}
}

func TestIntersectVectors(t *testing.T) {
	for _, tc := range ivtcs {
		res := IntersectVectors(tc.vecs, tc.proc)
		require.Equal(t, bytes.Compare(res.UnsafeGetRawData(), tc.result.UnsafeGetRawData()), 0)
	}
}

func newivTestCase[T any](v [][]T, r []T, typ types.Type) intersectVectorsTestCase {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	vecs := make([]*vector.Vector, len(v))
	for i := range vecs {
		vecs[i] = vector.NewVec(typ)
		vector.AppendFixedList(vecs[i], v[i], nil, proc.Mp())
	}
	res := vector.NewVec(typ)
	vector.AppendFixedList(res, r, nil, proc.Mp())
	return intersectVectorsTestCase{
		vecs:   vecs,
		result: res,
		proc:   proc,
	}
}

func newivstrTestCase(v [][]string, r []string, typ types.Type) intersectVectorsTestCase {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	vecs := make([]*vector.Vector, len(v))
	for i := range vecs {
		vecs[i] = testutil.NewStringVector(0, typ, proc.Mp(), false, v[i])
	}
	res := testutil.NewStringVector(0, typ, proc.Mp(), false, r)
	return intersectVectorsTestCase{
		vecs:   vecs,
		result: res,
		proc:   proc,
	}
}
