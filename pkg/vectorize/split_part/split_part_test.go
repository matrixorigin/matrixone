// Copyright 2021 - 2022 Matrix Origin
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

package split_part

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"testing"
)

var (
	mp         = testutil.TestUtilMp
	v1         = testutil.NewStringVector(10, types.T_varchar.ToType(), mp, false, []string{"a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c"})
	v2         = testutil.NewStringVector(10, types.T_varchar.ToType(), mp, false, []string{",", ",", ",", ",", ",", ",", ",", ",", ",", ","})
	v3         = testutil.NewUInt32Vector(10, types.T_uint32.ToType(), mp, false, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	s1, s2, s3 = vector.MustStrCols(v1), vector.MustStrCols(v2), vector.MustTCols[uint32](v3)
)

func TestSplitSingle(t *testing.T) {
	kases := []struct {
		str    string
		sep    string
		cnt    uint32
		res    string
		isnull bool
	}{
		{"a,b,c", ",", 1, "a", false},
		{"a,b,c", ",", 2, "b", false},
		{"a,b,c", ",", 3, "c", false},
		{"a,b,c", ",", 4, "", true},
		{"axbxc", "x", 1, "a", false},
		{"axbxc", "bxc", 2, "", true},
		{"axbxc", "bxc", 1, "ax", false},
		{"axbxc", "bxcd", 1, "axbxc", false},
		{"axbxc", "bxcd", 2, "", true},
		{"啊啊啊x", "x", 1, "啊啊啊", false},
		{"啊啊啊x", "x", 2, "", true},
		{"1啊啊x啊x", "啊啊", 1, "1", false},
		{"1啊啊x啊x", "啊啊", 2, "x啊x", false},
		{"axbcd", "a", 2, "xbcd", false},
	}
	for _, k := range kases {
		res, isNull := SplitSingle(k.str, k.sep, k.cnt)
		if res != k.res || isNull != k.isnull {
			t.Errorf("expected %v, %v, got %v, %v", k.res, k.isnull, res, isNull)
		}
	}
}

func TestSplitPart(t *testing.T) {
	rs := make([]string, len(s1))
	nsp := nulls.NewWithSize(len(s1))
	SplitPart1(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart2(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart3(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart4(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart5(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart6(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart7(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	v1.Nsp.Set(0)
	v2.Nsp.Set(0)
	v3.Nsp.Set(0)
	v1.Nsp.Set(5)
	v2.Nsp.Set(5)
	v3.Nsp.Set(5)
	SplitPart1(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart2(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart3(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart4(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart5(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart6(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
	SplitPart7(s1, s2, s3, []*nulls.Nulls{v1.Nsp, v2.Nsp, v3.Nsp}, rs, nsp)
}
