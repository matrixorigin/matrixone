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

package function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/stretchr/testify/require"
)

func TestFunctionOverloadID(t *testing.T) {
	tcs := []struct {
		fid        int32
		overloadId int32
	}{
		{fid: 0, overloadId: 0},
		{fid: 1, overloadId: 10},
		{fid: 10, overloadId: 15},
		{fid: 400, overloadId: 1165},
		{fid: 3004, overloadId: 12345},
	}
	for _, tc := range tcs {
		f := EncodeOverloadID(tc.fid, tc.overloadId)
		actualF, actualO := DecodeOverloadID(f)
		require.Equal(t, tc.fid, actualF)
		require.Equal(t, tc.overloadId, actualO)
	}
}

func TestToPrintCastTable(t *testing.T) {
	println("[Implicit Type Convert Rule for +, -, *, >, = and so on:]")
	for i, rs := range binaryTable {
		for j, r := range rs {
			if r.convert {
				println(fmt.Sprintf("%s + %s ===> %s + %s",
					types.T(i).OidString(), types.T(j).OidString(),
					r.left.OidString(), r.right.OidString()))
			}
		}
	}

	for i := 0; i < 5; i++ {
		fmt.Println()
	}

	println("[Implicit Type Convert Rule for div and / :]")
	for i, rs := range binaryTable2 {
		for j, r := range rs {
			if r.convert {
				println(fmt.Sprintf("%s / %s ===> %s / %s",
					types.T(i).OidString(), types.T(j).OidString(),
					r.left.OidString(), r.right.OidString()))
			}
		}
	}

	for i := 0; i < 5; i++ {
		fmt.Println()
	}

	println("[Implicit type conversions that we support :]")
	for t1, t := range castTable {
		for t2, k := range t {
			if k {
				str := fmt.Sprintf("%s ==> %s",
					types.T(t1).OidString(), types.T(t2).OidString())
				if preferredTypeConvert[t1][t2] {
					str += " (preferred)"
				}
				println(str)
			}
		}
	}
}
