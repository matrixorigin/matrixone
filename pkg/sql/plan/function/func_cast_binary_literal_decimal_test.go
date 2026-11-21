// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestCastBinaryLiteralDecimalFromByteContract drives the REAL string-to-decimal
// cast executor (strToDecimal64/128/256) over non-constant IsBin vectors on
// all three widths (#31816): surviving rows keep the unsigned big-endian
// integer at the destination scale, NULL rows stay NULL, a select-list masked
// row is never decoded, an out-of-range payload propagates the cast's own
// error (never a 1264-family diagnostic), and the same entry converts a valid
// payload again after that failure.
func TestCastBinaryLiteralDecimalFromByteContract(t *testing.T) {
	const p150 = "\x00\x00\x00\x96"                     // N=150 -> 15000 at scale 2
	const p65 = "\x41"                                  // N=65 -> 6500 at scale 2
	const pHighBit = "\xff\xff\xff\xff\xff\xff\xfe\xcd" // 2^64-307

	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			var width int32 = 10
			switch oid {
			case types.T_decimal128:
				width = 20
			case types.T_decimal256:
				width = 38
			}
			target := types.New(oid, width, 2)

			// The payload that must be rejected when actually decoded is
			// width-specific: 2^64-307 exceeds every DECIMAL64 budget and
			// the DECIMAL(20,2) coefficient bound; 33 significant bytes
			// exceed the 256-bit carrier itself.
			rejected := pHighBit
			if oid == types.T_decimal256 {
				rejected = "\x01" + strings.Repeat("\x00", 32)
			}

			var wanted, reuseWant any
			switch oid {
			case types.T_decimal64:
				wanted = []types.Decimal64{15000, 6500, 0, 0}
				reuseWant = []types.Decimal64{6500}
			case types.T_decimal128:
				wanted = []types.Decimal128{{B0_63: 15000}, {B0_63: 6500}, {}, {}}
				reuseWant = []types.Decimal128{{B0_63: 6500}}
			case types.T_decimal256:
				wanted = []types.Decimal256{{B0_63: 15000}, {B0_63: 6500}, {}, {}}
				reuseWant = []types.Decimal256{{B0_63: 6500}}
			}

			// Row 3 is masked: the select-list contract forbids decoding it,
			// so carrying a payload that would error keeps the proof strict.
			pass := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_binary.ToType(),
					[]string{p150, p65, "\x00", rejected}, []bool{false, false, true, false}),
				NewFunctionTestInput(target, emptyCastTargetValues(target), nil),
			}, NewFunctionTestResult(target, false, wanted, []bool{false, false, true, true}), NewCast).
				WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}})
			t.Cleanup(pass.Free)
			pass.parameters[0].SetIsBin(true)
			ok, info := pass.Run()
			require.True(t, ok, info)

			fail := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_binary.ToType(), []string{rejected}, nil),
				NewFunctionTestInput(target, emptyCastTargetValues(target), nil),
			}, NewFunctionTestResult(target, false, nil, nil), NewCast)
			t.Cleanup(fail.Free)
			fail.parameters[0].SetIsBin(true)
			_, err := fail.DebugRun()
			require.Error(t, err, "the out-of-range payload is the decoder's own rejection, never a policy clip")
			require.False(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRangeValueForColumn),
				"decode failures are the cast's diagnostics, never a misclassified 1264")

			reuse := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_binary.ToType(), []string{p65}, nil),
				NewFunctionTestInput(target, emptyCastTargetValues(target), nil),
			}, NewFunctionTestResult(target, false, reuseWant, nil), NewCast)
			t.Cleanup(reuse.Free)
			reuse.parameters[0].SetIsBin(true)
			ok2, info2 := reuse.Run()
			require.True(t, ok2, info2)
		})
	}
}
