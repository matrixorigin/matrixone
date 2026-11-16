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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSerialExtractEnum(t *testing.T) {
	values := []types.Enum{0, 1, 65535, 0}
	nulls := []bool{false, false, false, true}
	packed := make([]string, len(values))
	packer := types.NewPacker()
	defer packer.Close()
	for i, value := range values {
		packer.Reset()
		if nulls[i] {
			packer.EncodeNull()
		} else {
			packer.EncodeEnum(value)
		}
		packed[i] = string(packer.Bytes())
	}
	for _, encoding := range []uint32{types.EnumEncodingLegacy, types.EnumEncodingFramed} {
		for _, collation := range []uint32{0, types.EnumCollationUTF8Bin, types.EnumCollationGeneralCI} {
			for _, constantIndex := range []bool{false, true} {
				t.Run(fmt.Sprintf("encoding=%d/collation=%d/const=%v", encoding, collation, constantIndex), func(t *testing.T) {
					target := types.T_enum.ToType()
					target.SetEnumEncoding(encoding)
					target.SetEnumCollation(collation)
					index := NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 0, 0, 0}, nil)
					if constantIndex {
						index = NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil)
					}
					proc := testutil.NewProcess(t)
					fc := NewFunctionTestCase(proc, []FunctionTestInput{
						NewFunctionTestInput(types.T_varchar.ToType(), packed, nil),
						index,
						NewFunctionTestConstInput(target, []types.Enum{0}, nil),
					}, NewFunctionTestResult(target, false, values, nulls), builtInSerialExtract)
					t.Cleanup(func() {
						for _, parameter := range fc.parameters {
							parameter.Free(proc.Mp())
						}
						fc.result.Free()
					})
					require.NoError(t, fc.result.PreExtendAndReset(fc.fnLength))
					require.NoError(t, fc.fn(fc.parameters, fc.result, proc, fc.fnLength, nil))
					result := fc.GetResultVectorDirectly()
					require.Equal(t, target, *result.GetType())
					got := vector.MustFixedColWithTypeCheck[types.Enum](result)
					for i, want := range values {
						require.Equal(t, nulls[i], result.IsNull(uint64(i)))
						if !nulls[i] {
							require.Equal(t, want, got[i])
						}
					}
				})
			}
		}
	}
}

func TestSerialExtractEnumErrors(t *testing.T) {
	packer := types.NewPacker()
	packer.EncodeEnum(1)
	good := string(packer.Bytes())
	packer.Reset()
	packer.EncodeUint16(1)
	wrongType := string(packer.Bytes())
	packer.Close()

	for _, tc := range []struct {
		name   string
		packed string
		index  int64
	}{
		{name: "wrong tuple type", packed: wrongType, index: 0},
		{name: "negative index", packed: good, index: -1},
		{name: "index past tuple", packed: good, index: 1},
		{name: "malformed tuple", packed: "\x50", index: 0},
	} {
		for _, constantIndex := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/const=%v", tc.name, constantIndex), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				target := types.T_enum.ToType()
				index := NewFunctionTestInput(types.T_int64.ToType(), []int64{tc.index}, nil)
				if constantIndex {
					index = NewFunctionTestConstInput(types.T_int64.ToType(), []int64{tc.index}, nil)
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.packed}, nil),
					index,
					NewFunctionTestConstInput(target, []types.Enum{0}, nil),
				}, NewFunctionTestResult(target, true, []types.Enum{0}, nil), builtInSerialExtract)
				t.Cleanup(func() {
					for _, parameter := range fc.parameters {
						parameter.Free(proc.Mp())
					}
					fc.result.Free()
				})
				require.NoError(t, fc.result.PreExtendAndReset(fc.fnLength))
				require.Error(t, fc.fn(fc.parameters, fc.result, proc, fc.fnLength, nil))
			})
		}
	}
}
