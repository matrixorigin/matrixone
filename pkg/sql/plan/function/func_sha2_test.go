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
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSHA2PreservesWideStringInputs(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	tests := []struct {
		name      string
		inputType types.Type
		input     string
		algorithm int64
	}{
		{
			name:      "longtext over varchar limit",
			inputType: types.New(types.T_text, types.MaxLongTextLen, 0),
			input:     strings.Repeat("x", 160000),
			algorithm: 256,
		},
		{
			name:      "mediumtext just over varchar limit",
			inputType: types.New(types.T_text, types.MaxMediumTextLen, 0),
			input:     strings.Repeat("a", 65535) + "b",
			algorithm: 512,
		},
		{
			name:      "blob preserves binary bytes",
			inputType: types.New(types.T_blob, types.MaxBlobLen, 0),
			input:     string([]byte(strings.Repeat("\x00\x01\x02\x03", 20000))),
			algorithm: 224,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
				test.inputType,
				types.T_int64.ToType(),
			})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Empty(t, targets)

			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(test.inputType, []string{test.input}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{test.algorithm}, nil),
				},
				NewFunctionTestResult(
					types.T_varchar.ToType(),
					false,
					[]string{sha2TestDigest(test.input, test.algorithm)},
					nil,
				),
				SHA2Func,
			)
			succeeded, info := testCase.Run()
			require.True(t, succeeded, info)
		})
	}
}

func TestSHA2StillFormatsScalarInput(t *testing.T) {
	resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	})
	require.NoError(t, err)
	targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Len(t, targets, 2)
	require.Equal(t, types.T_varchar, targets[0].Oid)
	require.Equal(t, int32(20), targets[0].Width)
	require.Equal(t, types.T_int64, targets[1].Oid)
}

func TestSHA2PreservesEveryMySQLStringDomain(t *testing.T) {
	for _, oid := range []types.T{
		types.T_char,
		types.T_varchar,
		types.T_binary,
		types.T_varbinary,
		types.T_blob,
		types.T_text,
	} {
		t.Run(oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
				oid.ToType(),
				types.T_int64.ToType(),
			})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Empty(t, targets)
		})
	}
}

func sha2TestDigest(input string, algorithm int64) string {
	var digest []byte
	switch algorithm {
	case 224:
		sum := sha256.Sum224([]byte(input))
		digest = sum[:]
	case 256:
		sum := sha256.Sum256([]byte(input))
		digest = sum[:]
	case 384:
		sum := sha512.Sum384([]byte(input))
		digest = sum[:]
	case 512:
		sum := sha512.Sum512([]byte(input))
		digest = sum[:]
	default:
		panic("unsupported SHA2 test algorithm")
	}
	return hex.EncodeToString(digest)
}
