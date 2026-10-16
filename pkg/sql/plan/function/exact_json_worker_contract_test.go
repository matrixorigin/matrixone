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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// This file can run unchanged in the pre-feature worktree. It supplies the
// existing decimal tag directly, without calling the new exact parser, so its
// old/new worker oracle is independent of ingestion and capability detection.
func TestExactJSONWorkerWireCast(t *testing.T) {
	for _, tc := range []struct{ text string }{{"9007199254740993.001"}, {"0.125"}, {"-9007199254740993.001"}} {
		t.Run(tc.text, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			payload := binary.AppendUvarint(nil, uint64(len(tc.text)))
			payload = append(payload, tc.text...)
			encoded, err := types.EncodeJson(bytejson.ByteJson{Type: bytejson.TpCodeDecimal, Data: payload})
			require.NoError(t, err)
			require.Equal(t, tc.text, types.DecodeJson(encoded).String())
			target := types.New(types.T_decimal128, 38, 3)
			want, err := types.ParseDecimal128(tc.text, 38, 3)
			require.NoError(t, err)
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{string(encoded)}, nil),
				NewFunctionTestInput(target, []types.Decimal128{}, nil),
			}, NewFunctionTestResult(target, false, []types.Decimal128{want}, nil), NewCast)
			ok, info := test.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

func TestExactJSONWorkerWireFunctionIdentity(t *testing.T) {
	proc := testutil.NewProcess(t)
	_, err := GetFunctionById(proc.Ctx, int64(601)<<32)
	require.NoError(t, err)
}
