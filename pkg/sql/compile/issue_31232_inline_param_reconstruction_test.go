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

package compile

import (
	"context"
	"encoding/json"
	"testing"

	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// inlineSourceParamWire mirrors the planning serialization of an inline
// LOAD's source param: the binary payload itself travels once in
// ExternScan.Data, so it is stripped from the wire payload; the consumed
// column-list and assignment trees stay out of the wire payload as well.
func inlineSourceParamWire(t *testing.T, param *tree.ExternParam) string {
	t.Helper()
	param.Data = ""
	if param.Tail != nil {
		param.Tail.ColumnList = nil
		param.Tail.Assignments = nil
	}
	payload, err := json.Marshal(param)
	require.NoError(t, err)
	return string(payload)
}

// inlineLoadExternScan mirrors the scan-node encoding the LOAD planners
// produce: the inline payload, format and derived reader options travel in the
// proto node's ExternScan.
func inlineLoadExternScan(param *tree.ExternParam) *plan.ExternScan {
	return &plan.ExternScan{
		Type:         int32(plan.ExternType_LOAD),
		LoadType:     int32(tree.INLINE),
		Data:         param.Data,
		Format:       param.Format,
		IgnoredLines: uint64(param.Tail.IgnoredLines),
		EnclosedBy:   []byte{'"'},
		Terminated:   ",",
		EscapedBy:    []byte{0},
		JsonType:     param.JsonData,
	}
}

// getExternParam must reconstruct an inline LOAD's reader-visible ExternParam
// from the serialized source param, preserving the parsed SET NULLIF
// sentinels, the LINES/FIELDS tail options, the format and the JSON type.
// Dropping any of them silently disables the option during execution.
func TestGetExternParamReconstructsInlineSourceParam(t *testing.T) {
	cases := []struct {
		name   string
		param  *tree.ExternParam
		verify func(t *testing.T, got *tree.ExternParam)
	}{
		{
			name: "csv nullif sentinel plus lines starting by",
			param: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INLINE,
					Format:   tree.CSV,
					Data:     "DATA:1,alpha,0\nDATA:2,beta,7\n",
					Tail: &tree.TailParameter{
						IgnoredLines: 1,
						Fields:       tree.NewFields(",", false, '"', 0),
						Lines:        tree.NewLines("DATA:", ""),
					},
				},
				ExParam: tree.ExParam{NullMap: map[string][]string{"flag": {"0"}}},
			},
			verify: func(t *testing.T, got *tree.ExternParam) {
				require.Equal(t, map[string][]string{"flag": {"0"}}, got.NullMap,
					"inline NULLIF sentinels reach execution")
				require.Equal(t, uint64(1), got.Tail.IgnoredLines)
				require.Equal(t, ",", got.Tail.Fields.Terminated.Value)
				require.NotNil(t, got.Tail.Lines)
				require.Equal(t, "DATA:", got.Tail.Lines.StartingBy,
					"inline LINES STARTING BY reaches execution")
				require.Equal(t, tree.CSV, got.Format)
				require.Empty(t, got.JsonData)
			},
		},
		{
			name: "csv multiple nullif sentinels",
			param: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INLINE,
					Format:   tree.CSV,
					Data:     "1,alpha,0\n2,beta,5\n",
					Tail:     &tree.TailParameter{Fields: tree.NewFields(",", false, '"', 0)},
				},
				ExParam: tree.ExParam{NullMap: map[string][]string{"flag": {"0", "5"}}},
			},
			verify: func(t *testing.T, got *tree.ExternParam) {
				require.Equal(t, map[string][]string{"flag": {"0", "5"}}, got.NullMap,
					"every NULLIF sentinel of a column survives the transport")
			},
		},
		{
			name: "jsonline nullif sentinel",
			param: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INLINE,
					Format:   tree.JSONLINE,
					Data:     "{\"id\":1,\"flag\":0}\n{\"id\":2,\"flag\":7}\n",
					Tail:     &tree.TailParameter{},
				},
				ExParam: tree.ExParam{JsonData: "object", NullMap: map[string][]string{"flag": {"0"}}},
			},
			verify: func(t *testing.T, got *tree.ExternParam) {
				require.Equal(t, tree.JSONLINE, got.Format)
				require.Equal(t, "object", got.JsonData)
				require.Equal(t, map[string][]string{"flag": {"0"}}, got.NullMap)
				require.NotNil(t, got.Tail)
			},
		},
		{
			name: "csv plain without options",
			param: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INLINE,
					Format:   tree.CSV,
					Data:     "1,alpha,0\n2,beta,7\n",
					Tail:     &tree.TailParameter{},
				},
			},
			verify: func(t *testing.T, got *tree.ExternParam) {
				require.Empty(t, got.NullMap)
				require.Nil(t, got.Tail.Lines)
				require.Equal(t, uint64(0), got.Tail.IgnoredLines)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scan := inlineLoadExternScan(tc.param)
			wire := inlineSourceParamWire(t, tc.param)
			proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			c := &Compile{proc: proc}
			got, err := c.getExternParam(proc, scan, wire)
			require.NoError(t, err)
			require.Equal(t, tree.INLINE, got.ScanType)
			require.Equal(t, scan.Data, got.Data, "the inline payload is restored from ExternScan.Data")
			tc.verify(t, got)
		})
	}
}
