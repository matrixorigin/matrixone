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

// The LOAD planners encode the inline escape byte in the flat ExternScan
// proto: byte 0 (escape processing disabled) when ESCAPED BY is omitted or
// empty, the explicit byte otherwise. The serialized source param leaves the
// field nil when the statement omitted the clause, where the reader would
// enable its default backslash escape and change accepted data. getExternParam
// must keep the established inline escape byte from the proto so inline loads
// stay byte-compatible with the pre-transport behavior.
func TestInlineLoadEscapeByte(t *testing.T) {
	cases := []struct {
		name        string
		escaped     *tree.EscapedBy // parser shape carried in the serialized source param
		protoEscape byte            // planner-encoded escape byte in ExternScan
		want        byte
	}{
		{
			name:        "omitted ESCAPED BY keeps escape processing disabled",
			escaped:     nil,
			protoEscape: 0,
			want:        0,
		},
		{
			name:        "explicit empty ESCAPED BY keeps escape processing disabled",
			escaped:     &tree.EscapedBy{},
			protoEscape: 0,
			want:        0,
		},
		{
			name:        "explicit custom escape byte is preserved",
			escaped:     &tree.EscapedBy{Value: '|'},
			protoEscape: '|',
			want:        '|',
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := "1,row\\8\n"
			src := &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INLINE,
					Format:   tree.CSV,
					Data:     payload,
					Tail: &tree.TailParameter{
						Fields: &tree.Fields{
							Terminated: &tree.Terminated{Value: ","},
							EscapedBy:  tc.escaped,
						},
					},
				},
			}
			// Mirror the transport: the payload travels once, in ExternScan.Data.
			src.Data = ""
			wire, err := json.Marshal(src)
			require.NoError(t, err)

			scan := &plan.ExternScan{
				Type:       int32(plan.ExternType_LOAD),
				LoadType:   int32(tree.INLINE),
				Data:       payload,
				Format:     tree.CSV,
				Terminated: ",",
				EnclosedBy: []byte{'"'},
				EscapedBy:  []byte{tc.protoEscape},
			}
			proc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			c := &Compile{proc: proc}
			got, err := c.getExternParam(proc, scan, string(wire))
			require.NoError(t, err)
			require.Equal(t, tree.INLINE, got.ScanType)
			require.Equal(t, payload, got.Data, "the inline payload is restored from ExternScan.Data")
			require.NotNil(t, got.Tail)
			require.NotNil(t, got.Tail.Fields,
				"a serialized source param without a FIELDS tail must not lose reader defaults")
			require.NotNil(t, got.Tail.Fields.EscapedBy,
				"the established inline escape byte must be restored from the proto, not left nil")
			require.Equal(t, tc.want, got.Tail.Fields.EscapedBy.Value)
			require.Equal(t, ",", got.Tail.Fields.Terminated.Value)
		})
	}
}
