// Copyright 2026 Matrix Origin
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

package external

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// In strict SQL mode (non-LOCAL load), getColData must reject over-width
// CHAR/VARCHAR values for both parallel and non-parallel LOAD; LOCAL or
// non-strict loads keep the lenient truncate/accept behavior.
func Test_getColData_StringWidthStrict(t *testing.T) {
	cases := []struct {
		name          string
		oid           types.T
		width         int32
		val           string
		strictSqlMode bool
		local         bool
		parallel      bool
		wantErr       bool
	}{
		{"varchar over-width strict serial", types.T_varchar, 3, "abcd", true, false, false, true},
		{"varchar over-width strict parallel", types.T_varchar, 3, "abcd", true, false, true, true},
		{"char over-width strict serial", types.T_char, 3, "abcd", true, false, false, true},
		{"char over-width strict parallel", types.T_char, 3, "abcd", true, false, true, true},
		{"varchar fits strict serial", types.T_varchar, 3, "abc", true, false, false, false},
		{"varchar fits strict parallel", types.T_varchar, 3, "abc", true, false, true, false},
		{"varchar over-width non-strict", types.T_varchar, 3, "abcd", false, false, false, false},
		{"varchar over-width local", types.T_varchar, 3, "abcd", true, true, false, false},
		{"varchar multibyte over-width strict", types.T_varchar, 2, "你好世", true, false, false, true},
		{"varchar multibyte fits strict", types.T_varchar, 2, "你好", true, false, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			colType := plan.Type{Id: int32(tc.oid), Width: tc.width}

			bat := batch.New([]string{"a"})
			bat.Vecs[0] = vector.NewVec(types.New(tc.oid, tc.width, 0))

			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx:           context.Background(),
					ParallelLoad:  tc.parallel,
					StrictSqlMode: tc.strictSqlMode,
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{
							Format: tree.CSV,
						},
						ExParam: tree.ExParam{
							Local: tc.local,
						},
					},
					Cols: []*plan.ColDef{{Name: "a", Typ: colType}},
				},
			}

			line := []csvparser.Field{{Val: tc.val}}
			attr := plan.ExternAttr{ColName: "a", ColIndex: 0, ColFieldIndex: 0}

			err := getColData(bat, line, 0, param, proc.GetMPool(), attr, proc)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "Data too long for column 'a'")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
