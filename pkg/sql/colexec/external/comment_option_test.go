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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestNewCSVParserCommentOption verifies that the external table's COMMENT
// option flows into the CSV reader: absent => no comment marker (every line is
// data), set => lines whose raw prefix matches the marker are skipped.
func TestNewCSVParserCommentOption(t *testing.T) {
	mkParam := func(comment string) *tree.ExternParam {
		opt := []string{"format", "csv"}
		if comment != "" {
			opt = append(opt, "comment", comment)
		}
		return &tree.ExternParam{
			ExParamConst: tree.ExParamConst{Format: tree.CSV, Option: opt, Tail: &tree.TailParameter{}},
		}
	}

	readAll := func(comment, input string) [][]string {
		p, err := newCSVParserFromReader(mkParam(comment), strings.NewReader(input))
		require.NoError(t, err)
		var rows [][]string
		for {
			row, err := p.Read(nil)
			if err != nil {
				break
			}
			vals := make([]string, len(row))
			for i := range row {
				// Read reuses an internal buffer that Val aliases via an unsafe
				// conversion, so clone before the next Read overwrites it.
				vals[i] = strings.Clone(row[i].Val)
			}
			rows = append(rows, vals)
		}
		return rows
	}

	input := "#c1,c2\n1,a\nREMx,REMy\n2,b\n"

	// default (no comment option): every line is data
	require.Len(t, readAll("", input), 4)

	// comment '#': the '#c1,c2' line is skipped
	rows := readAll("#", input)
	require.Len(t, rows, 3)
	require.Equal(t, "1", rows[0][0])

	// comment 'REM': the 'REMx,REMy' line is skipped, '#'-lines stay data
	rows = readAll("REM", input)
	require.Len(t, rows, 3)
	require.Equal(t, "#c1", rows[0][0])

	// an enclosed value beginning with the marker is data, not a comment
	rows = readAll("#", "\"#x\",1\n2,b\n")
	require.Len(t, rows, 2)
	require.Equal(t, "#x", rows[0][0])
}
