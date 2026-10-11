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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A JSONLINE record that is still incomplete at end of file must fail the
// strict scan instead of being silently dropped (issue #29842).
func TestJSONLineIncompleteRecordAtEOFFails(t *testing.T) {
	for name, content := range map[string]string{
		"complete then truncated":      `{"a":1,"s":"x"}` + "\n" + `{"a":2,"s":"y"`,
		"complete then truncated + nl": `{"a":1,"s":"x"}` + "\n" + `{"a":2,"s":"y"` + "\n",
		"only truncated":               `{"a":2,"s":"y"`,
		"complete then malformed":      `{"a":1,"s":"x"}` + "\n" + `{"a":2,"s":{invalid}}`,
	} {
		t.Run(name, func(t *testing.T) {
			param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
			err := readAllText(t, param, proc, bat, content)
			require.Error(t, err)
			require.Contains(t, err.Error(), "incomplete json record")
		})
	}

	t.Run("complete records still load", func(t *testing.T) {
		param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, colLine)
		require.NoError(t, readAllText(t, param, proc, bat, `{"a":1,"s":"x"}`+"\n"+`{"a":2,"s":"y"}`))
		require.Equal(t, 2, bat.RowCount())
	})

	t.Run("tolerant scan reports it as a row", func(t *testing.T) {
		param, proc, bat := errorModeParam(t, tree.JSONLINE, tree.OBJECT, numTestCols)
		require.NoError(t, readAllText(t, param, proc, bat, `{"a":1,"s":"x"}`+"\n"+`{"a":2,"s":"y"`))
		require.Equal(t, 2, bat.RowCount())
	})
}
