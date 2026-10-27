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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestLoadFieldDelimiters(t *testing.T) {
	tests := []struct {
		name              string
		fields            *tree.Fields
		terminated        string
		enclosed, escaped []byte
	}{
		{name: "nil", terminated: ",", enclosed: []byte{'"'}, escaped: []byte{0}},
		{name: "empty", fields: &tree.Fields{}, terminated: ",", enclosed: []byte{'"'}, escaped: []byte{0}},
		{name: "zero bytes and empty terminator", fields: &tree.Fields{Terminated: &tree.Terminated{}, EnclosedBy: &tree.EnclosedBy{}, EscapedBy: &tree.EscapedBy{}}, terminated: "", enclosed: []byte{'"'}, escaped: []byte{0}},
		{name: "custom", fields: &tree.Fields{Optionally: true, Terminated: &tree.Terminated{Value: "||"}, EnclosedBy: &tree.EnclosedBy{Value: '\''}, EscapedBy: &tree.EscapedBy{Value: '\\'}}, terminated: "||", enclosed: []byte{'\''}, escaped: []byte{'\\'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terminated, enclosed, escaped := loadFieldDelimiters(tt.fields)
			require.Equal(t, tt.terminated, terminated)
			require.Equal(t, tt.enclosed, enclosed)
			require.Equal(t, tt.escaped, escaped)
		})
	}
}
