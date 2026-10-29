// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExportParamStringClauses pins the clause rendering used for audit and
// stage-dump bookkeeping: an unset ESCAPED BY (grammar default: not written)
// must not panic or print, while an explicit one (including the disabling
// empty value) renders; LINES STARTING BY renders only when set.
func TestExportParamStringClauses(t *testing.T) {
	t.Run("defaults without escaped-by or starting-by", func(t *testing.T) {
		ep := &ExportParam{
			Fields: &Fields{
				Terminated: &Terminated{Value: ","},
				EnclosedBy: &EnclosedBy{Value: '"'},
			},
			Lines: &Lines{TerminatedBy: &Terminated{Value: "\n"}},
		}
		require.Equal(t, "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'", ep.String())
	})

	t.Run("explicit escaped and starting by", func(t *testing.T) {
		ep := &ExportParam{
			Fields: &Fields{
				Terminated: &Terminated{Value: "|"},
				EnclosedBy: &EnclosedBy{Value: 0},
				EscapedBy:  &EscapedBy{Value: '!'},
			},
			Lines: &Lines{StartingBy: ">>", TerminatedBy: &Terminated{Value: "\n"}},
		}
		// NB: the literal renderer quotes rune-by-rune (pre-existing style).
		require.Equal(t, "FIELDS TERMINATED BY '|' ENCLOSED BY '' ESCAPED BY '!' LINES STARTING BY '>''>' LINES TERMINATED BY '\\n'", ep.String())
	})

	t.Run("escaped by empty renders as disabling", func(t *testing.T) {
		ep := &ExportParam{
			Fields: &Fields{
				Terminated: &Terminated{Value: ","},
				EnclosedBy: &EnclosedBy{Value: '"'},
				EscapedBy:  &EscapedBy{Value: 0},
			},
			Lines: &Lines{TerminatedBy: &Terminated{Value: "\n"}},
		}
		require.Contains(t, ep.String(), "ESCAPED BY ''")
	})

	t.Run("nil sub-structs never panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			_ = (&ExportParam{Fields: &Fields{}, Lines: &Lines{}}).String()
			_ = (&ExportParam{}).String()
		})
	})
}
