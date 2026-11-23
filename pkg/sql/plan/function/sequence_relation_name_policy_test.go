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

package function

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The normalization boundary must follow the session's real
// lower_case_table_names policy exactly as the parser applies it to CREATE
// identifiers (#31909): the mysql lexer folds identifiers only under mode 1
// (GetDbOrTblName), so mode 0 and the mixed mode 2 keep the authored spelling,
// and an unavailable / non-int64 / failing resolver falls back to the
// frontend default (1).
func TestSequenceRelationNamePolicy(t *testing.T) {
	newProc := func(t *testing.T) *process.Process {
		proc := testutil.NewProcess(t)
		return proc
	}

	t.Run("case-sensitive mode 0 keeps the name", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(0))
		require.Equal(t, "MySeq", sequenceRelationName(proc, "MySeq"))
	})

	t.Run("mode 1 lowercases", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(1))
		require.Equal(t, "myseq", sequenceRelationName(proc, "MySeq"))
	})

	t.Run("mode 2 keeps the name like CREATE (folding is mode-1-only)", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		proc.SetResolveVariableFunc(sequenceCasePolicyResolver(2))
		require.Equal(t, "MySeq", sequenceRelationName(proc, "MySeq"))
	})

	t.Run("no resolver defaults to the frontend fallback 1", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		require.Equal(t, "myseq", sequenceRelationName(proc, "MySeq"))
	})

	t.Run("resolver error falls back to mode 1", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		proc.SetResolveVariableFunc(func(name string, isSystem, isGlobal bool) (interface{}, error) {
			return nil, errors.New("sysvar unavailable")
		})
		require.Equal(t, "myseq", sequenceRelationName(proc, "MySeq"))
	})

	t.Run("non-int64 value falls back to mode 1", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		proc.SetResolveVariableFunc(func(name string, isSystem, isGlobal bool) (interface{}, error) {
			return "1", nil
		})
		require.Equal(t, "myseq", sequenceRelationName(proc, "MySeq"))
	})

	t.Run("empty name stays empty", func(t *testing.T) {
		proc := newProc(t)
		defer proc.Free()
		require.Equal(t, "", sequenceRelationName(proc, ""))
	})
}
