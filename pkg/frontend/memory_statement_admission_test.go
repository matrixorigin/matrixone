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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestStatementMemoryAdmissionClassificationAndContextLifetime(t *testing.T) {
	for _, stmt := range []tree.Statement{&tree.CommitTransaction{}, &tree.RollbackTransaction{}, &tree.Select{}} {
		proc := testutil.NewProcess(t)
		beforeProc := proc.Ctx
		beforeTop := proc.GetTopContext()
		beforeRequest := t.Context()
		e := &ExecCtx{reqCtx: beforeRequest, proc: proc, stmt: stmt}
		require.NoError(t, e.admitStatementMemory())
		_, commit := stmt.(*tree.CommitTransaction)
		_, rollback := stmt.(*tree.RollbackTransaction)
		require.Equal(t, commit || rollback, process.HasControlMemoryAdmission(e.reqCtx))
		require.Equal(t, commit || rollback, process.HasControlMemoryAdmission(proc.GetTopContext()))
		require.NoError(t, e.admitStatementMemory(), "the common execution path must not acquire twice")
		e.releaseStatementMemoryAdmission()
		e.releaseStatementMemoryAdmission()
		require.Same(t, beforeRequest, e.reqCtx)
		require.Same(t, beforeProc, proc.Ctx)
		require.Same(t, beforeTop, proc.GetTopContext())
		require.Nil(t, e.statementMemoryRelease)
	}
}

func TestStatementMemoryAdmissionReleasedAtGenerationAndRequestBoundaries(t *testing.T) {
	for _, closeRequest := range []bool{false, true} {
		calls := 0
		e := &ExecCtx{reqCtx: context.Background(), statementMemoryRelease: func() { calls++ }}
		if closeRequest {
			e.Close()
		} else {
			e.beginStatementGeneration(&UserInput{})
		}
		e.Close()
		require.Equal(t, 1, calls)
	}
}
