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

package compile

import (
	"testing"

	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// stubCtx is a minimal compileplugin.CompileContext for the smoke
// tests. Each Hook is exercised just far enough to cover its
// entry-log line.
type stubCtx struct {
	isFrontend bool
}

func (s *stubCtx) Ctx() compileplugin.Context             { return nil }
func (s *stubCtx) Database() engine.Database              { return nil }
func (s *stubCtx) QryDatabase() string                    { return "" }
func (s *stubCtx) OriginalTableDef() *plan.TableDef       { return nil }
func (s *stubCtx) IndexInfo() *plan.CreateTable           { return nil }
func (s *stubCtx) MainTableID() uint64                    { return 0 }
func (s *stubCtx) MainExtra() *api.SchemaExtra            { return nil }
func (s *stubCtx) RunSql(_ string) error                  { return nil }
func (s *stubCtx) BuildIndexTable(_ *plan.TableDef) error { return nil }
func (s *stubCtx) ResolveVariable(_ string, _, _ bool) (any, error) {
	return int64(0), nil
}
func (s *stubCtx) IsFrontend() bool                             { return s.isFrontend }
func (s *stubCtx) IsTableClone() bool                           { return false }
func (s *stubCtx) IsExperimentalEnabled(_ string) (bool, error) { return true, nil }
func (s *stubCtx) IsCCPRTaskTransaction() bool                  { return false }
func (s *stubCtx) IsTableFromPublication(_ *plan.TableDef) bool { return false }
func (s *stubCtx) SinkerTypeFromAlgo(_ string) int8             { return 0 }
func (s *stubCtx) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	return nil
}
func (s *stubCtx) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	return nil
}
func (s *stubCtx) RunSqlWithResult(_ string) (executor.Result, error) {
	return executor.Result{}, nil
}
func (s *stubCtx) RegisterIdxcronUpdate(_ uint64, _, _, _, _ string, _ []byte) error {
	return nil
}

// TestHnswHandleCreateIndex_LogLine — drives HandleCreateIndex just
// far enough that its entry-log line fires; the static def-count check
// errors out, which is fine for the smoke purpose.
func TestHnswHandleCreateIndex_LogLine(t *testing.T) {
	err := Hooks{}.HandleCreateIndex(&stubCtx{isFrontend: true}, map[string]*plan.IndexDef{})
	require.Error(t, err)
}

// TestHnswHandleReindex_DelegatesToCreate — HandleReindex routes
// through HandleCreateIndex, so the same log line is covered.
func TestHnswHandleReindex_DelegatesToCreate(t *testing.T) {
	err := Hooks{}.HandleReindex(&stubCtx{isFrontend: true}, map[string]*plan.IndexDef{}, false)
	require.Error(t, err)
}

func TestHnswHandleDropIndex_LogLine(t *testing.T) {
	require.NoError(t, Hooks{}.HandleDropIndex(&stubCtx{}, map[string]*plan.IndexDef{}))
}

func TestHnswValidateReindexParams_Passthrough(t *testing.T) {
	old := map[string]string{"a": "1"}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{})
	require.NoError(t, err)
	require.Equal(t, old, got)
}

func TestHnswIdxcronMetadata_NoOp(t *testing.T) {
	got, err := Hooks{}.IdxcronMetadata(&stubCtx{})
	require.NoError(t, err)
	require.Nil(t, got)
}
