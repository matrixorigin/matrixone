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

package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
)

// subCompileCtx mocks the CompileContext a CREATE TABLE ... CLONE (and
// snapshot/restore, which replays as a table clone) spawns: a frontend
// sub-Compile whose IsFrontend stays true but whose sysvar resolution falls
// back to the background resolver executor.DefaultResolveVariable — the worst
// case the IsFrontend doc-comment calls out ("background paths set resolvers
// too … ProcessInitSQL's executor.DefaultResolveVariable").
//
// Only ResolveVariable / IsFrontend / IsTableClone / OriginalTableDef carry
// real behavior; the remaining 16 CompileContext methods are zero-value stubs
// (BuildIdxcronMetadata never calls them).
type subCompileCtx struct {
	tableDef       *plan.TableDef
	registeredMeta []byte // captured by RegisterIdxcronUpdate, if ever called
}

func (c *subCompileCtx) ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error) {
	// The real background resolver wired by frontend's init().
	return executor.DefaultResolveVariable(name, isSystemVar, isGlobalVar)
}
func (c *subCompileCtx) IsFrontend() bool                 { return true }
func (c *subCompileCtx) IsTableClone() bool               { return true }
func (c *subCompileCtx) OriginalTableDef() *plan.TableDef { return c.tableDef }

func (c *subCompileCtx) RegisterIdxcronUpdate(_ uint64, _, _, _, _ string, metadata []byte) error {
	c.registeredMeta = metadata
	return nil
}

// --- zero-value stubs (unused by BuildIdxcronMetadata) ---
func (c *subCompileCtx) Ctx() compileplugin.Context                 { return nil }
func (c *subCompileCtx) Database() engine.Database                  { return nil }
func (c *subCompileCtx) QryDatabase() string                        { return "" }
func (c *subCompileCtx) IndexInfo() *plan.CreateTable               { return nil }
func (c *subCompileCtx) MainTableID() uint64                        { return 0 }
func (c *subCompileCtx) MainExtra() *api.SchemaExtra                { return nil }
func (c *subCompileCtx) RunSql(string) error                        { return nil }
func (c *subCompileCtx) BuildIndexTable(*plan.TableDef) error       { return nil }
func (c *subCompileCtx) IsExperimentalEnabled(string) (bool, error) { return true, nil }
func (c *subCompileCtx) IsCCPRTaskTransaction() bool                { return false }
func (c *subCompileCtx) IsTableFromPublication(*plan.TableDef) bool { return false }
func (c *subCompileCtx) SinkerTypeFromAlgo(string) int8             { return 0 }
func (c *subCompileCtx) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	return nil
}
func (c *subCompileCtx) DropIndexCdcTask(*plan.TableDef, string, string, string) error { return nil }
func (c *subCompileCtx) RunSqlWithResult(string) (executor.Result, error) {
	return executor.Result{}, nil
}

var _ compileplugin.CompileContext = (*subCompileCtx)(nil)

// idxcronSpecs mirror the per-algorithm IdxcronVarSpec each vector plugin
// declares (cagra/ivfpq/ivfflat plugin/compile). Reconstructed here (the real
// specs are unexported) to assert the contract end-to-end against the real
// resolver.
var idxcronSpecs = []struct {
	name         string
	spec         compileplugin.IdxcronVarSpec
	wantContains string
}{
	{"cagra", compileplugin.IdxcronVarSpec{
		FrontendProbeVar: "cagra_threads_search",
		Capture:          []string{"cagra_threads_build", "lower_case_table_names"},
	}, "cagra_threads_build"},
	{"ivfflat", compileplugin.IdxcronVarSpec{
		FrontendProbeVar: "ivf_threads_search",
		Capture:          []string{"ivf_threads_build", "lower_case_table_names"},
	}, "ivf_threads_build"},
	{"ivfpq", compileplugin.IdxcronVarSpec{
		FrontendProbeVar: "ivfpq_threads_search",
		Capture:          []string{"ivfpq_threads_build", "lower_case_table_names"},
	}, "ivfpq_threads_build"},
}

// TestIdxcronMetadata_CloneSubCompileRegisters proves the clone/restore path
// does NOT drop its idxcron registration. The FrontendProbeVar skip in
// BuildIdxcronMetadata fires only when the probe sysvar resolves to a literal
// nil (or errors). But every vector-index probe var is a REGISTERED system
// variable, so even the most degraded resolver a clone sub-Compile can inherit
// — the background executor.DefaultResolveVariable — returns its non-nil
// default. So the probe passes, the capture runs, and a non-nil metadata blob
// is produced: CreateAllIndexUpdateTasks registers the mo_index_update row
// instead of skipping it.
//
// This is the realistic counterpart to the cagra plugin's stub-driven
// TestCagraIdxcronMetadata_ProbeFail (which can only reach the skip by forcing
// a literal nil that no real resolver returns).
func TestIdxcronMetadata_CloneSubCompileRegisters(t *testing.T) {
	require.NotNil(t, executor.DefaultResolveVariable,
		"frontend init() must wire the background resolver")

	ctx := &subCompileCtx{tableDef: &plan.TableDef{Name: "t_copy"}}

	for _, tc := range idxcronSpecs {
		t.Run(tc.name, func(t *testing.T) {
			// The probe resolves to its non-nil default through the real
			// background resolver (not nil → no skip).
			probe, err := ctx.ResolveVariable(tc.spec.FrontendProbeVar, true, false)
			require.NoError(t, err)
			require.NotNil(t, probe, "%s probe must be non-nil", tc.spec.FrontendProbeVar)

			md, err := compileplugin.BuildIdxcronMetadata(ctx, tc.spec)
			require.NoError(t, err)
			require.NotEmpty(t, md,
				"%s: clone sub-Compile must produce metadata, not skip registration", tc.name)
			require.Contains(t, string(md), tc.wantContains)
		})
	}
}

// TestIdxcronMetadata_SkipOnlyOnUnresolvableProbe pins the other side of the
// contract: the skip is reserved for a probe the resolver genuinely cannot
// surface (errors / nil). A registered sysvar never lands here — only a name
// with no definition does — so the skip cannot silently swallow a real
// clone's registration.
func TestIdxcronMetadata_SkipOnlyOnUnresolvableProbe(t *testing.T) {
	ctx := &subCompileCtx{tableDef: &plan.TableDef{Name: "t_copy"}}

	md, err := compileplugin.BuildIdxcronMetadata(ctx, compileplugin.IdxcronVarSpec{
		FrontendProbeVar: "definitely_not_a_real_sysvar",
		Capture:          []string{"cagra_threads_build"},
	})
	require.NoError(t, err)
	require.Nil(t, md, "an unresolvable probe defers to background (nil metadata)")
}
