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

package dependent

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// witnessSource is this file's minimal dependent.Source: one materialized
// batch per open, closed exactly once by the runtime that owns it.
type witnessSource struct {
	batch  *batch.Batch
	read   bool
	closed func()
}

func (s *witnessSource) Read(context.Context) (*batch.Batch, error) {
	if s.read {
		return nil, nil
	}
	s.read = true
	return s.batch, nil
}

func (s *witnessSource) Close() error { s.closed(); return nil }

// sourceStepWitnessQuery builds, by hand, the minimal valid dependent-query
// shapes root's GLM93 finding 1 asks for. variants:
//   - "sinkscan": the program region contains Node_SINK_SCAN whose SourceStep
//     arm names a step region (a PROJECT-rooted pipeline over a TABLE_SCAN).
//   - "sinkscan-prod": the same reader, but the pulled step region ends at a
//     Node_SINK carrying materialized.CTESinkOption - the production
//     shared-CTE materialization boundary the compile-side fixtures use.
//   - "direct": no reader at all - the program reads the same TABLE_SCAN
//     descendant directly (the known-good production per-row shape).
//
// The main region always carries the capture-free SCALAR call stamped as a
// guarded MERGE payload over the out-step node bearing the DMLMerge
// ExtraOptions, mirroring the writer identity the membership walk checks.
// This deliberately does not hunt for a new SQL binder shape: the node/stage
// vocabulary follows the existing reader fixtures (the compile package's
// shared-CTE SINK/SINK_SCAN plans and this package's streaming driver).
func sourceStepWitnessQuery(variant string) (*plan.Query, *plan.DependentExpr) {
	int64Type := plan.Type{Id: int32(types.T_int64)}
	decType := plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	col := func(t plan.Type) *plan.Expr {
		return &plan.Expr{Typ: t, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
	}
	call := &plan.DependentExpr{
		ProgramId:           1,
		OccurrenceId:        1,
		StageId:             1,
		Reducer:             plan.SubqueryRef_SCALAR,
		GuardedMergePayload: true,
	}
	callExpr := &plan.Expr{Typ: decType, Expr: &plan.Expr_Dependent{Dependent: call}}

	nodes := []*plan.Node{
		{NodeId: 0, NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{
			Name: "outer_source",
			Cols: []*plan.ColDef{{Name: "k", Typ: int64Type}},
		}, ProjectList: []*plan.Expr{col(int64Type)}},
		{NodeId: 1, NodeType: plan.Node_PROJECT, Children: []int32{0},
			ExtraOptions: icebergapi.DMLMergePlanExtraOptions,
			ProjectList:  []*plan.Expr{col(int64Type), callExpr}},
		{NodeId: 2, NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{
			Name: "owned_step",
			Cols: []*plan.ColDef{{Name: "v", Typ: decType}},
		}, ProjectList: []*plan.Expr{col(decType)}},
		{NodeId: 3, NodeType: plan.Node_PROJECT, Children: []int32{2},
			ProjectList: []*plan.Expr{col(decType)}},
		{NodeId: 4, NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{1},
			ProjectList: []*plan.Expr{col(decType)}},
	}
	steps := []int32{1}
	stepOwners := []uint32{0}
	programRoot := int32(3)
	switch variant {
	case "sinkscan", "sinkscan-prod":
		// The program root IS the materialized reader: its frame is exactly
		// the pulled step region's output, no extra projection frame hop.
		if variant == "sinkscan-prod" {
			nodes = append(nodes, &plan.Node{NodeId: 5, NodeType: plan.Node_SINK,
				Children: []int32{3}, ProjectList: []*plan.Expr{col(decType)},
				ExtraOptions: materialized.CTESinkOption})
			steps = []int32{1, 5}
		} else {
			steps = []int32{1, 3}
		}
		stepOwners = []uint32{0, 1}
		programRoot = 4
	case "direct":
	}
	q := &plan.Query{
		StmtType: plan.Query_MERGE,
		Nodes:    nodes,
		Steps:    steps,
		Correlation: &plan.CorrelationSpec{
			Version:        plan.CorrelationVersion,
			MainOutputStep: 0,
			StepOwners:     stepOwners,
			Programs:       []*plan.CorrelationProgram{{RootNode: programRoot, OutputTypes: []plan.Type{decType}}},
			Stages:         []*plan.CorrelationStage{{OwnerNode: 1, Kind: plan.CorrelationStage_INPUT}},
		},
	}
	return q, call
}

// TestSourceStepReaderDescendantWitness proves the executed materialized
// SourceStep descendant and the conservative sharing fallback with the real
// compiled runtime (root glm93 finding 1): the step region's TABLE_SCAN
// descendant is opened on EVERY winning row - two distinct observed
// evaluations for two rows, never one shared memo - and the occurrence is
// excluded from the memoized share class by the existing allowedNode audit.
func TestSourceStepReaderDescendantWitness(t *testing.T) {
	for _, variant := range []string{"direct", "sinkscan-prod", "sinkscan"} {
		t.Run(variant, func(t *testing.T) {
			q, call := sourceStepWitnessQuery(variant)

			require.True(t, captureFreeScalarCall(call),
				"the witness call is exactly the capture-free SCALAR candidate shape")
			require.Equal(t, map[uint32]bool{1: true}, icebergMergeGuardedPayloadOccurrences(q),
				"the stamped call is a guarded MERGE payload member")
			if variant == "direct" {
				require.True(t, auditShareEligibleProgram(q, 1),
					"the reader-free program sits inside the audited share class (control)")
			} else {
				require.False(t, auditShareEligibleProgram(q, 1),
					"the SINK_SCAN reader is outside allowedNode's audited classes: conservative rejection keeps per-row evaluation")
			}
			require.NoError(t, q.ValidateCorrelation(),
				"the hand-built plan passes the correlation validator")

			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)

			decGoType := types.T_decimal64.ToType()
			intGoType := types.T_int64.ToType()
			opened, closed := make(map[int32]int), make(map[int32]int)
			template, err := Compile(proc.Ctx, q, func(node *plan.Node) (SourceFactory, error) {
				if node.NodeType != plan.Node_TABLE_SCAN {
					return nil, nil
				}
				return func(child *process.Process, sourceAllocation *mpool.MPool, account *mpool.AllocationAccount) (Source, error) {
					opened[node.NodeId]++
					b := batch.NewWithSize(len(node.TableDef.Cols))
					rows := 2
					for i, c := range node.TableDef.Cols {
						switch types.T(c.Typ.Id) {
						case types.T_int64:
							b.Vecs[i] = vector.NewVec(intGoType, sourceAllocation)
							if err := vector.AppendFixed(b.Vecs[i], int64(1), false, sourceAllocation); err != nil {
								b.Clean(sourceAllocation)
								return nil, err
							}
							if err := vector.AppendFixed(b.Vecs[i], int64(2), false, sourceAllocation); err != nil {
								b.Clean(sourceAllocation)
								return nil, err
							}
						case types.T_decimal64:
							b.Vecs[i] = vector.NewVec(decGoType, sourceAllocation)
							rows = 1
							if err := vector.AppendFixed(b.Vecs[i], types.Decimal64(uint64(1000*opened[node.NodeId])), false, sourceAllocation); err != nil {
								b.Clean(sourceAllocation)
								return nil, err
							}
						default:
							b.Clean(sourceAllocation)
							return nil, moerr.NewInternalErrorNoCtx("unexpected witness source type")
						}
					}
					b.SetRowCount(rows)
					return &witnessSource{batch: b, closed: func() { closed[node.NodeId]++; b.Clean(sourceAllocation) }}, nil
				}, nil
			})
			require.NoError(t, err)
			require.Empty(t, opened, "sources open lazily at execution, never at compile")

			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<26, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()

			op := &Operator{Template: template}
			defer func() {
				op.Free(proc, false, nil)
				require.NoError(t, op.ClearAllocationAccount(account))
				op.Release()
			}()
			require.NoError(t, op.SetAllocationAccount(account))
			require.NoError(t, op.Prepare(proc))

			var ks []int64
			var vs []types.Decimal64
			for {
				result, err := vm.Exec(op, proc)
				if err != nil {
					t.Fatalf("vm.Exec failed: %v; opened=%v closed=%v", err, opened, closed)
				}
				b := result.Batch
				if b == nil {
					break
				}
				require.Equal(t, 2, len(b.Vecs))
				for row := 0; row < b.RowCount(); row++ {
					ks = append(ks, vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], row))
					vs = append(vs, vector.GetFixedAtNoTypeCheck[types.Decimal64](b.Vecs[1], row))
				}
			}
			require.Equal(t, []int64{1, 2}, ks)
			require.Len(t, vs, 2)
			if variant == "direct" {
				// The audited-eligible CONTROL: the template's scalarShareEligible
				// memoizes the first winning row's finalized result, so the SAME
				// Execution's two rows observe ONE materialized evaluation.
				require.Equal(t, 1, opened[2],
					"the share-eligible control opens the owned step ONCE for both rows (the memo)")
				require.Equal(t, []types.Decimal64{types.Decimal64(1000), types.Decimal64(1000)}, vs,
					"both rows observe the memoized first evaluation")
				require.Equal(t, 1, closed[2])
			} else {
				require.NotEqual(t, vs[0], vs[1],
					"the two rows observed two DISTINCT owned-step evaluations (per-row fallback), not one shared memo")
				require.Equal(t, []types.Decimal64{types.Decimal64(1000), types.Decimal64(2000)}, vs,
					"the descendant's per-open effect is exactly the two sequential materialized evaluations")
				require.Equal(t, 2, opened[2],
					"the reader pulled the owned step region on EVERY winning row: two evaluations")
				require.Equal(t, 2, closed[2], "each pulled source was closed exactly once")
			}
			require.Equal(t, 1, opened[0], "the outer row source is a single open")
		})
	}
}
