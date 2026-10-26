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
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A probe locates a session; only the original full SQL binding can authorize
// repair. Keep every era/scope candidate when equivalent probe work is shared.
type legacyFloatSessionProbe struct {
	islands  []legacyFloatSessionIsland
	mode     tableDumpBindingMode
	searches []tableDumpSessionCandidate
}

type legacyFloatSessionIsland struct{ source, want *planpb.Expr }
type legacyFloatSessionFoldKey struct{}

func prepareLegacyFloatSessionProbe(ctx CompilerContext, stored, folded, source *planpb.Expr, search tableDumpSessionCandidate, budget *tableDumpBindingBudget) (*legacyFloatSessionProbe, error) {
	if source == nil || folded == nil || search.mode&tableDumpBindingPreJSON != 0 {
		return nil, nil
	}
	if err := chargeFloatStringRebind(source, budget); err != nil {
		return nil, err
	}
	// Binding a timestamp literal may already consume the reading timezone.
	// Replaying that value cannot recover its SQL binding; retain full binding.
	bindPhaseValue := false
	_ = planpb.VisitExprTree(source, func(e *planpb.Expr) error {
		bindPhaseValue = bindPhaseValue || e.GetLit() != nil && e.Typ.Id == int32(types.T_timestamp)
		return nil
	})
	if bindPhaseValue {
		return nil, nil
	}
	probe := &legacyFloatSessionProbe{mode: search.mode & (tableDumpBindingLegacyFloatFormatting | tableDumpBindingPreJSONFolding), searches: []tableDumpSessionCandidate{search}}
	formatting := false
	var collect func(*planpb.Expr, *planpb.Expr, *planpb.Expr) error
	collect = func(want, bound, original *planpb.Expr) error {
		if want == nil || bound == nil || original == nil {
			return nil
		}
		if err := ctx.GetContext().Err(); err != nil {
			return err
		}
		if bound.GetLit() != nil && want.GetLit() != nil && original.GetF() != nil {
			if err := chargeFloatStringRebind(original, budget); err != nil {
				return err
			}
			zone, locale := tableDumpDefaultDependencies(original)
			if !zone && !locale {
				return nil
			}
			residual := DeepCopyExpr(original)
			preserved := make(map[*planpb.Expr]bool)
			var mark func(*planpb.Expr) error
			mark = func(e *planpb.Expr) error {
				if e == nil {
					return nil
				}
				if err := chargeFloatStringRebind(e, budget); err != nil {
					return err
				}
				z, l := tableDumpDefaultDependencies(e)
				preserved[e] = z || l
				if fn := e.GetF(); fn != nil {
					for _, arg := range fn.Args {
						if err := mark(arg); err != nil {
							return err
						}
					}
				}
				if list := e.GetList(); list != nil {
					for _, entry := range list.List {
						if err := mark(entry); err != nil {
							return err
						}
					}
				}
				return nil
			}
			if err := mark(residual); err != nil {
				return err
			}
			formatting = formatting || hasFloatDefaultFormatting(original)
			proc := legacyFloatSessionProbeProcess(ctx, probe.mode)
			proc.Ctx = context.WithValue(proc.Ctx, legacyFloatSessionFoldKey{}, preserved)
			residual, err := ConstantFold(batch.EmptyForConstFoldBatch, residual, proc, false, true)
			if err != nil {
				return nil // An unavailable projection is never a negative proof.
			}
			probe.islands = append(probe.islands, legacyFloatSessionIsland{source: residual, want: want})
			return nil
		}
		if a, b, c := want.GetF(), bound.GetF(), original.GetF(); a != nil && b != nil && c != nil &&
			a.Func != nil && b.Func != nil && c.Func != nil && a.Func.Obj == b.Func.Obj && b.Func.Obj == c.Func.Obj &&
			len(a.Args) == len(b.Args) && len(b.Args) == len(c.Args) {
			for i := range a.Args {
				if err := collect(a.Args[i], b.Args[i], c.Args[i]); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := collect(stored, folded, source); err != nil {
		return nil, err
	}
	if len(probe.islands) == 0 {
		return nil, nil
	}
	if !formatting {
		probe.mode &^= tableDumpBindingLegacyFloatFormatting
	}
	return probe, nil
}

func addLegacyFloatSessionProbe(ctx CompilerContext, stored, folded, source *planpb.Expr, search tableDumpSessionCandidate, probes *[]*legacyFloatSessionProbe, keys map[string]int, budget *tableDumpBindingBudget) error {
	probe, err := prepareLegacyFloatSessionProbe(ctx, stored, folded, source, search, budget)
	if err != nil || probe == nil {
		return err
	}
	key, err := probe.key(budget)
	if err != nil {
		return err
	}
	if index, exists := keys[key]; exists {
		group := (*probes)[index]
		group.searches = append(group.searches, search)
		group.searches[0].dependency.locale = group.searches[0].dependency.locale || search.dependency.locale
	} else {
		keys[key] = len(*probes)
		*probes = append(*probes, probe)
	}
	return nil
}

func legacyFloatSessionProbeProcess(ctx CompilerContext, mode tableDumpBindingMode) *process.Process {
	proofCtx := ctx.GetContext()
	if mode&tableDumpBindingLegacyFloatFormatting != 0 {
		proofCtx = function.WithLegacyFloatStringFormatting(proofCtx)
	}
	if mode&tableDumpBindingPreJSONFolding != 0 {
		proofCtx = context.WithValue(proofCtx, historicalFloatDefaultFoldingKey{}, true)
	}
	return floatStringProofProcess(proofCtx, ctx.GetProcess())
}

// Exact serialized probe identity includes types, provenance and literal sources.
// It shares evaluation only; the original candidates remain authoritative.
func (p *legacyFloatSessionProbe) key(budget *tableDumpBindingBudget) (string, error) {
	list := &planpb.Expr{Typ: planpb.Type{Id: int32(p.mode)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{}}}
	for _, island := range p.islands {
		list.GetList().List = append(list.GetList().List, island.source, island.want)
	}
	if err := chargeFloatStringRebind(list, budget); err != nil {
		return "", err
	}
	data, err := proto.Marshal(list)
	return string(data), err
}

func (p *legacyFloatSessionProbe) matches(ctx CompilerContext, budget *tableDumpBindingBudget) (bool, error) {
	proc := legacyFloatSessionProbeProcess(ctx, p.mode)
	for _, island := range p.islands {
		if err := ctx.GetContext().Err(); err != nil {
			return false, err
		}
		// Precharge every copied/evaluated subtree and complete comparison input.
		for _, expr := range []*planpb.Expr{island.source, island.want} {
			if err := chargeFloatStringRebind(expr, budget); err != nil {
				return false, err
			}
		}
		folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(island.source), proc, false, true)
		if err != nil || !sameLegacyFloatOrigin(island.want, folded) {
			return false, ctx.GetContext().Err()
		}
	}
	return true, nil
}

func matchLegacyFloatSessionProbes(ctx CompilerContext, zone *time.Location, locale string, timeZones func(func(*time.Location) bool), probes []*legacyFloatSessionProbe, budget *tableDumpBindingBudget, candidateVisitLimit int,
	bind func(tableDumpSessionCandidate, *time.Location, string) (*planpb.Expr, error), match func(tableDumpSessionCandidate, *planpb.Expr) (bool, error)) (bool, error) {
	if len(probes) == 0 {
		return false, nil
	}
	// Preserve headroom for authoritative binding and the original fallback.
	limit := budget.visits + (candidateVisitLimit-budget.visits)*3/4
	locales := []string{locale}
	for _, other := range function.TemporalLocaleNames() {
		if other != locale {
			locales = append(locales, other)
		}
	}
	for tz := range timeZones {
		for _, p := range probes {
			for _, lc := range locales {
				if lc != locale && !p.searches[0].dependency.locale {
					continue
				}
				if budget.visits >= limit {
					return false, nil
				}
				ctx.GetProcess().GetSessionInfo().TimeZone, ctx.GetProcess().GetSessionInfo().LCTimeNames = tz, lc
				possible, err := p.matches(ctx, budget)
				if err != nil {
					return false, err
				}
				if budget.visits > candidateVisitLimit {
					return false, moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
				}
				if !possible {
					continue
				}
				for _, search := range p.searches {
					bound, err := bind(search, tz, lc)
					if err != nil {
						return false, err
					}
					matched, err := match(search, bound)
					if err != nil {
						return false, err
					}
					if budget.visits > candidateVisitLimit {
						return false, moerr.NewInvalidInputNoCtx("table dump expression binding exceeds limit")
					}
					if matched {
						return true, nil
					}
				}
			}
		}
	}
	ctx.GetProcess().GetSessionInfo().TimeZone, ctx.GetProcess().GetSessionInfo().LCTimeNames = zone, locale
	return false, nil
}
