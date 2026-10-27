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
	"crypto/sha256"
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

type legacyFloatSessionIsland struct {
	source, want *planpb.Expr
	memo         *legacyFloatIslandMemo
	wantValue    [sha256.Size]byte
}
type legacyFloatSessionFoldKey struct{}

// Verification-local evaluation sharing excludes the expected literal. A later
// DEFAULT may have the same residual SQL but a different creation session. Only
// immutable, fully folded result bytes are shared; each consumer still compares
// its own complete island and performs its own authoritative binding proof.
type legacyFloatProbeMemo struct {
	bytes, entries int
	sources        map[legacyFloatProbeSource]*legacyFloatIslandMemo
}

type legacyFloatProbeSource struct {
	source string
	mode   tableDumpBindingMode
}

type legacyFloatProbeSession struct {
	zone   *time.Location
	locale string
}

type legacyFloatIslandMemo struct {
	owner   *tableDumpBindingBudget
	proven  bool // A full origin/admission comparison accepted this source; only an ordering hint.
	results map[legacyFloatProbeSession]legacyFloatProbeResult
}

type legacyFloatProbeResult struct {
	data  []byte
	value [sha256.Size]byte
}

const (
	legacyFloatProbeMemoBytes   = 32 << 20
	legacyFloatProbeMemoEntries = 65_536
	// Account conservatively for map buckets, keys, slice headers and owners in
	// addition to all retained serialized source/result bytes.
	legacyFloatProbeMemoOverhead = 256
)

func (m *legacyFloatProbeMemo) admits(size int) bool {
	return m.entries < legacyFloatProbeMemoEntries && size <= legacyFloatProbeMemoBytes-m.bytes-legacyFloatProbeMemoOverhead
}

func (b *tableDumpBindingBudget) prepareSessionIsland(island *legacyFloatSessionIsland, mode tableDumpBindingMode) error {
	if island.want.GetLit() == nil {
		return nil
	}
	m := &b.probeMemo
	if err := chargeFloatStringRebind(island.want, b); err != nil {
		return err
	}
	var err error
	island.wantValue, err = legacyFloatLiteralFingerprint(island.want)
	if err != nil {
		return err
	}
	if err := chargeFloatStringRebind(island.source, b); err != nil {
		return err
	}
	data, err := proto.Marshal(island.source)
	if err != nil {
		return err
	}
	key := legacyFloatProbeSource{string(data), mode}
	if previous := m.sources[key]; previous != nil {
		island.memo = previous
		return nil
	}
	if !m.admits(len(data)) {
		return nil // Optional sharing is full; ordinary bounded proof remains.
	}
	if m.sources == nil {
		m.sources = make(map[legacyFloatProbeSource]*legacyFloatIslandMemo)
	}
	island.memo = &legacyFloatIslandMemo{owner: b}
	m.sources[key] = island.memo
	m.entries++
	m.bytes += len(data) + legacyFloatProbeMemoOverhead
	return nil
}

// Literal payload inequality can reject a session without decoding and copying
// its whole result tree. Types and Src still go through the existing comparator
// on every positive hint. Canonicalize signed floating zero because proto.Equal
// compares float values with ==, whereas their protobuf wire bits differ.
func legacyFloatLiteralFingerprint(expr *planpb.Expr) ([sha256.Size]byte, error) {
	literal := *expr.GetLit()
	literal.Src = nil
	switch value := literal.Value.(type) {
	case *planpb.Literal_Dval:
		if value.Dval == 0 {
			literal.Value = &planpb.Literal_Dval{Dval: 0}
		}
	case *planpb.Literal_Fval:
		if value.Fval == 0 {
			literal.Value = &planpb.Literal_Fval{Fval: 0}
		}
	}
	data, err := proto.Marshal(&literal)
	return sha256.Sum256(data), err
}

// Hints live only for one table verification, alongside its unchanged total
// work budget. Retain no expression or binding era: each consumer must bind and
// verify its own complete candidate. Bounded retention only accelerates search;
// evicted or unsuitable sessions remain reachable through the normal fallback.
type tableDumpSessionHint struct {
	zone   *time.Location
	locale string
}

const tableDumpMaxSessionHints = 8

func (b *tableDumpBindingBudget) rememberSession(zone *time.Location, locale string) {
	hint := tableDumpSessionHint{zone, locale}
	for _, previous := range b.sessionHints {
		if previous == hint {
			return
		}
	}
	if len(b.sessionHints) == tableDumpMaxSessionHints {
		copy(b.sessionHints, b.sessionHints[1:])
		b.sessionHints[len(b.sessionHints)-1] = hint
	} else {
		b.sessionHints = append(b.sessionHints, hint)
	}
}

func (b *tableDumpBindingBudget) matchSessionHints(zone *time.Location, locale string,
	bind func(*time.Location, string) (*planpb.Expr, error), match func(*planpb.Expr) (bool, error)) (bool, error) {
	for _, hint := range b.sessionHints {
		if hint.zone == zone && hint.locale == locale {
			continue // This candidate was already bound in the reading session.
		}
		bound, err := bind(hint.zone, hint.locale)
		if err != nil {
			return false, err
		}
		if matched, err := match(bound); matched || err != nil {
			return matched, err
		}
	}
	return false, nil
}

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
	for i := range probe.islands {
		if err := budget.prepareSessionIsland(&probe.islands[i], probe.mode); err != nil {
			return nil, err
		}
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
	session := legacyFloatProbeSession{proc.GetSessionInfo().TimeZone, function.TemporalLocaleNameForProcess(proc)}
	for _, island := range p.islands {
		if err := ctx.GetContext().Err(); err != nil {
			return false, err
		}
		var folded *planpb.Expr
		memo := island.memo
		if memo != nil && memo.owner == budget {
			// Charge a lookup, then the actual decoded comparison input on hits.
			if err := chargeFloatStringRebind(&planpb.Expr{}, budget); err != nil {
				return false, err
			}
			if result, exists := memo.results[session]; exists {
				if result.value != island.wantValue {
					return false, nil
				}
				folded = &planpb.Expr{}
				if err := proto.Unmarshal(result.data, folded); err != nil {
					return false, err
				}
				if err := chargeFloatStringRebind(folded, budget); err != nil {
					return false, err
				}
			}
		}
		if folded == nil {
			if err := chargeFloatStringRebind(island.source, budget); err != nil {
				return false, err
			}
			var err error
			folded, err = ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(island.source), proc, false, true)
			if err != nil {
				return false, ctx.GetContext().Err()
			}
			m := &budget.probeMemo
			if memo != nil && memo.owner == budget && folded.GetLit() != nil {
				if err := chargeFloatStringRebind(folded, budget); err != nil {
					return false, err
				}
				value, err := legacyFloatLiteralFingerprint(folded)
				if err != nil {
					return false, err
				}
				if m.admits(proto.Size(folded)) {
					data, err := proto.Marshal(folded)
					if err != nil {
						return false, err
					}
					if memo.results == nil {
						memo.results = make(map[legacyFloatProbeSession]legacyFloatProbeResult)
					}
					memo.results[session] = legacyFloatProbeResult{data, value}
					m.entries++
					m.bytes += len(data) + legacyFloatProbeMemoOverhead
				}
				if value != island.wantValue {
					return false, nil
				}
			}
		}
		if err := chargeFloatStringRebind(island.want, budget); err != nil {
			return false, err
		}
		if !sameLegacyFloatOrigin(island.want, folded) {
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
	// Keep named-rule identities stable across expression searches in this table.
	if budget.probeTimeZones == nil {
		budget.probeTimeZones = timeZones
	}
	timeZones = budget.probeTimeZones
	locales := []string{locale}
	for _, other := range function.TemporalLocaleNames() {
		if other != locale {
			locales = append(locales, other)
		}
	}
	searchProbes := func(candidates []*legacyFloatSessionProbe, limit int) (bool, error) {
		for tz := range timeZones {
			for _, p := range candidates {
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
							for _, island := range p.islands {
								if island.memo != nil && island.memo.owner == budget {
									island.memo.proven = true
								}
							}
							return true, nil
						}
					}
				}
			}
		}
		return false, nil
	}
	// Reuse a previously proved residual's precision/formatting as an ordering
	// hint, including when a later DEFAULT needs a different timezone. Each
	// candidate still belongs to THIS expression. Limit unsuccessful preference
	// work, then retain the fair search and its original fallback headroom.
	var preferred []*legacyFloatSessionProbe
	for _, probe := range probes {
		proven := len(probe.islands) > 0
		for _, island := range probe.islands {
			proven = proven && island.memo != nil && island.memo.owner == budget && island.memo.proven
		}
		if proven {
			preferred = append(preferred, probe)
		}
	}
	if len(preferred) > 0 {
		limit := budget.visits + (candidateVisitLimit-budget.visits)/4
		if found, err := searchProbes(preferred, limit); found || err != nil {
			return found, err
		}
	}
	limit := budget.visits + (candidateVisitLimit-budget.visits)*3/4
	if found, err := searchProbes(probes, limit); found || err != nil {
		return found, err
	}

	ctx.GetProcess().GetSessionInfo().TimeZone, ctx.GetProcess().GetSessionInfo().LCTimeNames = zone, locale
	return false, nil
}
