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

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type ProvenanceState uint8

const (
	ProvenanceUnknown ProvenanceState = iota
	ProvenanceNone
	ProvenanceSingleSource
	// ProvenancePureNull marks a bare, untyped NULL and transparent projections
	// of it. Unlike a source column, it contributes no type or collation contract
	// when a set operation resolves a common output type.
	ProvenancePureNull
)

// CTASDefaultPolicy is deliberately separate from source-column identity.
// View schema generation consumes the source snapshot directly, while CTAS
// uses this policy to decide whether the target may copy that source default.
type CTASDefaultPolicy uint8

const (
	CTASDefaultNone CTASDefaultPolicy = iota
	CTASDefaultInheritSource
	CTASDefaultInheritViewSource
	CTASDefaultUseTypeDefault
)

// SourceColumnMetadata is an immutable planner-local snapshot. It deliberately
// contains only metadata consumed by output-schema builders, so transparent
// query boundaries can share it without retaining or repeatedly copying a
// complete catalog ColDef.
type SourceColumnMetadata struct {
	Typ         plan.Type
	Default     *plan.Default
	NullAbility bool
}

type SourceColumn struct {
	RelPos   int32
	ColPos   int32
	TableID  uint64
	Metadata SourceColumnMetadata
}

type OutputColumnProvenance struct {
	State             ProvenanceState
	Source            *SourceColumn
	CTASDefaultPolicy CTASDefaultPolicy
}

func snapshotSourceColumnMetadata(col *plan.ColDef) SourceColumnMetadata {
	typ := col.Typ
	metadata := SourceColumnMetadata{
		NullAbility: col.Default == nil || col.Default.NullAbility,
		Typ: plan.Type{
			Id:          typ.Id,
			NotNullable: typ.NotNullable,
			AutoIncr:    typ.AutoIncr,
			Width:       typ.Width,
			Scale:       typ.Scale,
			Table:       typ.Table,
			Enumvalues:  typ.Enumvalues,
		},
	}
	if col.Default == nil {
		metadata.NullAbility = !typ.NotNullable
	}
	if col.Default != nil && (col.Default.Expr != nil || col.Default.OriginString != "") {
		metadata.Default = DeepCopyDefault(col.Default)
	}
	return metadata
}

func hasExplicitSourceDefault(metadata SourceColumnMetadata) bool {
	return metadata.Default != nil
}

// isGeneratedExpressionDefault follows buildDefaultExpr's persisted contract:
// a DEFAULT whose source AST is a tree.ParenExpr keeps the outer parentheses
// in OriginString. Expr alone is insufficient because constant folding may turn
// a generated expression into a literal while its CTAS semantics stay distinct
// from an ordinary literal default.
func isGeneratedExpressionDefault(def *plan.Default) bool {
	if def == nil {
		return false
	}
	origin := strings.TrimSpace(def.OriginString)
	return len(origin) >= 2 && origin[0] == '(' && origin[len(origin)-1] == ')'
}

func ctasViewDefaultPolicy(metadata SourceColumnMetadata) CTASDefaultPolicy {
	if !hasExplicitSourceDefault(metadata) {
		return CTASDefaultNone
	}
	if isGeneratedExpressionDefault(metadata.Default) {
		return CTASDefaultInheritViewSource
	}
	if !metadata.Typ.NotNullable {
		return CTASDefaultNone
	}
	if _, ok := ctasViewTypeDefaultOrigin(metadata.Typ); ok {
		return CTASDefaultUseTypeDefault
	}
	return CTASDefaultNone
}

func (bc *BindContext) markViewCTASDefaultBoundary(viewCols []*plan.ColDef) {
	for i := 0; i < min(len(bc.headings), len(bc.projects)); i++ {
		provenance := bc.outputColumnProvenanceForProject(int32(i))
		if provenance.State == ProvenanceSingleSource && provenance.Source != nil {
			// The authoritative View schema has already applied outer-join
			// null-extension. Preserve source identity/default policy, but take
			// nullability from that boundary instead of the pre-join base column.
			if i < len(viewCols) {
				source := *provenance.Source
				source.Metadata.NullAbility = snapshotSourceColumnMetadata(viewCols[i]).NullAbility
				provenance.Source = &source
			}
			provenance.CTASDefaultPolicy = ctasViewDefaultPolicy(provenance.Source.Metadata)
		}
		bc.outputColumnProvenance[int32(i)] = provenance
	}
}

// transparentOutputSourceExpr unwraps planner display adapters that preserve
// the identity of one source column. All other functions are semantic
// expressions and therefore clear lineage.
func transparentOutputSourceExpr(expr *plan.Expr) (*plan.Expr, bool) {
	if expr == nil {
		return nil, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 || fn.Args[1] == nil || fn.Args[1].GetCol() == nil {
		return nil, false
	}

	sourceExpr := fn.Args[1]
	switch fn.Func.ObjName {
	case moEnumCastIndexToValueFun:
		return sourceExpr, isEnumPlanType(&sourceExpr.Typ)
	case moSetCastIndexToValueFun:
		return sourceExpr, isSetPlanType(&sourceExpr.Typ)
	default:
		return nil, false
	}
}

func (bc *BindContext) outputColumnProvenanceForExpr(expr *plan.Expr) OutputColumnProvenance {
	if expr == nil {
		return OutputColumnProvenance{State: ProvenanceNone}
	}
	if isPureNullLiteralExpr(expr) {
		return OutputColumnProvenance{State: ProvenancePureNull}
	}
	if sourceExpr, ok := transparentOutputSourceExpr(expr); ok {
		return bc.outputColumnProvenanceForExpr(sourceExpr)
	}

	col := expr.GetCol()
	if col == nil {
		return OutputColumnProvenance{State: ProvenanceNone}
	}
	if col.RelPos == bc.projectTag && col.ColPos >= 0 && int(col.ColPos) < len(bc.projects) {
		if provenance, recorded := bc.outputColumnProvenance[col.ColPos]; recorded {
			return provenance
		}
		project := bc.projects[col.ColPos]
		if project == nil {
			return OutputColumnProvenance{State: ProvenanceNone}
		}
		if projectCol := project.GetCol(); projectCol != nil &&
			projectCol.RelPos == col.RelPos && projectCol.ColPos == col.ColPos {
			return OutputColumnProvenance{State: ProvenanceNone}
		}
		return bc.outputColumnProvenanceForExpr(project)
	}
	if bc.groupTag > 0 && col.RelPos == bc.groupTag && col.ColPos >= 0 && int(col.ColPos) < len(bc.groups) {
		groupExpr := bc.groups[col.ColPos]
		if groupExpr == nil {
			return OutputColumnProvenance{State: ProvenanceNone}
		}
		if groupCol := groupExpr.GetCol(); groupCol != nil &&
			groupCol.RelPos == col.RelPos && groupCol.ColPos == col.ColPos {
			return OutputColumnProvenance{State: ProvenanceNone}
		}
		return bc.outputColumnProvenanceForExpr(groupExpr)
	}
	binding := bc.bindingByTag[col.RelPos]
	if binding == nil || col.ColPos < 0 || int(col.ColPos) >= len(binding.outputColumnProvenance) {
		return OutputColumnProvenance{State: ProvenanceNone}
	}
	return binding.outputColumnProvenance[col.ColPos]
}

func (bc *BindContext) outputColumnProvenanceForProject(colPos int32) OutputColumnProvenance {
	if provenance, recorded := bc.outputColumnProvenance[colPos]; recorded {
		return provenance
	}
	if colPos < 0 || int(colPos) >= len(bc.projects) {
		return OutputColumnProvenance{State: ProvenanceNone}
	}
	return bc.outputColumnProvenanceForExpr(bc.projects[colPos])
}

func (bc *BindContext) outputColumnProvenanceForBoundary() []OutputColumnProvenance {
	provenance := make([]OutputColumnProvenance, min(len(bc.headings), len(bc.projects)))
	for i := range provenance {
		provenance[i] = bc.outputColumnProvenanceForProject(int32(i))
	}
	return provenance
}

func (bc *BindContext) clearOutputColumnProvenance() {
	for i := 0; i < min(len(bc.headings), len(bc.projects)); i++ {
		bc.outputColumnProvenance[int32(i)] = OutputColumnProvenance{State: ProvenanceNone}
	}
}
