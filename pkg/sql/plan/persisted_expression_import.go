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
	"math"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var nextNumericValueProgramScope atomic.Uint64

// importPersistedExpr copies one catalog expression into the current plan,
// refreshes ordinary function identities, and assigns fresh identities to
// validated private numeric-value programs. Catalog bytes remain immutable.
func importPersistedExpr(ctx context.Context, catalogRoot *Expr) (*Expr, error) {
	if catalogRoot == nil {
		return nil, nil
	}
	roots, err := importPersistedExprRoots(ctx, []*Expr{catalogRoot})
	if err != nil {
		return nil, err
	}
	return roots[0], nil
}

// importPersistedExprRoots imports a complete expression group. Some
// executable owners keep a value REF0 and its sibling BOOL REF1 in separate
// roots, so validation and scope remapping must see the whole group at once.
func importPersistedExprRoots(ctx context.Context, catalogRoots []*Expr) ([]*Expr, error) {
	if len(catalogRoots) == 0 {
		return nil, nil
	}
	programs, err := validatePersistedNumericProgramRoots(ctx, catalogRoots)
	if err != nil {
		return nil, err
	}

	roots := DeepCopyExprList(catalogRoots)
	if err := walkPersistedExprRoots(ctx, roots, func(expr *Expr) error {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
		if fid == function.SCALAR_PROGRAM || fid == function.SCALAR_PROGRAM_REF {
			// The validated numeric-program IDs are stable private contracts. They
			// have no SQL registry name and must never be rebound by ObjName.
			if overload != 0 {
				return persistedNumericProgramError(ctx, "private function overload is not supported")
			}
			return nil
		}
		newFID, err := function.GetFunctionIdByName(ctx, fn.Func.GetObjName())
		if err != nil {
			return err
		}
		if fid != newFID {
			fn.Func.Obj = function.EncodeOverloadID(newFID, overload)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := freshenValidatedNumericProgramScopes(ctx, roots, programs); err != nil {
		return nil, err
	}
	return roots, nil
}

// cloneWithFreshNumericProgramScopes creates a new plan owner for one complete
// expression group. The shared mapping preserves every REF0/REF1 alias while
// preventing an independently rewritten copy from reusing query-wide scope IDs.
func cloneWithFreshNumericProgramScopes(ctx context.Context, roots []*Expr) ([]*Expr, error) {
	programs, err := validatePersistedNumericProgramRoots(ctx, roots)
	if err != nil {
		return nil, err
	}
	clones := DeepCopyExprList(roots)
	if err := freshenValidatedNumericProgramScopes(ctx, clones, programs); err != nil {
		return nil, err
	}
	return clones, nil
}

func freshenValidatedNumericProgramScopes(ctx context.Context, roots []*Expr, programs *persistedNumericProgramGroup) error {
	if programs == nil || len(programs.scopes) == 0 {
		return nil
	}
	reserved := make(map[int64]struct{}, len(programs.scopes))
	for scope := range programs.scopes {
		reserved[scope] = struct{}{}
	}
	remapped := make(map[int64]int64, len(programs.scopes))
	for oldScope := range programs.scopes {
		newScope, err := allocateNumericValueProgramScope(ctx, reserved)
		if err != nil {
			return err
		}
		reserved[newScope] = struct{}{}
		remapped[oldScope] = newScope
	}
	return remapValidatedNumericProgramScopes(ctx, roots, programs, remapped)
}

// canonicalizeValidatedNumericProgramScopes assigns first-encounter ordinals
// to one already-validated expression group. This is for semantic comparison
// of independently bound trees; unlike import, it does not allocate plan IDs.
func canonicalizeValidatedNumericProgramScopes(
	ctx context.Context, roots []*Expr, programs *persistedNumericProgramGroup,
) error {
	if programs == nil || len(programs.scopes) == 0 {
		return nil
	}
	remapped := make(map[int64]int64, len(programs.scopes))
	if err := walkPersistedExprRoots(ctx, roots, func(expr *Expr) error {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		fid, _ := function.DecodeOverloadID(fn.Func.GetObj())
		if fid != function.SCALAR_PROGRAM && fid != function.SCALAR_PROGRAM_REF {
			return nil
		}
		scope, ok := numericValueProgramScope(expr)
		if !ok {
			return persistedNumericProgramError(ctx, "program scope is not an int64 literal")
		}
		if _, exists := remapped[scope]; !exists {
			remapped[scope] = int64(len(remapped) + 1)
		}
		return nil
	}); err != nil {
		return err
	}
	if len(remapped) != len(programs.scopes) {
		return persistedNumericProgramError(ctx, "program scope is missing from retained expression graph")
	}
	return remapValidatedNumericProgramScopes(ctx, roots, programs, remapped)
}

func remapValidatedNumericProgramScopes(
	ctx context.Context, roots []*Expr, programs *persistedNumericProgramGroup, remapped map[int64]int64,
) error {
	if programs == nil {
		return nil
	}
	if err := walkPersistedExprRoots(ctx, roots, func(expr *Expr) error {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		fid, _ := function.DecodeOverloadID(fn.Func.GetObj())
		switch fid {
		case function.SCALAR_PROGRAM:
			scope, ok := numericPlanInt64(fn.Args[0])
			newScope, exists := remapped[scope]
			if !ok || !exists {
				return persistedNumericProgramError(ctx, "program scope is not an int64 literal")
			}
			setNumericPlanInt64(fn.Args[0], newScope)
		case function.SCALAR_PROGRAM_REF:
			argOffset := 0
			if len(fn.Args) == 3 {
				argOffset = 1
			}
			scope, ok := numericPlanInt64(fn.Args[argOffset])
			newScope, exists := remapped[scope]
			if !ok || !exists {
				return persistedNumericProgramError(ctx, "reference scope is not an int64 literal")
			}
			setNumericPlanInt64(fn.Args[argOffset], newScope)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

type persistedNumericProgramGroup struct {
	definitions map[int64]*Expr
	anchors     map[int64]bool
	references  map[int64]map[int64]bool
	scopes      map[int64]struct{}
}

// validatePersistedNumericProgramIdentity checks one retained private node's
// local wire shape and records its scope. Literal.Src is retained for identity
// refresh and scope rebasing, but it is not an executable edge and cannot
// satisfy a live reference's anchored definition.
func validatePersistedNumericProgramIdentity(ctx context.Context, expr *Expr, scopes map[int64]struct{}) error {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return nil
	}
	fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if fid != function.SCALAR_PROGRAM && fid != function.SCALAR_PROGRAM_REF {
		return nil
	}
	if overload != 0 {
		return persistedNumericProgramError(ctx, "private function overload is not supported")
	}
	switch fid {
	case function.SCALAR_PROGRAM:
		if len(fn.Args) != 2 && len(fn.Args) != 3 {
			return persistedNumericProgramError(ctx, "program has an invalid argument count")
		}
		scope, ok := numericPlanInt64(fn.Args[0])
		if !ok || scope <= 0 || fn.Args[1] == nil {
			return persistedNumericProgramError(ctx, "program has an invalid scope or value")
		}
		if len(fn.Args) == 3 && (fn.Args[2] == nil || types.T(fn.Args[2].Typ.Id) != types.T_bool) {
			return persistedNumericProgramError(ctx, "program kind output is not BOOL")
		}
		scopes[scope] = struct{}{}
	case function.SCALAR_PROGRAM_REF:
		if len(fn.Args) != 2 && len(fn.Args) != 3 {
			return persistedNumericProgramError(ctx, "reference has an invalid argument count")
		}
		argOffset := 0
		var definition *Expr
		if len(fn.Args) == 3 {
			argOffset = 1
			definition = fn.Args[0]
		}
		scope, scopeOK := numericPlanInt64(fn.Args[argOffset])
		slot, slotOK := numericPlanInt64(fn.Args[argOffset+1])
		if !scopeOK || scope <= 0 || !slotOK || (slot != 0 && slot != 1) {
			return persistedNumericProgramError(ctx, "reference has an invalid scope or slot")
		}
		if slot == 1 {
			if definition != nil || types.T(expr.Typ.Id) != types.T_bool {
				return persistedNumericProgramError(ctx, "kind reference has an invalid definition or type")
			}
		} else {
			if definition == nil {
				return persistedNumericProgramError(ctx, "slot-zero reference is missing its definition")
			}
			defFn := definition.GetF()
			if defFn == nil || defFn.Func == nil || len(defFn.Args) != 2 && len(defFn.Args) != 3 {
				return persistedNumericProgramError(ctx, "slot-zero reference has an invalid definition")
			}
			defID, defOverload := function.DecodeOverloadID(defFn.Func.GetObj())
			defScope, defScopeOK := numericPlanInt64(defFn.Args[0])
			if defID != function.SCALAR_PROGRAM || defOverload != 0 || !defScopeOK || defScope != scope ||
				defFn.Args[1] == nil || !proto.Equal(&expr.Typ, &defFn.Args[1].Typ) ||
				(len(defFn.Args) == 3 && (defFn.Args[2] == nil || types.T(defFn.Args[2].Typ.Id) != types.T_bool)) {
				return persistedNumericProgramError(ctx, "slot-zero definition does not match its reference type or scope")
			}
		}
		scopes[scope] = struct{}{}
	}
	return nil
}

func validatePersistedNumericProgramRoots(ctx context.Context, roots []*Expr) (*persistedNumericProgramGroup, error) {
	// Check the pointer graph before semantic validation. A catalog protobuf is
	// expected to be a tree/DAG, but reject an in-memory cycle before DeepCopy or
	// proto.Equal can recurse through malformed input.
	if err := walkPersistedExprRoots(ctx, roots, func(*Expr) error { return nil }); err != nil {
		return nil, err
	}
	group := &persistedNumericProgramGroup{
		definitions: make(map[int64]*Expr),
		anchors:     make(map[int64]bool),
		references:  make(map[int64]map[int64]bool),
		scopes:      make(map[int64]struct{}),
	}
	if err := walkPersistedExprRoots(ctx, roots, func(expr *Expr) error {
		return validatePersistedNumericProgramIdentity(ctx, expr, group.scopes)
	}); err != nil {
		return nil, err
	}
	if err := walkPersistedExecutableExprRoots(ctx, roots, func(expr *Expr) error {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		fid, overload := function.DecodeOverloadID(fn.Func.GetObj())
		if fid != function.SCALAR_PROGRAM && fid != function.SCALAR_PROGRAM_REF {
			return nil
		}
		if overload != 0 {
			return persistedNumericProgramError(ctx, "private function overload is not supported")
		}
		switch fid {
		case function.SCALAR_PROGRAM:
			if len(fn.Args) != 2 && len(fn.Args) != 3 {
				return persistedNumericProgramError(ctx, "program has an invalid argument count")
			}
			scope, ok := numericPlanInt64(fn.Args[0])
			if !ok || scope <= 0 || fn.Args[1] == nil {
				return persistedNumericProgramError(ctx, "program has an invalid scope or value")
			}
			if len(fn.Args) == 3 && (fn.Args[2] == nil || types.T(fn.Args[2].Typ.Id) != types.T_bool) {
				return persistedNumericProgramError(ctx, "program kind output is not BOOL")
			}
			if old, exists := group.definitions[scope]; exists && !proto.Equal(old, expr) {
				return persistedNumericProgramError(ctx, "scope has conflicting definitions")
			}
			group.definitions[scope] = expr
		case function.SCALAR_PROGRAM_REF:
			if len(fn.Args) != 2 && len(fn.Args) != 3 {
				return persistedNumericProgramError(ctx, "reference has an invalid argument count")
			}
			argOffset := 0
			if len(fn.Args) == 3 {
				argOffset = 1
			}
			scope, scopeOK := numericPlanInt64(fn.Args[argOffset])
			slot, slotOK := numericPlanInt64(fn.Args[argOffset+1])
			if !scopeOK || scope <= 0 || !slotOK || (slot != 0 && slot != 1) {
				return persistedNumericProgramError(ctx, "reference has an invalid scope or slot")
			}
			if len(fn.Args) == 3 {
				if slot != 0 || fn.Args[0] == nil || fn.Args[0].GetF() == nil || fn.Args[0].GetF().Func == nil {
					return persistedNumericProgramError(ctx, "only slot zero may carry its definition")
				}
				definition := fn.Args[0]
				definitionID, definitionOverload := function.DecodeOverloadID(definition.GetF().Func.GetObj())
				definitionScope, definitionScopeOK := numericProgramDefinitionScope(definition)
				if definitionID != function.SCALAR_PROGRAM || definitionOverload != 0 ||
					!definitionScopeOK || definitionScope != scope ||
					(len(definition.GetF().Args) != 2 && len(definition.GetF().Args) != 3) ||
					definition.GetF().Args[1] == nil ||
					!proto.Equal(&expr.Typ, &definition.GetF().Args[1].Typ) {
					return persistedNumericProgramError(ctx, "slot-zero definition does not match its reference type or scope")
				}
				group.anchors[scope] = true
			} else if slot == 0 {
				return persistedNumericProgramError(ctx, "slot-zero reference is missing its definition")
			}
			if slot == 1 && types.T(expr.Typ.Id) != types.T_bool {
				return persistedNumericProgramError(ctx, "slot-one reference is not BOOL")
			}
			if group.references[scope] == nil {
				group.references[scope] = make(map[int64]bool)
			}
			group.references[scope][slot] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for scope, slots := range group.references {
		definition := group.definitions[scope]
		if definition == nil || !group.anchors[scope] {
			return nil, persistedNumericProgramError(ctx, "reference has no anchored definition")
		}
		if !slots[0] {
			return nil, persistedNumericProgramError(ctx, "program has no value reference")
		}
	}
	for scope := range group.definitions {
		if !group.anchors[scope] {
			return nil, persistedNumericProgramError(ctx, "definition is not anchored by a value reference")
		}
	}
	if err := validatePersistedNumericProgramCycles(ctx, group); err != nil {
		return nil, err
	}
	return group, nil
}

func validatePersistedNumericProgramCycles(ctx context.Context, group *persistedNumericProgramGroup) error {
	edges, err := collectPersistedNumericProgramDependencies(ctx, group)
	if err != nil {
		return err
	}
	state := make(map[int64]uint8, len(group.definitions))
	var visit func(int64) error
	visit = func(scope int64) error {
		switch state[scope] {
		case 1:
			return persistedNumericProgramError(ctx, "program scope graph contains a cycle")
		case 2:
			return nil
		}
		state[scope] = 1
		for dependency := range edges[scope] {
			if _, exists := group.definitions[dependency]; !exists {
				return persistedNumericProgramError(ctx, "program references a missing definition")
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[scope] = 2
		return nil
	}
	for scope := range group.definitions {
		if err := visit(scope); err != nil {
			return err
		}
	}
	return nil
}

func collectPersistedNumericProgramDependencies(
	ctx context.Context, group *persistedNumericProgramGroup,
) (map[int64]map[int64]struct{}, error) {
	edges := make(map[int64]map[int64]struct{}, len(group.definitions))
	for scope, definition := range group.definitions {
		fn := definition.GetF()
		if fn == nil || len(fn.Args) < 2 {
			return nil, persistedNumericProgramError(ctx, "program definition is incomplete")
		}
		edges[scope] = make(map[int64]struct{})
		for _, source := range fn.Args[1:] {
			if err := walkPersistedExprRootsWithChildren(ctx, []*Expr{source}, func(expr *Expr) error {
				ref := expr.GetF()
				if ref == nil || ref.Func == nil {
					return nil
				}
				fid, _ := function.DecodeOverloadID(ref.Func.GetObj())
				if fid != function.SCALAR_PROGRAM_REF {
					return nil
				}
				refScope, _, ok := numericProgramRefIdentity(expr)
				if !ok {
					return persistedNumericProgramError(ctx, "reference has an invalid identity")
				}
				edges[scope][refScope] = struct{}{}
				return nil
			}, persistedCycleDependencyChildren); err != nil {
				return nil, err
			}
		}
	}
	return edges, nil
}

// A REF contributes one dependency edge to the enclosing definition. Its
// inline REF0 definition is validated and scanned independently, so walking
// into that body here would rediscover every descendant edge at each ancestor.
func persistedCycleDependencyChildren(expr *Expr) []*Expr {
	fn := expr.GetF()
	if fn != nil && fn.Func != nil {
		fid, _ := function.DecodeOverloadID(fn.Func.GetObj())
		if fid == function.SCALAR_PROGRAM_REF {
			return nil
		}
	}
	return persistedExecutableExprChildren(expr)
}

func walkPersistedExprRoots(ctx context.Context, roots []*Expr, visit func(*Expr) error) error {
	return walkPersistedExprRootsWithChildren(ctx, roots, visit, persistedExprChildren)
}

func walkPersistedExecutableExprRoots(ctx context.Context, roots []*Expr, visit func(*Expr) error) error {
	return walkPersistedExprRootsWithChildren(ctx, roots, visit, persistedExecutableExprChildren)
}

func walkPersistedExprRootsWithChildren(
	ctx context.Context,
	roots []*Expr,
	visit func(*Expr) error,
	childrenOf func(*Expr) []*Expr,
) error {
	state := make(map[*Expr]uint8)
	var walk func(*Expr) error
	walk = func(expr *Expr) error {
		if expr == nil {
			return nil
		}
		switch state[expr] {
		case 1:
			return persistedNumericProgramError(ctx, "expression graph contains a cycle")
		case 2:
			return nil
		}
		state[expr] = 1
		if err := visit(expr); err != nil {
			return err
		}
		for _, child := range childrenOf(expr) {
			if err := walk(child); err != nil {
				return err
			}
		}
		state[expr] = 2
		return nil
	}
	for _, root := range roots {
		if err := walk(root); err != nil {
			return err
		}
	}
	return nil
}

func persistedExprChildren(expr *Expr) []*Expr {
	return persistedExprChildrenWithRetainedSources(expr, true)
}

func persistedExecutableExprChildren(expr *Expr) []*Expr {
	return persistedExprChildrenWithRetainedSources(expr, false)
}

func persistedExprChildrenWithRetainedSources(expr *Expr, includeRetainedSources bool) []*Expr {
	children := make([]*Expr, 0, 4)
	switch node := expr.Expr.(type) {
	case *planpb.Expr_F:
		if node.F != nil {
			children = append(children, node.F.Args...)
		}
	case *planpb.Expr_Lit:
	case *planpb.Expr_List:
		if node.List != nil {
			children = append(children, node.List.List...)
		}
	case *planpb.Expr_Sub:
		if node.Sub != nil {
			children = append(children, node.Sub.Child)
		}
	case *planpb.Expr_W:
		if node.W != nil {
			children = append(children, node.W.WindowFunc)
			children = append(children, node.W.PartitionBy...)
			for _, order := range node.W.OrderBy {
				if order != nil {
					children = append(children, order.Expr)
				}
			}
			if node.W.Frame != nil {
				if node.W.Frame.Start != nil {
					children = append(children, node.W.Frame.Start.Val)
				}
				if node.W.Frame.End != nil {
					children = append(children, node.W.Frame.End.Val)
				}
			}
		}
	case *planpb.Expr_Dependent:
		if node.Dependent != nil {
			children = append(children, node.Dependent.Inputs...)
			children = append(children, node.Dependent.Lhs...)
			children = append(children, node.Dependent.Comparison)
		}
	}
	if includeRetainedSources {
		if literal := expr.GetLit(); literal != nil {
			children = append(children, literal.Src)
		}
		if prepared := expr.GetPreparedNumeric(); prepared != nil {
			children = append(children, prepared.StringDomainSource)
		}
	}
	return children
}

func persistedNumericProgramError(ctx context.Context, reason string) error {
	return moerr.NewInvalidInputf(ctx, "invalid persisted numeric value program: %s", reason)
}

func setNumericPlanInt64(expr *Expr, value int64) {
	if expr == nil || expr.GetLit() == nil {
		return
	}
	if literal, ok := expr.GetLit().Value.(*planpb.Literal_I64Val); ok && literal != nil {
		literal.I64Val = value
	}
}

func allocateNumericValueProgramScope(ctx context.Context, reserved map[int64]struct{}) (int64, error) {
	for {
		scopeID := nextNumericValueProgramScope.Add(1)
		if scopeID == 0 || scopeID > math.MaxInt64 {
			return 0, moerr.NewInternalError(ctx, "numeric value program scope exhausted")
		}
		scope := int64(scopeID)
		if _, exists := reserved[scope]; !exists {
			return scope, nil
		}
	}
}
