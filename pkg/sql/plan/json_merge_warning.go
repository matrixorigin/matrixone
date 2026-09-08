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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const JSONMergeDeprecatedWarning = "'JSON_MERGE' is deprecated and will be removed in a future release. Please use JSON_MERGE_PRESERVE/JSON_MERGE_PATCH instead"

// JSONMergeWarningSink is intentionally smaller than process.Session.  The
// planner only needs the optional diagnostic append operation and must remain
// usable by tests and compiler contexts that do not have a frontend session.
type JSONMergeWarningSink interface {
	AppendWarningDiagnostic(code uint16, msg string)
}

// JSONMergeWarningOrigin identifies why a statement is being bound.  A plan
// rebuilt internally for EXECUTE and a persisted view expansion must not
// replay the warning emitted for the user's original PREPARE/statement.
type JSONMergeWarningOrigin uint8

const (
	JSONMergeWarningUser JSONMergeWarningOrigin = iota
	JSONMergeWarningInternalReprepare
	JSONMergeWarningStoredView
)

type jsonMergeWarningContextKey struct{}

type jsonMergeWarningContext struct {
	sink   JSONMergeWarningSink
	origin JSONMergeWarningOrigin
	seen   map[*tree.FuncExpr]struct{}
}

// WithJSONMergeWarningContext installs the diagnostic sink and starts a new
// syntactic-call-site set for one top-level plan build.
func WithJSONMergeWarningContext(
	ctx context.Context,
	sink JSONMergeWarningSink,
	origin JSONMergeWarningOrigin,
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, jsonMergeWarningContextKey{}, &jsonMergeWarningContext{
		sink:   sink,
		origin: origin,
		seen:   make(map[*tree.FuncExpr]struct{}),
	})
}

// WithJSONMergeWarningOrigin changes only the lifecycle origin, preserving the
// sink and syntactic-call-site state already attached to the planning context.
func WithJSONMergeWarningOrigin(ctx context.Context, origin JSONMergeWarningOrigin) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	current, _ := ctx.Value(jsonMergeWarningContextKey{}).(*jsonMergeWarningContext)
	if current == nil {
		return WithJSONMergeWarningContext(ctx, nil, origin)
	}
	copy := *current
	copy.origin = origin
	return context.WithValue(ctx, jsonMergeWarningContextKey{}, &copy)
}

// JSONMergeWarningOriginFromContext returns the explicitly selected planning
// origin, if a frontend caller installed one.
func JSONMergeWarningOriginFromContext(ctx context.Context) (JSONMergeWarningOrigin, bool) {
	if ctx == nil {
		return JSONMergeWarningUser, false
	}
	current, ok := ctx.Value(jsonMergeWarningContextKey{}).(*jsonMergeWarningContext)
	if !ok || current == nil {
		return JSONMergeWarningUser, false
	}
	return current.origin, true
}

func appendJSONMergeWarning(ctx context.Context, expr *tree.FuncExpr) {
	if ctx == nil || expr == nil {
		return
	}
	current, ok := ctx.Value(jsonMergeWarningContextKey{}).(*jsonMergeWarningContext)
	if !ok || current == nil || current.origin != JSONMergeWarningUser || current.sink == nil {
		return
	}
	if current.seen == nil {
		current.seen = make(map[*tree.FuncExpr]struct{})
	}
	if _, exists := current.seen[expr]; exists {
		return
	}
	current.seen[expr] = struct{}{}
	current.sink.AppendWarningDiagnostic(moerr.ER_WARN_DEPRECATED_SYNTAX, JSONMergeDeprecatedWarning)
}
