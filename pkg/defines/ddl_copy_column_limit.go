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

package defines

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type alterCopyColumnLimitKey struct{}

type alterCopyColumnLimitGrant struct {
	database string
	parent   string
	limit    int
	derived  bool
	children map[string]struct{}
}

// WithoutAlterCopyColumnLimit masks reconstruction authority inherited from a
// previous COPY. Expression-only replay must not retain a width allowance.
func WithoutAlterCopyColumnLimit(ctx context.Context) context.Context {
	if ctx == nil || alterCopyColumnGrant(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, alterCopyColumnLimitKey{}, (*alterCopyColumnLimitGrant)(nil))
}

func alterCopyColumnGrant(ctx context.Context) *alterCopyColumnLimitGrant {
	if ctx == nil {
		return nil
	}
	grant, _ := ctx.Value(alterCopyColumnLimitKey{}).(*alterCopyColumnLimitGrant)
	return grant
}

// WithAlterCopyColumnLimit is called only after the live COPY target width has
// been validated. Database is the resolved database, not a name prefix.
func WithAlterCopyColumnLimit(ctx context.Context, database, table string, limit int) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = WithoutAlterCopyColumnLimit(ctx)
	if database == "" || table == "" || limit <= 0 {
		return ctx
	}
	return context.WithValue(ctx, alterCopyColumnLimitKey{}, &alterCopyColumnLimitGrant{
		database: database, parent: strings.ToLower(table), limit: limit,
	})
}

// AlterCopyColumnLimit returns authority only for the exact reconstruction
// identity. The maps in published grants are immutable.
func AlterCopyColumnLimit(ctx context.Context, database, table string) int {
	grant := alterCopyColumnGrant(ctx)
	if grant == nil || database != grant.database {
		return 0
	}
	name := strings.ToLower(table)
	if !grant.derived {
		if name == grant.parent {
			return grant.limit
		}
		return 0
	}
	if _, ok := grant.children[name]; ok {
		return grant.limit
	}
	return 0
}

// WithAlterCopyPartitionColumnLimit derives child-only authority from a
// validated parent grant. Storage supplies its actual canonical child names;
// a derived grant cannot delegate, and the caller context is never mutated.
func WithAlterCopyPartitionColumnLimit(ctx context.Context, database, parent string, children []string) (context.Context, error) {
	grant := alterCopyColumnGrant(ctx)
	if grant == nil {
		return ctx, nil
	}
	if grant.derived || database != grant.database || strings.ToLower(parent) != grant.parent {
		return nil, moerr.NewInternalError(ctx, "partition COPY column allowance owner mismatch")
	}
	names := make(map[string]struct{}, len(children))
	for _, child := range children {
		name := strings.ToLower(child)
		if name == "" || name == grant.parent {
			return nil, moerr.NewInternalError(ctx, "invalid partition COPY column allowance target")
		}
		if _, exists := names[name]; exists {
			return nil, moerr.NewInternalError(ctx, "duplicate partition COPY column allowance target")
		}
		names[name] = struct{}{}
	}
	return context.WithValue(ctx, alterCopyColumnLimitKey{}, &alterCopyColumnLimitGrant{
		database: grant.database, parent: grant.parent, limit: grant.limit,
		derived: true, children: names,
	}), nil
}
