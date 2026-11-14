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

type privateCopyPartitionKey struct{}

type privateCopyPartitionGrant struct {
	database string
	parent   string
	children map[string]struct{}
	derived  bool
}

// WithPrivateCopyPartitionParent scopes the physical children of an already
// validated private COPY replica. It does not itself authorize a SQL CREATE.
func WithPrivateCopyPartitionParent(ctx context.Context, database, parent string) (context.Context, error) {
	if ctx == nil || database == "" || parent == "" {
		return nil, moerr.NewInternalErrorNoCtx("invalid private COPY partition owner")
	}
	return context.WithValue(ctx, privateCopyPartitionKey{}, &privateCopyPartitionGrant{
		database: database, parent: strings.ToLower(parent),
	}), nil
}

func HasPrivateCopyPartitionParent(ctx context.Context, database, parent string) bool {
	if ctx == nil {
		return false
	}
	grant, _ := ctx.Value(privateCopyPartitionKey{}).(*privateCopyPartitionGrant)
	return grant != nil && !grant.derived && grant.database == database && grant.parent == strings.ToLower(parent)
}

// WithPrivateCopyPartitionChildren is called after the partition service
// validates the parent's identity and its canonical child names.
func WithPrivateCopyPartitionChildren(ctx context.Context, database, parent string, children []string) (context.Context, error) {
	if !HasPrivateCopyPartitionParent(ctx, database, parent) {
		return nil, moerr.NewInternalErrorNoCtx("private COPY partition owner mismatch")
	}
	names := make(map[string]struct{}, len(children))
	for _, child := range children {
		name := strings.ToLower(child)
		if name == "" || name == strings.ToLower(parent) {
			return nil, moerr.NewInternalErrorNoCtx("invalid private COPY partition child")
		}
		if _, exists := names[name]; exists {
			return nil, moerr.NewInternalErrorNoCtx("duplicate private COPY partition child")
		}
		names[name] = struct{}{}
	}
	return context.WithValue(ctx, privateCopyPartitionKey{}, &privateCopyPartitionGrant{
		database: database, parent: strings.ToLower(parent), children: names, derived: true,
	}), nil
}

func IsPrivateCopyPartitionChild(ctx context.Context, database, parent, child string) bool {
	if ctx == nil {
		return false
	}
	grant, _ := ctx.Value(privateCopyPartitionKey{}).(*privateCopyPartitionGrant)
	if grant == nil || !grant.derived || grant.database != database || grant.parent != strings.ToLower(parent) {
		return false
	}
	_, ok := grant.children[strings.ToLower(child)]
	return ok
}
