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

package engine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// CollectChangesSchema describes the logical data-column layout expected by a
// CollectChanges caller. Seqnums are stable physical identities; their slice
// positions are the logical output positions. The two must remain distinct
// after DROP/ADD COLUMN creates gaps in physical sequence numbers.
type CollectChangesSchema struct {
	Attrs   []string
	Types   []types.Type
	Seqnums []uint16
}

func (s *CollectChangesSchema) Valid() bool {
	return s != nil && len(s.Attrs) > 0 &&
		len(s.Attrs) == len(s.Types) && len(s.Attrs) == len(s.Seqnums)
}

type collectChangesSchemaContextKey struct{}

// WithCollectChangesSchema attaches an immutable logical output schema to a
// CollectChanges request. Invalid schemas are ignored so legacy implementations
// that do not publish schema metadata retain their positional behavior.
func WithCollectChangesSchema(ctx context.Context, schema *CollectChangesSchema) context.Context {
	if ctx == nil || !schema.Valid() {
		return ctx
	}
	cloned := &CollectChangesSchema{
		Attrs:   append([]string(nil), schema.Attrs...),
		Types:   append([]types.Type(nil), schema.Types...),
		Seqnums: append([]uint16(nil), schema.Seqnums...),
	}
	return context.WithValue(ctx, collectChangesSchemaContextKey{}, cloned)
}

// CollectChangesSchemaFromContext returns the immutable schema attached by
// WithCollectChangesSchema, or nil for legacy callers.
func CollectChangesSchemaFromContext(ctx context.Context) *CollectChangesSchema {
	if ctx == nil {
		return nil
	}
	schema, _ := ctx.Value(collectChangesSchemaContextKey{}).(*CollectChangesSchema)
	return schema
}
