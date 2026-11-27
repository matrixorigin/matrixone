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

import "context"

type exactCatalogRowsKey struct{}

// WithExactCatalogRows marks bounded internal catalog operations whose keys
// must remain independent of other accounts, even under lock budget pressure.
func WithExactCatalogRows(ctx context.Context) context.Context {
	return context.WithValue(ctx, exactCatalogRowsKey{}, true)
}

func ExactCatalogRows(ctx context.Context) bool {
	return ctx != nil && ctx.Value(exactCatalogRowsKey{}) == true
}
