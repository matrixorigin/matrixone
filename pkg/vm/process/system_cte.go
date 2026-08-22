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

package process

import "context"

const SystemCTEMaxRecursionDepth = int64(65536)

type systemCTELimitsKey struct{}

// WithSystemCTELimits marks one internal catalog statement as independent of
// user recursive-CTE limits. Cancellation and the surrounding transaction are
// intentionally unchanged.
func WithSystemCTELimits(ctx context.Context) context.Context {
	return context.WithValue(ctx, systemCTELimitsKey{}, true)
}

func HasSystemCTELimits(ctx context.Context) bool {
	enabled, _ := ctx.Value(systemCTELimitsKey{}).(bool)
	return enabled
}

func SystemCTEResolver(
	resolver func(string, bool, bool) (interface{}, error),
) func(string, bool, bool) (interface{}, error) {
	return func(name string, system, global bool) (interface{}, error) {
		switch name {
		case "cte_max_recursion_depth":
			return SystemCTEMaxRecursionDepth, nil
		case CTEMemoryQuotaVariable:
			return int64(DefaultCTEMemoryQuotaBytes), nil
		default:
			if resolver == nil {
				return nil, nil
			}
			return resolver(name, system, global)
		}
	}
}
