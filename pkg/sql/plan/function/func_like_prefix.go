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

package function

import (
	"context"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func tryGeneralCIPrefixLike(
	ctx context.Context, values, patterns vector.FunctionParameterWrapper[types.Varlena],
	result *vector.FunctionResult[bool], length int,
) (bool, error) {
	pattern, null := patterns.GetStrValue(0)
	if null || len(pattern) < 2 || pattern[len(pattern)-1] != '%' {
		return false, nil
	}
	prefix := pattern[:len(pattern)-1]
	for _, b := range prefix {
		if b >= utf8.RuneSelf || b == '%' || b == '_' || b == DefaultEscapeChar {
			return false, nil
		}
	}
	for row := uint64(0); row < uint64(length); row++ {
		if likeBatchCanceled(ctx, row) {
			return true, ctx.Err()
		}
		value, null := values.GetStrValue(row)
		matched := !null && generalCIHasASCIIPrefix(value, prefix)
		if err := result.Append(matched, null); err != nil {
			return true, err
		}
	}
	return true, nil
}

// general_ci LIKE folds one character to one weight rune. With an ASCII
// literal prefix, compare those weights directly instead of allocating and
// folding the complete value. '%' observes none of the remaining suffix.
func generalCIHasASCIIPrefix(value, prefix []byte) bool {
	position := 0
	for _, wanted := range prefix {
		if wanted >= 'a' && wanted <= 'z' {
			wanted -= 'a' - 'A'
		}
		if position >= len(value) {
			return false
		}
		var weight uint32
		if b := value[position]; b < utf8.RuneSelf {
			if b >= 'a' && b <= 'z' {
				b -= 'a' - 'A'
			}
			weight = uint32(b)
			position++
		} else {
			r, size := utf8.DecodeRune(value[position:])
			if r == utf8.RuneError && size == 1 {
				// LIKE's synthetic malformed-byte weight is above ASCII.
				return false
			}
			weight = collation.GeneralCIWeightRune(r)
			position += size
		}
		if weight != uint32(wanted) {
			return false
		}
	}
	return true
}
