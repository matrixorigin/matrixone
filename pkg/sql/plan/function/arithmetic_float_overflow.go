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

package function

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func addFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	result := v1 + v2
	if math.IsInf(result, 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float64", "DOUBLE value is out of range in '(%v + %v)'", v1, v2)
	}
	return result, nil
}

func addFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	result := v1 + v2
	if math.IsInf(float64(result), 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float32", "FLOAT value is out of range in '(%v + %v)'", v1, v2)
	}
	return result, nil
}
