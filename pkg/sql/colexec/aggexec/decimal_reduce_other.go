//go:build !amd64 || !go1.27 || !goexperiment.simd

// Copyright 2021 Matrix Origin
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

package aggexec

import "github.com/matrixorigin/matrixone/pkg/container/vector"

func decimalReductionAvailable() bool { return false }

func reduceDecimalPrefix(_ *vector.Vector, _ int, _ int) decimalReductionPrefix {
	return decimalReductionPrefix{}
}
