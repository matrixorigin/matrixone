// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"strings"

	usearch "github.com/unum-cloud/usearch/golang"
)

/*
quantization := map[string]usearch.Quantization{"BF16": usearch.BF16, "F16": usearch.F16,

	"F32": usearch.F32, "F64": usearch.F64, "I8": usearch.I8, "B1": usearch.B1}
*/
func QuantizationValid(a string) (usearch.Quantization, bool) {
	q := strings.ToUpper(a)
	// we can only support below quantization
	quantization := map[string]usearch.Quantization{"F16": usearch.F16,
		"F32": usearch.F32, "I8": usearch.I8}
	r, ok := quantization[q]
	return r, ok
}
