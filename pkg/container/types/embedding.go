// Copyright 2023 Matrix Origin
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

package types

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

const (
	// MaxEmbeddingDimension Based on here: https://github.com/pgvector/pgvector/blob/b56971febeec389a011de7bb40b3349e24757aff/src/vector.h#L10
	MaxEmbeddingDimension = 16000
)

func BytesToEmbedding(input []byte) (res []float32) {
	if len(input)%4 != 0 {
		panic(moerr.NewInternalErrorNoCtx("the byte slice length must be a multiple of 4"))
	}

	dimension := len(input) / 4
	res = make([]float32, dimension)

	// Get the starting address of the byte slice
	ptr := unsafe.Pointer(&input[0])

	for i := 0; i < dimension; i++ {
		res[i] = math.Float32frombits(*(*uint32)(ptr))
		ptr = unsafe.Pointer(uintptr(ptr) + 4) // Increment the pointer by 4 bytes for the next iteration
	}

	return res
}

func EmbeddingToBytes(input []float32) []byte {
	totalBytes := len(input) * 4
	res := make([]byte, totalBytes)

	// Get the starting address of the byte slice
	ptr := unsafe.Pointer(&res[0])

	for _, val := range input {
		*(*uint32)(ptr) = math.Float32bits(val)
		ptr = unsafe.Pointer(uintptr(ptr) + 4) // Increment the pointer by 4 bytes for the next iteration
	}
	return res
}

func EmbeddingToString(input []float32) string {
	var strValues []string
	for _, value := range input {
		//TODO: Float decimal place
		strValues = append(strValues, fmt.Sprintf("%f", value))
	}
	return "[" + strings.Join(strValues, ", ") + "]"
}

func StringToEmbedding(input string) []float32 {
	input = strings.ReplaceAll(input, "[", "")
	input = strings.ReplaceAll(input, "]", "")
	input = strings.ReplaceAll(input, " ", "")

	numbers := strings.Split(input, ",")
	result := make([]float32, len(numbers))

	for i, numStr := range numbers {
		num, err := strconv.ParseFloat(numStr, 32)
		if err != nil {
			panic(err)
		}
		result[i] = float32(num)
	}

	return result
}

func CompareEmbedding(left, right []float32) int64 {

	if len(left) != len(right) {
		panic("Embeddings size mismatch")
	}

	for i := 0; i < len(left); i++ {
		if left[i] == right[i] {
			continue
		} else if left[i] > right[i] {
			return +1
		} else {
			return -1
		}
	}

	return 0
}

// Not useful.
func EmbeddingsToString(input [][]float32) string {
	var strValues []string
	for _, row := range input {
		strValues = append(strValues, EmbeddingToString(row))
	}
	return strings.Join(strValues, " ")
}
