// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package hnsw

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestQuantization(t *testing.T) {
	q, ok := QuantizationValid("f16")
	require.True(t, ok)
	require.Equal(t, q, usearch.F16)
	q, ok = QuantizationValid("")
	require.False(t, ok)
}

func TestUSearch(t *testing.T) {

	// Create Index
	vectorSize := 3
	vectorsCount := 100
	conf := usearch.DefaultConfig(uint(vectorSize))
	index, err := usearch.NewIndex(conf)
	if err != nil {
		panic("Failed to create Index")
	}
	defer index.Destroy()

	// Add to Index
	err = index.Reserve(uint(vectorsCount))
	if err != nil {
		panic("Failed to reserve")
	}
	for i := 0; i < vectorsCount; i++ {
		err = index.Add(usearch.Key(i), []float32{float32(i), float32(i + 1), float32(i + 2)})
		if err != nil {
			panic("Failed to add")
		}
	}

	// Search
	keys, distances, err := index.Search([]float32{0.0, 1.0, 2.0}, 3)
	if err != nil {
		panic("Failed to search")
	}
	fmt.Println(keys, distances)
}
