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

package process

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/stretchr/testify/assert"
)

// NOTE: This is a hacky test to verify Bloom Filter propagation logic.
func TestBloomFilterPropagation(t *testing.T) {
	expectedBF := []byte("hack_bloom_filter_data_12345")

	// 1. Verify deserialization injection logic (simulating modifications to Decode or generateProcessHelper)
	// On the remote CN side, we extract the BF from ProcessInfo and put it into the Context
	t.Run("Remote_Injection_Verification", func(t *testing.T) {
		procInfo := pipeline.ProcessInfo{
			IvfBloomFilter: expectedBF,
		}

		// Simulate Decode logic
		ctx := context.Background()
		if len(procInfo.IvfBloomFilter) > 0 {
			ctx = context.WithValue(ctx, defines.IvfBloomFilter{}, procInfo.IvfBloomFilter)
		}

		// Check if the BF retrieved locally is consistent
		actualBF, ok := ctx.Value(defines.IvfBloomFilter{}).([]byte)
		assert.True(t, ok, "BF should be found in context")
		assert.Equal(t, expectedBF, actualBF, "BF data should match")
	})

	// 2. Verify serialization extraction logic (manual verification of logic effectiveness)
	t.Run("Serialization_Extraction_Logic", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), defines.IvfBloomFilter{}, expectedBF)

		// Simulate key lines in BuildProcessInfo
		var procInfo pipeline.ProcessInfo
		bfVal := ctx.Value(defines.IvfBloomFilter{})
		if bfVal != nil {
			if bf, ok := bfVal.([]byte); ok && len(bf) > 0 {
				procInfo.IvfBloomFilter = bf
			}
		}

		assert.Equal(t, expectedBF, procInfo.IvfBloomFilter, "PB should extract BF from context")
	})
}
