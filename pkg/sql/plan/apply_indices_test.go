// Copyright 2024 Matrix Origin
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

package plan

import (
	"testing"
)

func TestCalculatePostFilterOverFetchFactor(t *testing.T) {
	tests := []struct {
		name          string
		originalLimit uint64
		expectedMin   float64
		expectedMax   float64
	}{
		// Small limits (< 10): should return 5.0x
		{
			name:          "Very small limit - 1",
			originalLimit: 1,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},
		{
			name:          "Very small limit - 3",
			originalLimit: 3,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},
		{
			name:          "Small limit - 5",
			originalLimit: 5,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},
		{
			name:          "Small limit boundary - 9",
			originalLimit: 9,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},

		// Medium limits (10-49): should return 2.0x
		{
			name:          "Medium limit lower boundary - 10",
			originalLimit: 10,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},
		{
			name:          "Medium limit - 20",
			originalLimit: 20,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},
		{
			name:          "Medium limit - 30",
			originalLimit: 30,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},
		{
			name:          "Medium limit upper boundary - 49",
			originalLimit: 49,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},

		// Large limits (50-99): should return 1.5x
		{
			name:          "Large limit lower boundary - 50",
			originalLimit: 50,
			expectedMin:   1.5,
			expectedMax:   1.5,
		},
		{
			name:          "Large limit - 75",
			originalLimit: 75,
			expectedMin:   1.5,
			expectedMax:   1.5,
		},
		{
			name:          "Large limit upper boundary - 99",
			originalLimit: 99,
			expectedMin:   1.5,
			expectedMax:   1.5,
		},

		// Very large limits (100-199): should return 1.3x
		{
			name:          "Very large limit lower boundary - 100",
			originalLimit: 100,
			expectedMin:   1.3,
			expectedMax:   1.3,
		},
		{
			name:          "Very large limit - 150",
			originalLimit: 150,
			expectedMin:   1.3,
			expectedMax:   1.3,
		},
		{
			name:          "Very large limit upper boundary - 199",
			originalLimit: 199,
			expectedMin:   1.3,
			expectedMax:   1.3,
		},

		// Huge limits (200+): should return 1.2x
		{
			name:          "Huge limit lower boundary - 200",
			originalLimit: 200,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
		{
			name:          "Huge limit - 500",
			originalLimit: 500,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
		{
			name:          "Huge limit - 1000",
			originalLimit: 1000,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
		{
			name:          "Huge limit - 10000",
			originalLimit: 10000,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculatePostFilterOverFetchFactor(tt.originalLimit)

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("calculatePostFilterOverFetchFactor(%d) = %f, want between %f and %f",
					tt.originalLimit, result, tt.expectedMin, tt.expectedMax)
			}

			// Verify the result is positive
			if result <= 0 {
				t.Errorf("calculatePostFilterOverFetchFactor(%d) = %f, want positive value",
					tt.originalLimit, result)
			}

			// Verify the result is at least 1.0 (must fetch at least original limit)
			if result < 1.0 {
				t.Errorf("calculatePostFilterOverFetchFactor(%d) = %f, want >= 1.0",
					tt.originalLimit, result)
			}
		})
	}
}

// Test the actual over-fetch calculation results
func TestCalculatePostFilterOverFetchFactor_ActualValues(t *testing.T) {
	testCases := []struct {
		limit         uint64
		expectedFetch uint64 // expected number of rows to fetch
	}{
		// Small limits (5x factor)
		{limit: 3, expectedFetch: 15},  // 3 * 5 = 15
		{limit: 5, expectedFetch: 25},  // 5 * 5 = 25
		{limit: 10, expectedFetch: 20}, // 10 * 2 = 20 (crosses boundary)

		// Medium limits (2x factor)
		{limit: 20, expectedFetch: 40}, // 20 * 2 = 40
		{limit: 30, expectedFetch: 60}, // 30 * 2 = 60
		{limit: 49, expectedFetch: 98}, // 49 * 2 = 98
		{limit: 50, expectedFetch: 75}, // 50 * 1.5 = 75 (crosses boundary)

		// Large limits (1.5x factor)
		{limit: 80, expectedFetch: 120},  // 80 * 1.5 = 120
		{limit: 99, expectedFetch: 148},  // 99 * 1.5 = 148.5, truncated to 148
		{limit: 100, expectedFetch: 130}, // 100 * 1.3 = 130 (crosses boundary)

		// Very large limits (1.3x factor)
		{limit: 150, expectedFetch: 195}, // 150 * 1.3 = 195
		{limit: 199, expectedFetch: 258}, // 199 * 1.3 = 258.7, truncated to 258
		{limit: 200, expectedFetch: 240}, // 200 * 1.2 = 240 (crosses boundary)

		// Huge limits (1.2x factor)
		{limit: 250, expectedFetch: 300},   // 250 * 1.2 = 300
		{limit: 500, expectedFetch: 600},   // 500 * 1.2 = 600
		{limit: 1000, expectedFetch: 1200}, // 1000 * 1.2 = 1200
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			factor := calculatePostFilterOverFetchFactor(tc.limit)
			actualFetch := uint64(float64(tc.limit) * factor)

			if actualFetch != tc.expectedFetch {
				t.Errorf("For limit %d: got fetch %d, want %d (factor: %f)",
					tc.limit, actualFetch, tc.expectedFetch, factor)
			}
		})
	}
}

// Benchmark the function to ensure it's fast
func BenchmarkCalculatePostFilterOverFetchFactor(b *testing.B) {
	limits := []uint64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 10000}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, limit := range limits {
			_ = calculatePostFilterOverFetchFactor(limit)
		}
	}
}

// Test edge case: zero limit (defensive programming)
func TestCalculatePostFilterOverFetchFactor_EdgeCases(t *testing.T) {
	// Test with zero - should still work (though not expected in real usage)
	result := calculatePostFilterOverFetchFactor(0)
	if result != 5.0 {
		t.Errorf("calculatePostFilterOverFetchFactor(0) = %f, want 5.0", result)
	}

	// Test with max uint64 value
	result = calculatePostFilterOverFetchFactor(^uint64(0))
	if result != 1.2 {
		t.Errorf("calculatePostFilterOverFetchFactor(max_uint64) = %f, want 1.2", result)
	}
}

// Test that the factor decreases as limit increases (monotonic property)
func TestCalculatePostFilterOverFetchFactor_MonotonicDecrease(t *testing.T) {
	testLimits := []uint64{1, 5, 10, 20, 50, 100, 200, 500, 1000}

	var prevFactor float64 = 10.0 // Start with a high value

	for _, limit := range testLimits {
		currentFactor := calculatePostFilterOverFetchFactor(limit)

		if currentFactor > prevFactor {
			t.Errorf("Factor should decrease as limit increases: limit=%d factor=%f > previous factor=%f",
				limit, currentFactor, prevFactor)
		}

		prevFactor = currentFactor
	}
}
