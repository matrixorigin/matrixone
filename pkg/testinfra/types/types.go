// Copyright 2024 Matrix Origin
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

package types

// CoverageStatus represents the test coverage state for a test category.
type CoverageStatus string

const (
	StatusCovered        CoverageStatus = "covered"
	StatusNeedsAttention CoverageStatus = "needs_attention"
	StatusNotRelated     CoverageStatus = "not_related"
)

// TestType represents the 6 test categories in MO's test infrastructure.
type TestType string

const (
	TestBVT       TestType = "bvt"
	TestStability TestType = "stability"
	TestChaos     TestType = "chaos"
	TestBigData   TestType = "bigdata"
	TestPITR      TestType = "pitr"
	TestSnapshot  TestType = "snapshot"
)

// CoverageItem describes coverage status for one test type.
type CoverageItem struct {
	Type        TestType       `json:"type"`
	Status      CoverageStatus `json:"status"`
	Description string         `json:"description"`
}

// SuggestedCase is a test case the AI suggests adding.
type SuggestedCase struct {
	Type     TestType `json:"type"`
	Category string   `json:"category"`
	Filename string   `json:"filename"`
	Content  string   `json:"content"`
	Reason   string   `json:"reason"`
}

// CoverageReport is the structured output of the analysis.
type CoverageReport struct {
	PRNumber        int             `json:"pr_number"`
	Summary         string          `json:"summary"`
	AffectedModules []string        `json:"affected_modules"`
	Coverage        []CoverageItem  `json:"coverage"`
	SuggestedCases  []SuggestedCase `json:"suggested_cases"`
}
