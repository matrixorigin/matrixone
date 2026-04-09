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

import (
	"encoding/json"
	"testing"
)

func TestCoverageStatusValues(t *testing.T) {
	if StatusCovered != "covered" {
		t.Errorf("StatusCovered = %q, want %q", StatusCovered, "covered")
	}
	if StatusNeedsAttention != "needs_attention" {
		t.Errorf("StatusNeedsAttention = %q, want %q", StatusNeedsAttention, "needs_attention")
	}
	if StatusNotRelated != "not_related" {
		t.Errorf("StatusNotRelated = %q, want %q", StatusNotRelated, "not_related")
	}
}

func TestTestTypeValues(t *testing.T) {
	expected := map[TestType]string{
		TestBVT:       "bvt",
		TestStability: "stability",
		TestChaos:     "chaos",
		TestBigData:   "bigdata",
		TestPITR:      "pitr",
		TestSnapshot:  "snapshot",
	}
	for tt, want := range expected {
		if string(tt) != want {
			t.Errorf("TestType %v = %q, want %q", tt, string(tt), want)
		}
	}
}

func TestCoverageReportJSON(t *testing.T) {
	report := CoverageReport{
		PRNumber:        123,
		Summary:         "test summary",
		AffectedModules: []string{"pkg/sql"},
		Coverage: []CoverageItem{
			{Type: TestBVT, Status: StatusCovered, Description: "ok"},
			{Type: TestStability, Status: StatusNotRelated, Description: "n/a"},
		},
		SuggestedCases: []SuggestedCase{
			{
				Type:     TestBVT,
				Category: "function",
				Filename: "test1.test",
				Content:  "SELECT 1;",
				Reason:   "missing coverage",
			},
		},
	}

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got CoverageReport
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.PRNumber != 123 {
		t.Errorf("PRNumber = %d, want 123", got.PRNumber)
	}
	if got.Summary != "test summary" {
		t.Errorf("Summary = %q, want %q", got.Summary, "test summary")
	}
	if len(got.Coverage) != 2 {
		t.Fatalf("Coverage len = %d, want 2", len(got.Coverage))
	}
	if got.Coverage[0].Type != TestBVT {
		t.Errorf("Coverage[0].Type = %q, want %q", got.Coverage[0].Type, TestBVT)
	}
	if got.Coverage[0].Status != StatusCovered {
		t.Errorf("Coverage[0].Status = %q, want %q", got.Coverage[0].Status, StatusCovered)
	}
	if len(got.SuggestedCases) != 1 {
		t.Fatalf("SuggestedCases len = %d, want 1", len(got.SuggestedCases))
	}
	if got.SuggestedCases[0].Content != "SELECT 1;" {
		t.Errorf("SuggestedCases[0].Content = %q", got.SuggestedCases[0].Content)
	}
}
