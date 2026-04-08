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

package planner

import (
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// Planner generates a TestPlan from a DiffSummary using the Matcher.
type Planner struct {
	matcher *Matcher
}

// NewPlanner creates a Planner with the default mappings.
func NewPlanner() *Planner {
	return &Planner{matcher: NewDefaultMatcher()}
}

// NewPlannerWithMatcher creates a Planner with a custom Matcher.
func NewPlannerWithMatcher(m *Matcher) *Planner {
	return &Planner{matcher: m}
}

// GeneratePlan produces a TestPlan for the given diff summary.
func (p *Planner) GeneratePlan(diff *types.DiffSummary) *types.TestPlan {
	plan := &types.TestPlan{
		ID:          fmt.Sprintf("tp-pr%d-%d", diff.PRNumber, time.Now().Unix()),
		PRNumber:    diff.PRNumber,
		BaseBranch:  diff.BaseBranch,
		HeadBranch:  diff.HeadBranch,
		CreatedAt:   time.Now(),
		DiffSummary: *diff,
	}

	result := p.matcher.MatchAll(diff.Files)

	taskID := 0

	// Always run SCA if any Go files changed.
	if hasGoFiles(diff.Files) {
		taskID++
		plan.Tasks = append(plan.Tasks, types.TestTask{
			ID:       fmt.Sprintf("task-%d", taskID),
			Type:     types.TestTypeSCA,
			Priority: types.PriorityCritical,
			Status:   types.TaskStatusPending,
			Reason:   "Go source files changed – static analysis required",
		})
	}

	// Generate UT tasks for each affected package.
	for _, pkg := range result.UTPackages {
		taskID++
		plan.Tasks = append(plan.Tasks, types.TestTask{
			ID:       fmt.Sprintf("task-%d", taskID),
			Type:     types.TestTypeUT,
			Package:  pkg,
			Priority: result.Priority,
			Status:   types.TaskStatusPending,
			Reason:   fmt.Sprintf("unit tests for affected package %s", pkg),
		})
	}

	// Generate BVT tasks for each affected category.
	for _, cat := range result.BVTCategories {
		taskID++
		plan.Tasks = append(plan.Tasks, types.TestTask{
			ID:       fmt.Sprintf("task-%d", taskID),
			Type:     types.TestTypeBVT,
			Category: cat,
			Priority: result.Priority,
			Status:   types.TaskStatusPending,
			Reason:   fmt.Sprintf("BVT category %s mapped from code changes", string(cat)),
		})
	}

	plan.Summary = p.buildSummary(diff, plan)
	return plan
}

// GeneratePlanFromDiff is a convenience function that parses a unified diff
// string and generates a TestPlan in one step.
func (p *Planner) GeneratePlanFromDiff(diffText string, prNumber int, baseBranch, headBranch string) *types.TestPlan {
	diff := ParseUnifiedDiff(diffText)
	diff.PRNumber = prNumber
	diff.BaseBranch = baseBranch
	diff.HeadBranch = headBranch
	return p.GeneratePlan(diff)
}

func (p *Planner) buildSummary(diff *types.DiffSummary, plan *types.TestPlan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "TestPlan for PR #%d (%s → %s)\n", diff.PRNumber, diff.HeadBranch, diff.BaseBranch)
	fmt.Fprintf(&b, "Files changed: %d (+%d/-%d)\n", len(diff.Files), diff.TotalAdded, diff.TotalDeleted)
	fmt.Fprintf(&b, "Affected packages: %s\n", strings.Join(diff.AffectedPackages(), ", "))

	byType := plan.TaskCountByType()
	fmt.Fprintf(&b, "Tasks: %d total (UT: %d, BVT: %d, SCA: %d)",
		len(plan.Tasks), byType[types.TestTypeUT], byType[types.TestTypeBVT], byType[types.TestTypeSCA])
	return b.String()
}

func hasGoFiles(files []types.FileChange) bool {
	for _, f := range files {
		if strings.HasSuffix(f.Path, ".go") {
			return true
		}
	}
	return false
}
