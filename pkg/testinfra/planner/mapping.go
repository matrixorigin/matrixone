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

// Package planner implements the TestPlan generation logic. It analyses PR
// diffs, maps code changes to test categories, and produces a structured
// TestPlan that can be consumed by the executor.
package planner

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// PathMapping defines the static mapping between source code path prefixes
// and the BVT test categories / UT packages they are expected to exercise.
type PathMapping struct {
	// PathPrefix is matched against the beginning of a changed file path.
	PathPrefix string
	// UTPackages lists the Go packages whose unit tests should be run.
	UTPackages []string
	// BVTCategories lists the BVT categories whose .test files should run.
	BVTCategories []types.TestCategory
	// Priority is the default priority for tasks generated from this mapping.
	Priority types.Priority
}

// DefaultMappings returns the built-in static mapping table for the
// matrixone repository. This maps source code path prefixes to the test
// categories and UT packages they are likely to affect.
//
// The mapping is intentionally broad – it is better to run a few extra
// tests than to miss a regression.
func DefaultMappings() []PathMapping {
	return []PathMapping{
		// ── SQL Plan / Optimizer ──
		{
			PathPrefix:    "pkg/sql/plan/",
			UTPackages:    []string{"pkg/sql/plan/..."},
			BVTCategories: []types.TestCategory{types.CategoryOptimizer, types.CategoryPlanCache, types.CategoryJoin, types.CategorySubquery, types.CategoryCTE, types.CategoryRecursiveCTE, types.CategoryHint},
			Priority:      types.PriorityHigh,
		},
		// ── SQL Compile ──
		{
			PathPrefix:    "pkg/sql/compile/",
			UTPackages:    []string{"pkg/sql/compile/..."},
			BVTCategories: []types.TestCategory{types.CategoryDDL, types.CategoryDML, types.CategoryFunction, types.CategoryExpression},
			Priority:      types.PriorityHigh,
		},
		// ── Column Executors ──
		{
			PathPrefix:    "pkg/sql/colexec/",
			UTPackages:    []string{"pkg/sql/colexec/..."},
			BVTCategories: []types.TestCategory{types.CategoryFunction, types.CategoryExpression, types.CategoryJoin, types.CategoryWindow},
			Priority:      types.PriorityHigh,
		},
		// ── SQL Parsers ──
		{
			PathPrefix:    "pkg/sql/parsers/",
			UTPackages:    []string{"pkg/sql/parsers/..."},
			BVTCategories: []types.TestCategory{types.CategoryDDL, types.CategoryDML, types.CategoryPrepare},
			Priority:      types.PriorityHigh,
		},
		// ── Distributed TAE Engine ──
		{
			PathPrefix:    "pkg/vm/engine/disttae/",
			UTPackages:    []string{"pkg/vm/engine/disttae/..."},
			BVTCategories: []types.TestCategory{types.CategoryDisttae, types.CategoryPessimisticTransaction, types.CategoryOptimistic, types.CategorySnapshot, types.CategoryPITR},
			Priority:      types.PriorityCritical,
		},
		// ── TAE Engine ──
		{
			PathPrefix:    "pkg/vm/engine/tae/",
			UTPackages:    []string{"pkg/vm/engine/tae/..."},
			BVTCategories: []types.TestCategory{types.CategoryDisttae, types.CategoryPessimisticTransaction},
			Priority:      types.PriorityCritical,
		},
		// ── Object IO ──
		{
			PathPrefix:    "pkg/objectio/",
			UTPackages:    []string{"pkg/objectio/..."},
			BVTCategories: []types.TestCategory{types.CategoryLoadData, types.CategoryDisttae},
			Priority:      types.PriorityHigh,
		},
		// ── Frontend (session, auth, protocol) ──
		{
			PathPrefix:    "pkg/frontend/",
			UTPackages:    []string{"pkg/frontend/..."},
			BVTCategories: []types.TestCategory{types.CategorySecurity, types.CategoryTenant, types.CategoryAccessControl, types.CategorySnapshot, types.CategoryPITR, types.CategorySystemVariable, types.CategorySet},
			Priority:      types.PriorityHigh,
		},
		// ── Container types (vector, batch, bytejson) ──
		{
			PathPrefix:    "pkg/container/",
			UTPackages:    []string{"pkg/container/..."},
			BVTCategories: []types.TestCategory{types.CategoryDtype, types.CategoryArray, types.CategoryVector},
			Priority:      types.PriorityMedium,
		},
		// ── Fulltext ──
		{
			PathPrefix:    "pkg/fulltext/",
			UTPackages:    []string{"pkg/fulltext/...", "pkg/sql/colexec/table_function/..."},
			BVTCategories: []types.TestCategory{types.CategoryFulltext},
			Priority:      types.PriorityMedium,
		},
		// ── CDC ──
		{
			PathPrefix:    "pkg/cdc/",
			UTPackages:    []string{"pkg/cdc/..."},
			BVTCategories: []types.TestCategory{types.CategoryCDC},
			Priority:      types.PriorityMedium,
		},
		// ── UDF ──
		{
			PathPrefix:    "pkg/udf/",
			UTPackages:    []string{"pkg/udf/..."},
			BVTCategories: []types.TestCategory{types.CategoryUDF},
			Priority:      types.PriorityMedium,
		},
		// ── Lock service ──
		{
			PathPrefix:    "pkg/lockservice/",
			UTPackages:    []string{"pkg/lockservice/..."},
			BVTCategories: []types.TestCategory{types.CategoryPessimisticTransaction},
			Priority:      types.PriorityHigh,
		},
		// ── Transaction ──
		{
			PathPrefix:    "pkg/txn/",
			UTPackages:    []string{"pkg/txn/..."},
			BVTCategories: []types.TestCategory{types.CategoryPessimisticTransaction, types.CategoryOptimistic},
			Priority:      types.PriorityCritical,
		},
		// ── Catalog ──
		{
			PathPrefix:    "pkg/catalog/",
			UTPackages:    []string{"pkg/catalog/..."},
			BVTCategories: []types.TestCategory{types.CategoryDDL, types.CategoryDatabase, types.CategoryTable, types.CategorySystem},
			Priority:      types.PriorityHigh,
		},
		// ── File service ──
		{
			PathPrefix:    "pkg/fileservice/",
			UTPackages:    []string{"pkg/fileservice/..."},
			BVTCategories: []types.TestCategory{types.CategoryStage, types.CategoryLoadData},
			Priority:      types.PriorityMedium,
		},
		// ── Partition ──
		{
			PathPrefix:    "pkg/partition/",
			UTPackages:    []string{"pkg/partition/...", "pkg/partitionservice/...", "pkg/partitionprune/..."},
			BVTCategories: []types.TestCategory{types.CategoryDDL, types.CategoryDML},
			Priority:      types.PriorityMedium,
		},
		// ── Proxy ──
		{
			PathPrefix:    "pkg/proxy/",
			UTPackages:    []string{"pkg/proxy/..."},
			BVTCategories: []types.TestCategory{types.CategoryTenant},
			Priority:      types.PriorityLow,
		},
		// ── Bootstrap ──
		{
			PathPrefix:    "pkg/bootstrap/",
			UTPackages:    []string{"pkg/bootstrap/..."},
			BVTCategories: []types.TestCategory{types.CategorySystem},
			Priority:      types.PriorityMedium,
		},
		// ── Vector index ──
		{
			PathPrefix:    "pkg/vectorindex/",
			UTPackages:    []string{"pkg/vectorindex/..."},
			BVTCategories: []types.TestCategory{types.CategoryVector},
			Priority:      types.PriorityMedium,
		},
		// ── NLP/LLM ──
		{
			PathPrefix:    "pkg/monlp/",
			UTPackages:    []string{"pkg/monlp/..."},
			BVTCategories: []types.TestCategory{types.CategoryFulltext},
			Priority:      types.PriorityMedium,
		},
		// ── Stage ──
		{
			PathPrefix:    "pkg/stage/",
			UTPackages:    []string{"pkg/stage/..."},
			BVTCategories: []types.TestCategory{types.CategoryStage},
			Priority:      types.PriorityMedium,
		},
		// ── BVT test cases themselves ──
		{
			PathPrefix:    "test/distributed/",
			UTPackages:    nil,
			BVTCategories: nil, // handled specially by MatchBVTTestFile
			Priority:      types.PriorityHigh,
		},
	}
}

// MatchResult holds the test categories and UT packages that a single file
// change maps to.
type MatchResult struct {
	UTPackages    []string
	BVTCategories []types.TestCategory
	Priority      types.Priority
}

// Matcher uses PathMappings to resolve which tests a set of file changes
// should trigger.
type Matcher struct {
	mappings []PathMapping
}

// NewMatcher creates a Matcher with the given mappings.
func NewMatcher(mappings []PathMapping) *Matcher {
	return &Matcher{mappings: mappings}
}

// NewDefaultMatcher creates a Matcher using DefaultMappings.
func NewDefaultMatcher() *Matcher {
	return NewMatcher(DefaultMappings())
}

// Match returns the aggregated MatchResult for a single file path.
func (m *Matcher) Match(filePath string) *MatchResult {
	var result MatchResult
	result.Priority = types.PriorityLow

	for _, mapping := range m.mappings {
		if strings.HasPrefix(filePath, mapping.PathPrefix) {
			result.UTPackages = appendUnique(result.UTPackages, mapping.UTPackages)
			result.BVTCategories = appendUniqueCategories(result.BVTCategories, mapping.BVTCategories)
			if mapping.Priority < result.Priority {
				result.Priority = mapping.Priority
			}
		}
	}

	// Special handling for BVT test file changes: infer category from path.
	if cat := inferBVTCategory(filePath); cat != "" {
		result.BVTCategories = appendUniqueCategories(result.BVTCategories, []types.TestCategory{cat})
	}

	return &result
}

// MatchAll aggregates match results across multiple file changes.
func (m *Matcher) MatchAll(files []types.FileChange) *MatchResult {
	var agg MatchResult
	agg.Priority = types.PriorityLow

	for _, f := range files {
		r := m.Match(f.Path)
		agg.UTPackages = appendUnique(agg.UTPackages, r.UTPackages)
		agg.BVTCategories = appendUniqueCategories(agg.BVTCategories, r.BVTCategories)
		if r.Priority < agg.Priority {
			agg.Priority = r.Priority
		}
	}
	return &agg
}

// inferBVTCategory attempts to extract a BVT category from a file path
// under test/distributed/cases/<category>/...
func inferBVTCategory(path string) types.TestCategory {
	const prefix = "test/distributed/cases/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := path[len(prefix):]
	idx := strings.Index(rest, "/")
	if idx <= 0 {
		return ""
	}
	return types.TestCategory(rest[:idx])
}

func appendUnique(dst, src []string) []string {
	seen := make(map[string]struct{}, len(dst))
	for _, s := range dst {
		seen[s] = struct{}{}
	}
	for _, s := range src {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			dst = append(dst, s)
		}
	}
	return dst
}

func appendUniqueCategories(dst, src []types.TestCategory) []types.TestCategory {
	seen := make(map[types.TestCategory]struct{}, len(dst))
	for _, c := range dst {
		seen[c] = struct{}{}
	}
	for _, c := range src {
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			dst = append(dst, c)
		}
	}
	return dst
}
