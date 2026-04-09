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

package analyzer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/testinfra/dedup"
	"github.com/matrixorigin/matrixone/pkg/testinfra/llm"
	"github.com/matrixorigin/matrixone/pkg/testinfra/scanner"
	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

const maxDiffLines = 300

// Config holds analyzer configuration.
type Config struct {
	Repo     string
	RepoRoot string
	APIURL   string
	APIKey   string
	Model    string
}

// Analyzer orchestrates PR analysis: diff → skills → cases → LLM → report.
type Analyzer struct {
	config Config
	client *llm.Client
}

// New creates an Analyzer.
func New(cfg Config) *Analyzer {
	return &Analyzer{
		config: cfg,
		client: llm.NewClient(cfg.APIURL, cfg.APIKey, cfg.Model),
	}
}

// Analyze fetches the PR diff, loads skill docs and existing cases,
// calls the LLM, and returns a structured coverage report.
func (a *Analyzer) Analyze(ctx context.Context, prNumber int) (*types.CoverageReport, error) {
	diff, err := a.getDiff(prNumber)
	if err != nil {
		return nil, err
	}

	// Extract changed file paths from diff for category inference
	changedPaths := extractChangedPaths(diff)

	skills, err := a.loadSkills()
	if err != nil {
		return nil, err
	}

	cases, err := scanner.ScanBVTCases(a.config.RepoRoot)
	if err != nil {
		return nil, err
	}
	caseSummary := scanner.FormatCaseSummary(cases)

	// Read actual case examples from relevant categories
	sampleCases := scanner.ReadSampleCases(a.config.RepoRoot, changedPaths, 3, 40)

	systemMsg := buildSystemPrompt(skills)
	userMsg := buildUserPrompt(prNumber, diff, caseSummary, sampleCases)

	response, err := a.client.Chat(ctx, []llm.Message{
		{Role: "system", Content: systemMsg},
		{Role: "user", Content: userMsg},
	})
	if err != nil {
		return nil, err
	}

	report, err := parseReport(response, prNumber)
	if err != nil {
		return nil, err
	}

	// Deduplicate suggested cases against existing test files
	d := dedup.New(a.config.RepoRoot)
	kept, skipped := d.Filter(report.SuggestedCases)
	report.SuggestedCases = kept
	if len(skipped) > 0 {
		report.Summary += fmt.Sprintf("\n\n[Dedup] Filtered %d case(s): %s", len(skipped), strings.Join(skipped, "; "))
	}

	return report, nil
}

// extractChangedPaths parses diff headers to get file paths.
func extractChangedPaths(diff string) []string {
	var paths []string
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "+++ b/") {
			paths = append(paths, line[6:])
		}
	}
	return paths
}

func (a *Analyzer) getDiff(prNumber int) (string, error) {
	cmd := exec.Command("gh", "pr", "diff", strconv.Itoa(prNumber), "--repo", a.config.Repo)
	out, err := cmd.Output()
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("gh pr diff failed for #%d: %v", prNumber, err)
	}

	diff := string(out)
	lines := strings.Split(diff, "\n")
	if len(lines) > maxDiffLines {
		diff = strings.Join(lines[:maxDiffLines], "\n")
		diff += fmt.Sprintf("\n\n... (truncated, showing first %d of %d lines)", maxDiffLines, len(lines))
	}
	return diff, nil
}

// prioritySkills are the most important skill docs to include in the prompt.
// These are loaded first; others are added only if token budget allows.
var prioritySkills = []string{
	"module-test-mapping.md",
	"testing-guide.md",
}

func (a *Analyzer) loadSkills() (string, error) {
	skillsDir := filepath.Join(a.config.RepoRoot, "docs", "ai-skills")

	var b strings.Builder
	// Load priority docs first
	for _, name := range prioritySkills {
		data, err := os.ReadFile(filepath.Join(skillsDir, name))
		if err != nil {
			continue
		}
		b.WriteString("## ")
		b.WriteString(name)
		b.WriteString("\n")
		b.Write(data)
		b.WriteString("\n\n")
	}

	// Load remaining docs if total is under 6KB
	entries, err := os.ReadDir(skillsDir)
	if err != nil {
		return b.String(), nil
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		// Skip already loaded
		skip := false
		for _, p := range prioritySkills {
			if entry.Name() == p {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		if b.Len() > 6000 {
			break
		}
		data, err := os.ReadFile(filepath.Join(skillsDir, entry.Name()))
		if err != nil {
			continue
		}
		b.WriteString("## ")
		b.WriteString(entry.Name())
		b.WriteString("\n")
		b.Write(data)
		b.WriteString("\n\n")
	}
	return b.String(), nil
}

func buildSystemPrompt(skills string) string {
	return `你是 MatrixOne 数据库的测试覆盖分析 Agent。分析 PR 代码变更，判断 6 类测试的覆盖情况，并**生成完整可运行的测试用例**。

## 6 类测试

### 1. BVT（轻量级回归，matrixone 仓库）
- 路径: test/distributed/cases/{category}/{name}.sql
- 格式: SQL + mo-tester 标签
- 标签: @bvt:issue#N（跳过区块）、@session:id=N（并发会话）、@wait:N:commit（等待提交）、@sortkey:cols（排序）、@ignore:cols（忽略列）、@regex("pattern",true/false)
- 结尾必须 drop 创建的表/数据库
- 每个文件必须自包含、可独立运行
- 不确定顺序用 @sortkey，不确定值（如时间）用 @ignore

### 2. 稳定性测试（mo-nightly-regression 仓库，main 分支）
- 路径: cases/sysbench/{scenario}/run.yml
- 格式: YAML (mo-load 配置)
- 内容: duration, transaction[].name/vuser/mode/script[].sql
- 场景: TPCH、TPCC、Sysbench(insert/select/update/delete/mixed)、Fulltext-vector

### 3. Chaos 测试（mo-nightly-regression 仓库，main 分支）
- 故障定义: mo-chaos-config/chaos_regression.yaml (Chaos Mesh YAML)
- 工作负载: mo-chaos-config/chaos_test_case.yaml (tasks[].run-steps/verify)
- 故障类型: PodChaos/pod-kill(杀CN/TN/LogService)、StressChaos/memory、NetworkChaos
- 工作负载: mo-tpcc、mo-load(sysbench)、mo-cdc-test、mo-vector-fulltext-test

### 4. 大数据量测试（mo-nightly-regression 仓库，main 分支）
- 路径: tools/mo-regression-test/cases/{test_name}/
- 格式: mo_ddl.yaml + mo_load_*.yaml + q00.sql
- 数据来源: COS 对象存储 load

### 5. PITR 测试（matrixone 仓库 BVT 级别）
- 路径: test/distributed/cases/pitr/
- 格式: .sql + mo-tester 标签
- 流程: CREATE PITR → 操作数据 → RESTORE → 验证一致性

### 6. Snapshot 测试（matrixone 仓库 BVT 级别）
- 路径: test/distributed/cases/snapshot/
- 格式: .sql + mo-tester 标签
- 场景: cluster/account/database/table 级别 snapshot 创建和恢复

## MO 知识文档
` + skills
}

func buildUserPrompt(prNumber int, diff, caseSummary, sampleCases string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("分析 PR #%d 的测试覆盖情况。\n\n", prNumber))
	b.WriteString("**关键原则**: 只为真正缺少测试覆盖的类型生成 case。如果某类测试已有覆盖或与本次变更完全无关，status 设为 covered 或 not_related，不要生成 case。不要盲目为每种测试类型都生成 case。\n\n")

	b.WriteString("## PR Diff:\n")
	b.WriteString(diff)
	b.WriteString("\n\n")

	b.WriteString("## 已有 BVT 测试目录（category/文件数）:\n")
	b.WriteString(caseSummary)
	b.WriteString("\n\n")

	if sampleCases != "" {
		b.WriteString("## 目标目录已有 case 示例（学习格式，避免与这些已有 case 重复）:\n")
		b.WriteString(sampleCases)
		b.WriteString("\n")
	}

	b.WriteString(`## 输出要求

请严格输出以下 JSON 格式（不要添加 markdown 代码块或其他文字）:
{
  "summary": "一句话描述改了什么",
  "affected_modules": ["受影响的模块路径"],
  "coverage": [
    {"type": "bvt", "status": "covered|needs_attention|not_related", "description": "说明"},
    {"type": "stability", "status": "covered|needs_attention|not_related", "description": "说明"},
    {"type": "chaos", "status": "covered|needs_attention|not_related", "description": "说明"},
    {"type": "bigdata", "status": "covered|needs_attention|not_related", "description": "说明"},
    {"type": "pitr", "status": "covered|needs_attention|not_related", "description": "说明"},
    {"type": "snapshot", "status": "covered|needs_attention|not_related", "description": "说明"}
  ],
  "suggested_cases": [
    {
      "type": "bvt|stability|chaos|bigdata|pitr|snapshot",
      "category": "BVT子目录名 或 nightly场景名",
      "filename": "文件名",
      "content": "完整的可运行内容",
      "reason": "补充原因"
    }
  ]
}

## 生成 case 规则

### BVT case 生成规则:
1. 必须是完整可运行的 .sql 文件
2. 参考上面的已有 case 示例的格式和风格
3. 结尾必须 drop 创建的表/数据库
4. 不确定的结果列用 @ignore，不确定的行序用 @sortkey
5. category 填对应的 BVT 子目录名（如 function, ddl, window, expression 等）

### 稳定性 case 生成规则:
1. 格式为 mo-load run.yml
2. 包含 duration、transaction 配置
3. category 填 "sysbench/{场景名}"

### Chaos case 生成规则:
1. 如果需要新增故障类型，生成 Chaos Mesh YAML
2. 如果需要新增工作负载，生成 chaos_test_case.yaml 格式的 task
3. category 填 "mo-chaos-config"

### PITR/Snapshot case 生成规则:
1. 与 BVT 同格式（.sql + mo-tester 标签）
2. PITR: CREATE PITR → DML → RESTORE → SELECT 验证
3. Snapshot: CREATE SNAPSHOT → DML → RESTORE ACCOUNT → SELECT 验证
4. category 分别填 "pitr" 或 "snapshot"

注意：
- coverage 必须包含全部 6 种类型
- suggested_cases 只列 status 为 needs_attention 的测试类型，绝不要为 not_related 或 covered 的类型生成 case
- 如果本次变更只涉及 BVT 相关代码（如 SQL 函数、DDL 等），就只生成 BVT case，不要强行凑其他类型
- 生成的 SQL 不要与已有示例中的内容重复，关注新增/修改代码路径的覆盖
- BVT/PITR/Snapshot case 的 content 必须是完整可运行的 SQL
- 稳定性/Chaos case 的 content 必须是完整的 YAML
`)
	return b.String()
}

func parseReport(response string, prNumber int) (*types.CoverageReport, error) {
	jsonStr := extractJSON(response)

	var report types.CoverageReport
	if err := json.Unmarshal([]byte(jsonStr), &report); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("parse LLM JSON: %v\nRaw:\n%s", err, response)
	}
	report.PRNumber = prNumber
	return &report, nil
}

func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	if idx := strings.Index(s, "```json"); idx >= 0 {
		s = s[idx+7:]
		if end := strings.Index(s, "```"); end >= 0 {
			s = s[:end]
		}
	} else if idx := strings.Index(s, "```"); idx >= 0 {
		s = s[idx+3:]
		if nl := strings.Index(s, "\n"); nl >= 0 {
			s = s[nl:]
		}
		if end := strings.Index(s, "```"); end >= 0 {
			s = s[:end]
		}
	}
	// Fix LLM producing string concatenation instead of proper JSON strings.
	// e.g. "line1\n" +\n                "line2\n" → "line1\nline2\n"
	s = sanitizeJSONConcat(s)
	return strings.TrimSpace(s)
}

// sanitizeJSONConcat merges broken "..." + "..." patterns that some LLMs produce.
func sanitizeJSONConcat(s string) string {
	for {
		// Match: " +\n{whitespace}" or " +\n{whitespace}'
		idx := strings.Index(s, "\" +")
		if idx < 0 {
			break
		}
		// Find the next quote after " +"
		rest := s[idx+3:]
		rest = strings.TrimLeft(rest, " \t\n\r")
		if len(rest) == 0 || rest[0] != '"' {
			break
		}
		// Merge: replace the closing quote + concat + opening quote with nothing
		s = s[:idx] + rest[1:]
	}
	return s
}

// FormatReport produces a terminal-friendly coverage report.
func FormatReport(r *types.CoverageReport) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("## PR #%d 测试覆盖分析\n\n", r.PRNumber))
	b.WriteString("### 变更摘要\n")
	b.WriteString(r.Summary)
	b.WriteString("\n\n")

	if len(r.AffectedModules) > 0 {
		b.WriteString("### 受影响模块\n")
		for _, m := range r.AffectedModules {
			b.WriteString("- ")
			b.WriteString(m)
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}

	b.WriteString("### 6 类测试覆盖情况\n\n")
	b.WriteString("| 测试类型 | 状态 | 说明 |\n")
	b.WriteString("|---------|------|------|\n")
	typeNames := map[types.TestType]string{
		types.TestBVT: "BVT", types.TestStability: "稳定性",
		types.TestChaos: "Chaos", types.TestBigData: "大数据",
		types.TestPITR: "PITR", types.TestSnapshot: "Snapshot",
	}
	for _, c := range r.Coverage {
		icon := "➖"
		switch c.Status {
		case types.StatusCovered:
			icon = "✅"
		case types.StatusNeedsAttention:
			icon = "⚠️"
		}
		name := typeNames[c.Type]
		if name == "" {
			name = string(c.Type)
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n", name, icon, c.Description))
	}
	b.WriteString("\n")

	if len(r.SuggestedCases) > 0 {
		b.WriteString(fmt.Sprintf("### 建议补充的 Case（%d 个）\n\n", len(r.SuggestedCases)))
		for i, sc := range r.SuggestedCases {
			path := "test/distributed/cases/" + sc.Category + "/" + sc.Filename
			b.WriteString(fmt.Sprintf("#### %d. %s\n", i+1, path))
			b.WriteString("原因: ")
			b.WriteString(sc.Reason)
			b.WriteString("\n\n```sql\n")
			b.WriteString(sc.Content)
			b.WriteString("\n```\n\n")
		}
	}

	return b.String()
}
