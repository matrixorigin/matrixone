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

// mo-testplan analyzes PR test coverage against MO's 6 test types
// (BVT, stability, chaos, big data, PITR, snapshot) using AI.
//
// Usage:
//
//	mo-testplan --pr 24088
//	mo-testplan --pr 24088 --json
//	mo-testplan --pr 24088 --write
//	mo-testplan --pr 24088 --write --create-pr
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/testinfra/analyzer"
	"github.com/matrixorigin/matrixone/pkg/testinfra/writer"
)

const (
	defaultAPIURL = "https://models.github.ai/inference/chat/completions"
	defaultModel  = "openai/gpt-4o-mini"
)

func main() {
	var (
		prNumber int
		repo     string
		apiURL   string
		apiKey   string
		model    string
		jsonOut  bool
		write    bool
		createPR bool
	)

	flag.IntVar(&prNumber, "pr", 0, "PR number to analyze")
	flag.StringVar(&repo, "repo", "matrixorigin/matrixone", "GitHub repository (owner/name)")
	flag.StringVar(&apiURL, "api-url", defaultAPIURL, "LLM API endpoint")
	flag.StringVar(&apiKey, "api-key", "", "API key (default: $GITHUB_TOKEN or gh auth token)")
	flag.StringVar(&model, "model", defaultModel, "LLM model name")
	flag.BoolVar(&jsonOut, "json", false, "output raw JSON instead of formatted report")
	flag.BoolVar(&write, "write", false, "write suggested BVT cases to test/distributed/cases/")
	flag.BoolVar(&createPR, "create-pr", false, "create a PR with written cases (implies --write)")
	flag.Parse()

	if prNumber == 0 {
		fmt.Fprintf(os.Stderr, "Usage: mo-testplan --pr <number> [--repo owner/name] [--json]\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if apiKey == "" {
		apiKey = resolveAPIKey()
	}
	if apiKey == "" {
		fmt.Fprintf(os.Stderr, "Error: no API key found.\n")
		fmt.Fprintf(os.Stderr, "Set --api-key, $GITHUB_TOKEN, or login with: gh auth login\n")
		os.Exit(1)
	}

	repoRoot := detectRepoRoot()
	if repoRoot == "" {
		fmt.Fprintf(os.Stderr, "Error: cannot detect repo root (no go.mod found).\n")
		os.Exit(1)
	}

	cfg := analyzer.Config{
		Repo:     repo,
		RepoRoot: repoRoot,
		APIURL:   apiURL,
		APIKey:   apiKey,
		Model:    model,
	}

	a := analyzer.New(cfg)
	report, err := a.Analyze(context.Background(), prNumber)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if jsonOut {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Stdout.Write(data)
		os.Stdout.WriteString("\n")
	} else {
		os.Stdout.WriteString(analyzer.FormatReport(report))
	}

	// --create-pr implies --write
	if createPR {
		write = true
	}

	if write && len(report.SuggestedCases) > 0 {
		written, err := writer.WriteCases(repoRoot, report.SuggestedCases)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing cases: %v\n", err)
			os.Exit(1)
		}
		if len(written) > 0 {
			os.Stdout.WriteString("\n### 已写入文件\n")
			for _, f := range written {
				os.Stdout.WriteString("- ")
				os.Stdout.WriteString(f)
				os.Stdout.WriteString("\n")
			}

			if createPR {
				prURL, err := writer.CreatePR(repoRoot, repo, prNumber, written)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error creating PR: %v\n", err)
					os.Exit(1)
				}
				os.Stdout.WriteString("\n### 已创建 PR\n")
				os.Stdout.WriteString(strings.TrimSpace(prURL))
				os.Stdout.WriteString("\n")
			}
		} else {
			os.Stdout.WriteString("\n无新增文件（已有 case 或建议为空）\n")
		}
	}
}

func resolveAPIKey() string {
	if key := os.Getenv("GITHUB_TOKEN"); key != "" {
		return key
	}
	out, err := exec.Command("gh", "auth", "token").Output()
	if err == nil {
		return strings.TrimSpace(string(out))
	}
	return ""
}

func detectRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
