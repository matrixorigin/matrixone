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

// mo-testplan is a CLI tool that generates a structured TestPlan from a
// git diff. It is designed to be used in CI pipelines (e.g., GitHub Actions)
// to automatically determine which tests to run for a given PR.
//
// Usage:
//
//	# From git diff on stdin:
//	git diff origin/main...HEAD | mo-testplan --pr 12345 --base main --head feature/x
//
//	# From a diff file:
//	mo-testplan --pr 12345 --base main --head feature/x --diff changes.patch
//
//	# Output format:
//	mo-testplan --pr 12345 --base main --head feature/x --format json
//	mo-testplan --pr 12345 --base main --head feature/x --format summary
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/testinfra/planner"
)

func main() {
	var (
		prNumber   int
		baseBranch string
		headBranch string
		diffFile   string
		format     string
	)

	flag.IntVar(&prNumber, "pr", 0, "PR number")
	flag.StringVar(&baseBranch, "base", "main", "base branch name")
	flag.StringVar(&headBranch, "head", "", "head branch name")
	flag.StringVar(&diffFile, "diff", "", "path to diff file (reads stdin if empty)")
	flag.StringVar(&format, "format", "json", "output format: json or summary")
	flag.Parse()

	// Read diff
	var diffBytes []byte
	var err error
	if diffFile != "" {
		diffBytes, err = os.ReadFile(diffFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading diff file: %v\n", err)
			os.Exit(1)
		}
	} else {
		diffBytes, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
			os.Exit(1)
		}
	}

	if len(diffBytes) == 0 {
		fmt.Fprintf(os.Stderr, "No diff provided. Pipe a git diff or use --diff flag.\n")
		os.Exit(1)
	}

	// Generate plan
	p := planner.NewPlanner()
	plan := p.GeneratePlanFromDiff(string(diffBytes), prNumber, baseBranch, headBranch)

	// Output
	switch format {
	case "json":
		data, err := plan.ToJSON()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error serializing plan: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(data))
	case "summary":
		fmt.Println(plan.Summary)
		fmt.Printf("\nTasks (%d):\n", len(plan.Tasks))
		for _, task := range plan.Tasks {
			target := string(task.Package)
			if target == "" {
				target = string(task.Category)
			}
			fmt.Printf("  [%s] %s %-8s %s (%s)\n",
				task.Priority.String(), task.ID, task.Type, target, task.Reason)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown format: %s (use json or summary)\n", format)
		os.Exit(1)
	}
}
