// Copyright 2026 Matrix Origin
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

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/system"
)

// Child protocol: each case re-execs this test binary as a child that calls
// the real production main() entry with a test-controlled argv, an explicit
// inherited GOMAXPROCS, and a discriminated expected startup-failure class.
// The child prints the witness line only when main() ends in the expected
// startup-failure class; any other panic, or an unexpected plain return,
// fails the child with a nonzero exit, so the parent can never mistake an
// unrelated failure for an applied flag.
const (
	maxProcessorChildArgsEnv  = "MO_MAX_PROCESSOR_CHILD_ARGS"
	maxProcessorChildClassEnv = "MO_MAX_PROCESSOR_CHILD_CLASS"
	maxProcessorClassNoCfg    = "no-configuration"
	maxProcessorClassCfgParse = "cfg-parse-error"
	maxProcessorChildTestRun  = "^TestMainAppliesMaxProcessorFlag$"
	maxProcessorWitnessLine   = "max-processor-witness runtime=%d system=%d avail=%d"
	maxProcessorWitnessPrefix = "max-processor-witness runtime="
	maxProcessorChildTimeout  = 2 * time.Minute
)

// TestMainAppliesMaxProcessorFlag proves the production main() applies an
// explicit positive -max-processor, whatever the real initialization left
// behind: the child runs the real main() and witnesses both the runtime
// scheduler limit and the system.GoMaxProcs owner that compile.NewCompile
// snapshots into its worker budget. Zero, omitted, and negative values
// must keep the pair the real initialization produced.
//
// The initialized baseline is never assumed: a control child with the same
// environment and no -max-processor observes it first, and the zero and
// negative expectations compare against that observed pair. A
// single-available-CPU affinity/cpuset policy that caps initialization
// below the inherited GOMAXPROCS is therefore respected instead of
// contradicting a hardcoded default. A QueryService-style read cannot
// witness the baseline: its handler calls SetGoMaxProcs even for a zero
// request and resynchronizes the cached owner, and this offline test runs
// no query service at all.
func TestMainAppliesMaxProcessorFlag(t *testing.T) {
	if rawArgs, ok := os.LookupEnv(maxProcessorChildArgsEnv); ok {
		runMaxProcessorChildMain(rawArgs)
	}

	// Request a value above any initialized possibility: no runtime,
	// affinity, or cgroup initialization can produce it, so only the
	// correctly parsed CLI flag can.
	higherThanAnyInit := runtime.NumCPU() + 3

	// The control child runs first and is an independent process, so the
	// zero/negative comparisons always reference the initialized pair of
	// this environment before any flag-carrying child.
	baseR, baseS, _, _ := runMaxProcessorChildCase(t, maxProcessorChildConfig{
		gomaxprocs: "4",
	})

	for _, c := range []struct {
		name      string
		args      string
		keepPair  bool
		wantProcs int
	}{
		{name: "zero keeps initialized pair", args: "-max-processor=0", keepPair: true},
		{name: "negative keeps initialized pair", args: "-max-processor=-3", keepPair: true},
		{name: "lower positive overrides pair", args: "-max-processor=1", wantProcs: 1},
		{name: "higher positive overrides pair", args: "-max-processor=" + strconv.Itoa(higherThanAnyInit), wantProcs: higherThanAnyInit},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, s, avail, out := runMaxProcessorChildCase(t, maxProcessorChildConfig{
				gomaxprocs: "4",
				args:       c.args,
			})
			if c.keepPair {
				if r != baseR || s != baseS {
					t.Fatalf("flag %q must keep the initialized pair runtime=%d system=%d, got runtime=%d system=%d\nchild output:\n%s",
						c.args, baseR, baseS, r, s, out)
				}
				return
			}
			if r != c.wantProcs || s != c.wantProcs {
				t.Fatalf("flag %q must set runtime=%d system=%d, got runtime=%d system=%d avail=%d\nchild output:\n%s",
					c.args, c.wantProcs, c.wantProcs, r, s, avail, out)
			}
		})
	}

	t.Run("nearest version control exits before any witness", func(t *testing.T) {
		// -version exits through maybePrintVersion before the startup
		// failure, so the child helper must not be able to misread that
		// exit as an applied-flag witness.
		ctx, cancel := context.WithTimeout(context.Background(), maxProcessorChildTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+maxProcessorChildTestRun)
		cmd.Dir = t.TempDir()
		cmd.Env = maxProcessorChildEnv(maxProcessorChildConfig{
			gomaxprocs: "4",
			args:       "-max-processor=1 -version",
		})
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("version control child failed: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "MatrixOne build info:") {
			t.Fatalf("version control child must print the version banner, got:\n%s", out)
		}
		if strings.Contains(string(out), maxProcessorWitnessPrefix) {
			t.Fatalf("version control child must not report a max-processor witness, got:\n%s", out)
		}
	})

	t.Run("dynamic launcher argv applies its cpu count", func(t *testing.T) {
		// Close the dynamic-launcher causality: the generated child argv
		// (built from the DynamicConfig cpu count) is handed to a real
		// child main(), which must honor the requested count after
		// flag.Parse. The child keeps the generated -cfg value, so main
		// ends in the config parse failure class because the generated
		// file does not exist in the child's temporary working
		// directory. The generated -debug-http pair is excluded from the
		// child argv because it starts the hardcoded dynamic debug
		// listener (127.0.0.1:606x) inside main and starting real
		// listeners is outside this offline test; every remaining
		// generated flag value is used verbatim.
		command := dynamicCNServiceCommand(0, higherThanAnyInit)
		if len(command) != 7 ||
			command[1] != "-cfg" || command[2] != "./mo-data/cn-0.toml" ||
			command[3] != "-max-processor" || command[4] != strconv.Itoa(higherThanAnyInit) ||
			command[5] != "-debug-http" || command[6] != "127.0.0.1:6060" {
			t.Fatalf("unexpected dynamic CN child argv: %q", command)
		}
		// command[0] is the parent launcher binary; the child argv takes
		// the generated flags verbatim except the debug-http pair.
		r, s, _, out := runMaxProcessorChildCase(t, maxProcessorChildConfig{
			gomaxprocs: "4",
			args:       strings.Join(command[1:5:5], " "),
			class:      maxProcessorClassCfgParse,
		})
		if r != higherThanAnyInit || s != higherThanAnyInit {
			t.Fatalf("dynamic child argv must set runtime=%d system=%d, got runtime=%d system=%d\nchild output:\n%s",
				higherThanAnyInit, higherThanAnyInit, r, s, out)
		}
	})

	t.Run("single allowed CPU keeps cap and honors positive override", func(t *testing.T) {
		cpu, ok := pinnedChildCPU()
		if !ok {
			t.Skip("requires linux sched_getaffinity, taskset, and an allowed CPU in this cpuset")
		}
		// Control first: the real initialized pair under a one-CPU
		// affinity policy, where initialization may legitimately cap
		// below the inherited GOMAXPROCS.
		pR, pS, avail, out := runPinnedMaxProcessorChildCase(t, cpu, maxProcessorChildConfig{
			gomaxprocs: "2",
		})
		if avail != 1 {
			t.Fatalf("pinned child must observe exactly one available CPU, got %d\nchild output:\n%s", avail, out)
		}
		for _, args := range []string{"-max-processor=0", "-max-processor=-3"} {
			r, s, _, out := runPinnedMaxProcessorChildCase(t, cpu, maxProcessorChildConfig{
				gomaxprocs: "2",
				args:       args,
			})
			if r != pR || s != pS {
				t.Fatalf("flag %q must keep the one-CPU initialized pair runtime=%d system=%d, got runtime=%d system=%d\nchild output:\n%s",
					args, pR, pS, r, s, out)
			}
		}
		r, s, _, out := runPinnedMaxProcessorChildCase(t, cpu, maxProcessorChildConfig{
			gomaxprocs: "2",
			args:       "-max-processor=3",
		})
		if r != 3 || s != 3 {
			t.Fatalf("flag -max-processor=3 must set runtime=3 system=3 under one allowed CPU, got runtime=%d system=%d\nchild output:\n%s",
				r, s, out)
		}
	})
}

// maxProcessorChildConfig describes one child run. gomaxprocs is the
// explicit inherited scheduler limit, args the logical mo-service flags
// (space separated, values contain no spaces), class the intended
// startup-failure class, and a gomaxprocs/args combination matching the
// control body means "omitted flag".
type maxProcessorChildConfig struct {
	gomaxprocs string
	args       string
	class      string
}

func (c maxProcessorChildConfig) expectClass() string {
	if c.class != "" {
		return c.class
	}
	return maxProcessorClassNoCfg
}

// maxProcessorChildEnv drops ambient GOMAXPROCS and the marker variables
// and installs the case's explicit inherited GOMAXPROCS, so no global
// parallel test pollution can hide behind an ambient scheduler limit.
func maxProcessorChildEnv(cfg maxProcessorChildConfig) []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GOMAXPROCS=") ||
			strings.HasPrefix(kv, maxProcessorChildArgsEnv+"=") ||
			strings.HasPrefix(kv, maxProcessorChildClassEnv+"=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"GOMAXPROCS="+cfg.gomaxprocs,
		maxProcessorChildArgsEnv+"="+cfg.args,
		maxProcessorChildClassEnv+"="+cfg.expectClass(),
	)
}

func runMaxProcessorChildCase(t *testing.T, cfg maxProcessorChildConfig) (runtimeProcs, systemProcs, avail int, out string) {
	t.Helper()
	return runMaxProcessorChild(t, cfg, func(ctx context.Context, dir string) *exec.Cmd {
		return exec.CommandContext(ctx, os.Args[0], "-test.run="+maxProcessorChildTestRun)
	})
}

func runPinnedMaxProcessorChildCase(t *testing.T, cpu int, cfg maxProcessorChildConfig) (runtimeProcs, systemProcs, avail int, out string) {
	t.Helper()
	return runMaxProcessorChild(t, cfg, func(ctx context.Context, dir string) *exec.Cmd {
		return exec.CommandContext(ctx, "taskset", "-c", strconv.Itoa(cpu), os.Args[0], "-test.run="+maxProcessorChildTestRun)
	})
}

// runMaxProcessorChild spawns one child with a bounded original parent wait
// and requires the child to exit zero AND print the witness line; anything
// else is a hard failure with the full child output preserved.
func runMaxProcessorChild(t *testing.T, cfg maxProcessorChildConfig, mk func(context.Context, string) *exec.Cmd) (runtimeProcs, systemProcs, avail int, out string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), maxProcessorChildTimeout)
	defer cancel()
	dir := t.TempDir()
	cmd := mk(ctx, dir)
	// Every child runs in its own fresh temporary working directory so the
	// dynamic child's generated -cfg value can never resolve to an existing
	// file and no child inherits the launcher's working directory.
	cmd.Dir = dir
	cmd.Env = maxProcessorChildEnv(cfg)
	combined, err := cmd.CombinedOutput()
	out = string(combined)
	if err != nil {
		t.Fatalf("max-processor child failed: %v\n%s", err, out)
	}
	r, s, a, ok := parseMaxProcessorWitness(out)
	if !ok {
		t.Fatalf("no max-processor witness line in child output:\n%s", out)
	}
	return r, s, a, out
}

func parseMaxProcessorWitness(out string) (runtimeProcs, systemProcs, avail int, ok bool) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, maxProcessorWitnessPrefix) {
			continue
		}
		n, err := fmt.Sscanf(line, maxProcessorWitnessLine, &runtimeProcs, &systemProcs, &avail)
		if err == nil && n == 3 {
			return runtimeProcs, systemProcs, avail, true
		}
	}
	return 0, 0, 0, false
}

// runMaxProcessorChildMain runs in the child process: it installs the
// test's argv and calls the real production main(). It never returns.
func runMaxProcessorChildMain(rawArgs string) {
	os.Args = append([]string{"mo-service"}, strings.Fields(rawArgs)...)
	defer func() {
		r := recover()
		if r == nil {
			fmt.Fprintln(os.Stderr, "max-processor-helper-unexpected-main-return")
			os.Exit(3)
		}
		if !isExpectedMaxProcessorStartupFailure(r, os.Getenv(maxProcessorChildClassEnv)) {
			fmt.Fprintf(os.Stderr, "max-processor-helper-unexpected-failure: %v\n%s\n", r, debug.Stack())
			os.Exit(3)
		}
		fmt.Printf(maxProcessorWitnessLine+"\n", runtime.GOMAXPROCS(0), system.GoMaxProcs(), runtime.NumCPU())
		os.Exit(0)
	}()
	main()
}

// isExpectedMaxProcessorStartupFailure discriminates the intended startup
// failures from any unrelated panic: only the exact "no configuration
// specified" termination, or the config parse failure carrying the
// dynamic-generated -cfg value, may be witnessed as the applied-flag run.
func isExpectedMaxProcessorStartupFailure(r any, class string) bool {
	err, ok := r.(error)
	if !ok {
		return false
	}
	switch class {
	case maxProcessorClassNoCfg:
		return err.Error() == "no configuration specified"
	case maxProcessorClassCfgParse:
		return strings.HasPrefix(err.Error(), "failed to parse config from ./mo-data/cn-0.toml, error:")
	}
	return false
}
