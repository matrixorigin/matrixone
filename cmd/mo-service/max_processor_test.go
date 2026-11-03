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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/system"
)

const maxProcessorHelperEnv = "MO_TEST_MAX_PROCESSOR_HELPER"

// TestMainAppliesMaxProcessorFlag runs the real main() in a child process
// with a conflicting inherited GOMAXPROCS and an explicit -max-processor,
// then reports the runtime and planner worker limits observed after main
// returns through its "no configuration specified" startup failure.
func TestMainAppliesMaxProcessorFlag(t *testing.T) {
	if v := os.Getenv(maxProcessorHelperEnv); v != "" {
		runMainWithMaxProcessor(v)
		return
	}

	inherited := 2
	cases := []struct {
		name string
		arg  int
		want int
	}{
		{name: "lower than inherited", arg: 1, want: 1},
		{name: "higher than inherited", arg: runtime.NumCPU() + 3, want: runtime.NumCPU() + 3},
		{name: "zero keeps inherited", arg: 0, want: inherited},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMainAppliesMaxProcessorFlag$")
			cmd.Env = append(os.Environ(),
				maxProcessorHelperEnv+"="+strconv.Itoa(c.arg),
				"GOMAXPROCS="+strconv.Itoa(inherited),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("helper failed: %v\n%s", err, out)
			}
			want := fmt.Sprintf("max-processor-result runtime=%d system=%d", c.want, c.want)
			if !strings.Contains(string(out), want) {
				t.Fatalf("want %q in helper output, got:\n%s", want, out)
			}
		})
	}
}

func runMainWithMaxProcessor(arg string) {
	defer func() {
		// main panics because no -cfg/-launch is given; the flag has been
		// handled by then.
		_ = recover()
		fmt.Printf("max-processor-result runtime=%d system=%d\n",
			runtime.GOMAXPROCS(0), system.GoMaxProcs())
		os.Exit(0)
	}()
	os.Args = []string{os.Args[0], "-max-processor", arg}
	main()
}
