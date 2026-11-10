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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := cli(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cli(ctx context.Context, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: tpch-compare prepare|worker|compare [options]")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	f.SetOutput(errOut)
	switch args[0] {
	case "profile":
		config := f.String("config", "", "campaign configuration JSON")
		id := f.String("id", "profiles", "new profiling identifier")
		baseline := f.String("baseline", "", "measured baseline run directory")
		pilot := f.Bool("pilot", false, "short SF1 profiling qualification")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		bounded, cancel := context.WithTimeoutCause(ctx, 24*time.Hour, errors.New("profiling campaign budget"))
		defer cancel()
		return profileCampaign(bounded, *config, *id, *baseline, *pilot)
	case "preflight":
		config := f.String("config", "", "campaign configuration JSON")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *config == "" {
			return errors.New("config is required")
		}
		p, err := preflight(ctx, *config)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "preflight passed; %d bytes available\n", p.FreeBytes)
		return err
	case "run":
		config := f.String("config", "", "campaign configuration JSON")
		phase := f.String("phase", "measure", "prepare or measure")
		id := f.String("id", "", "new run identifier")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *config == "" {
			return errors.New("config is required")
		}
		bounded, cancel := context.WithTimeoutCause(ctx, 24*time.Hour, errors.New("campaign wall-clock budget exhausted"))
		defer cancel()
		return campaignRun(bounded, *config, *phase, *id)
	case "report":
		config := f.String("config", "", "campaign configuration JSON")
		root := f.String("run-dir", "", "completed or incomplete run directory")
		output := f.String("output", "", "new report directory")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		return reportCampaign(*config, *root, *output)
	case "prepare":
		root := f.String("work-dir", "", "new dataset directory on local storage")
		tools := f.String("tools-dir", "", "directory containing pinned dbgen, qgen, queries and answers")
		scale := f.String("scale", "1", "TPC-H scale factor")
		revision := f.String("generator-revision", "", "generator source commit")
		version := f.String("generator-version", "", "actual DBGEN/QGEN version, separate from specification 3.0.1")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *root == "" || *tools == "" {
			return errors.New("work-dir and tools-dir are required")
		}
		limit, cancel := context.WithTimeoutCause(ctx, 4*time.Hour, errors.New("dataset preparation deadline"))
		defer cancel()
		d, err := prepare(limit, PrepareOptions{Root: *root, Tools: *tools, Scale: *scale, Revision: *revision, GeneratorVersion: *version, Generate: true})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "prepared SF%s: %d tables, %d qualification queries\n", d.Scale, len(d.Tables), len(d.Queries))
		return err
	case "worker":
		request := f.String("request", "", "worker request JSON")
		response := f.String("response", "", "new receipt JSON")
		controlled := f.Bool("controlled", false, "use inherited controller gate/event descriptors")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *request == "" || *response == "" {
			return errors.New("request and response are required")
		}
		r, err := readJSON[WorkerRequest](*request)
		if err != nil {
			return err
		}
		var control *workerControl
		if *controlled {
			control = &workerControl{gate: os.NewFile(3, "gate"), events: os.NewFile(4, "events")}
			defer control.close()
		}
		receipt, runErr := runWorker(ctx, r, control)
		if err := publishJSON(*response, receipt); err != nil {
			return errors.Join(runErr, err)
		}
		return runErr
	case "compare":
		dataset := f.String("dataset", "", "prepared dataset directory")
		left := f.String("left", "", "left worker receipt")
		right := f.String("right", "", "right worker receipt")
		leftUnlimited := f.String("left-unlimited", "", "left unlimited diagnostic receipt for LIMIT membership")
		rightUnlimited := f.String("right-unlimited", "", "right unlimited diagnostic receipt for LIMIT membership")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		count, err := compareReceipts(*dataset, *left, *right, *leftUnlimited, *rightUnlimited)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "full typed comparison passed: %d query executions\n", count)
		return err
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
