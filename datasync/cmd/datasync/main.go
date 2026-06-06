package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/matrixorigin/datasync/internal/app"
	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/run"
)

var version = "dev"

func main() {
	os.Exit(exitCode(os.Args[1:], os.Stdout, os.Stderr))
}

func exitCode(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("datasync", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to YAML configuration file")
	modeText := flags.String("mode", "sync", "run mode: sync, export, or import")
	runID := flags.String("run-id", "", "optional run id")
	showVersion := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *showVersion {
		fmt.Fprintln(stdout, version)
		return 0
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "missing required -config")
		return 2
	}
	mode, err := run.ParseMode(*modeText)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	if *runID == "" {
		*runID = app.NewRunID(time.Now())
	}

	result, err := app.App{Config: cfg, Mode: mode}.Run(context.Background(), *runID)
	if err != nil {
		fmt.Fprintf(stderr, "datasync failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "run_id=%s mode=%s planned_tasks=%d succeeded=%d failed=%d json_report=%s csv_report=%s\n",
		result.RunID,
		mode,
		result.PlannedTasks,
		result.Report.Summary.SucceededTasks,
		result.Report.Summary.FailedTasks,
		result.Report.Summary.JSONReportPath,
		result.Report.Summary.CSVReportPath,
	)
	return 0
}
