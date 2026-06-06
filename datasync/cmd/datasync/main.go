package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	datasync "github.com/matrixorigin/datasync/internal"
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
	cleanupExportAfterImport := flags.Bool("cleanup-export-after-import", false, "in sync mode, delete each table's exported SQL/CSV files after that table imports successfully; false by default")
	showVersion := flags.Bool("version", false, "print version and exit")
	flags.Usage = func() {
		fmt.Fprint(stderr, `Usage: datasync -config <config.yaml> [options]

datasync exports selected MatrixOne source tenant tables with mo-dump -csv
and imports them into configured target databases with mysql source.

Configuration:
  Optional top-level source and target entries define connection defaults:
  name, host, port, user, and password. Each databases entry supplies
  source.database and target.database, and can override any inherited field.
  Incomplete database entries are ignored after default inheritance. Set
  include_tables to allow only specific discovered tables, then use
  exclude_tables to remove tables from that candidate set.

Modes:
  -mode sync
      Default. Discover source tables, export each table, import each table,
      compare target rows with the source row count, and write reports.
  -mode export
      Discover and export only. Import status is recorded as skipped.
  -mode import
      Import from an existing run directory selected by -run-id. This skips
      source discovery and mo-dump, then reuses runs/<run-id>/report.json plus
      the existing SQL/CSV files.

Options:
`)
		flags.PrintDefaults()
		fmt.Fprint(stderr, `
Examples:
  datasync -config configs/example.yaml
  datasync -config configs/example.yaml -mode export -run-id qa-20260606
  datasync -config configs/example.yaml -mode import -run-id qa-20260606
  datasync -config configs/example.yaml -mode sync -cleanup-export-after-import

Notes:
  - Reports are written under output_dir/<run-id>/ as report.json, report.csv,
    and a Chinese summary report.md.
  - Row count mismatches after import are treated as table import failures and
    are retried according to retry.max_attempts.
  - -cleanup-export-after-import only affects successful table imports in sync
    mode. It is false by default, so exported SQL/CSV files are retained unless
    this option is explicitly set.
`)
	}
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
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
	mode, err := datasync.ParseMode(*modeText)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	cfg, err := datasync.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load config: %v\n", err)
		return 1
	}
	if *runID == "" {
		*runID = datasync.NewRunID(time.Now())
	}

	result, err := datasync.App{
		Config: cfg,
		Mode:   mode,
		Options: datasync.Options{
			CleanupExportAfterImport: *cleanupExportAfterImport,
		},
	}.Run(context.Background(), *runID)
	if err != nil {
		fmt.Fprintf(stderr, "datasync failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "run_id=%s mode=%s planned_tasks=%d succeeded=%d failed=%d json_report=%s csv_report=%s markdown_report=%s\n",
		result.RunID,
		mode,
		result.PlannedTasks,
		result.Report.Summary.SucceededTasks,
		result.Report.Summary.FailedTasks,
		result.Report.Summary.JSONReportPath,
		result.Report.Summary.CSVReportPath,
		result.Report.Summary.MarkdownReportPath,
	)
	return 0
}
