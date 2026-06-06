package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/datasync/internal/app"
	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/run"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to YAML configuration file")
	modeText := flag.String("mode", "sync", "run mode: sync, export, or import")
	runID := flag.String("run-id", "", "optional run id")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config")
		os.Exit(2)
	}
	mode, err := run.ParseMode(*modeText)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	if *runID == "" {
		*runID = app.NewRunID(time.Now())
	}

	result, err := app.App{Config: cfg, Mode: mode}.Run(context.Background(), *runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datasync failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("run_id=%s mode=%s planned_tasks=%d succeeded=%d failed=%d json_report=%s csv_report=%s\n",
		result.RunID,
		mode,
		result.PlannedTasks,
		result.Report.Summary.SucceededTasks,
		result.Report.Summary.FailedTasks,
		result.Report.Summary.JSONReportPath,
		result.Report.Summary.CSVReportPath,
	)
}
