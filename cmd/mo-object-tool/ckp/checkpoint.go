// Copyright 2021 Matrix Origin
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

package ckp

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool/interactive"
	"github.com/matrixorigin/matrixone/pkg/tools/toolfs"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	var storage toolfs.StorageOptions
	cmd := &cobra.Command{
		Use:   "ckp [directory]",
		Short: "Checkpoint viewer tool",
		Long:  "Tools for analyzing and browsing MatrixOne checkpoint files",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			return runViewer(dir, storage)
		},
	}
	addStorageFlags(cmd, &storage)

	cmd.AddCommand(infoCommand(&storage))
	cmd.AddCommand(viewCommand(&storage))
	cmd.AddCommand(dumpCommand(&storage))
	cmd.AddCommand(showCreateTableCommand(&storage))

	return cmd
}

func addStorageFlags(cmd *cobra.Command, storage *toolfs.StorageOptions) {
	cmd.PersistentFlags().StringVar(&storage.FSConfig, "fs-config", "", "MO config TOML containing fileservice settings")
	cmd.PersistentFlags().StringVar(&storage.FSName, "fs-name", "SHARED", "fileservice name to use from --fs-config")
	cmd.PersistentFlags().StringVar(&storage.S3, "s3", "", "S3 arguments, for example bucket=...,endpoint=...,region=...,key-prefix=...,key-id=...,key-secret=...")
	cmd.PersistentFlags().StringVar(&storage.Backend, "backend", "", "remote backend for --s3: S3 or MINIO")
}

func setupLogFile() (*os.File, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	logDir := homeDir + "/.mo-tool"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	logPath := logDir + "/ckp.log"
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Redirect logs to file
	logCfg := &logutil.LogConfig{
		Level:    "info",
		Format:   "console",
		Filename: logPath,
		MaxSize:  100,
		MaxDays:  7,
	}
	logutil.SetupMOLogger(logCfg)

	return logFile, nil
}

func runViewer(dir string, storage toolfs.StorageOptions) error {
	logFile, err := setupLogFile()
	if err != nil {
		return fmt.Errorf("setup log file: %w", err)
	}
	defer logFile.Close()

	ctx := context.Background()
	reader, err := openReader(ctx, dir, storage)
	if err != nil {
		return fmt.Errorf("open checkpoint dir: %w", err)
	}
	defer reader.Close()

	return interactive.Run(reader)
}

func infoCommand(storage *toolfs.StorageOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "info [directory]",
		Short: "Show checkpoint summary",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}

			logFile, err := setupLogFile()
			if err != nil {
				return fmt.Errorf("setup log file: %w", err)
			}
			defer logFile.Close()

			ctx := context.Background()
			reader, err := openReader(ctx, dir, *storage)
			if err != nil {
				return fmt.Errorf("open checkpoint dir: %w", err)
			}
			defer reader.Close()

			info := reader.Info()
			cmd.Printf("Checkpoint Directory: %s\n", info.Dir)
			cmd.Printf("Total Entries: %d\n", info.TotalEntries)
			cmd.Printf("  Global:      %d\n", info.GlobalCount)
			cmd.Printf("  Incremental: %d\n", info.IncrCount)
			cmd.Printf("  Compacted:   %d\n", info.CompactCount)
			if !info.EarliestTS.IsEmpty() {
				cmd.Printf("Earliest TS:   %s\n", info.EarliestTS.ToString())
			}
			if !info.LatestTS.IsEmpty() {
				cmd.Printf("Latest TS:     %s\n", info.LatestTS.ToString())
			}
			return nil
		},
	}
}

func viewCommand(storage *toolfs.StorageOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "view [directory]",
		Short: "Interactive checkpoint viewer",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}
			return runViewer(dir, *storage)
		},
	}
}

func openReader(ctx context.Context, dir string, storage toolfs.StorageOptions) (*checkpointtool.CheckpointReader, error) {
	if !storage.IsRemote() {
		return checkpointtool.Open(ctx, dir)
	}
	fs, display, err := toolfs.Open(ctx, storage)
	if err != nil {
		return nil, err
	}
	if display == "" {
		display = dir
	}
	return checkpointtool.OpenWithFS(ctx, fs, display, checkpointtool.WithCloseFS())
}

// dumpCommand implements the "ckp dump" subcommand for offline CSV export.
//
// Usage:
//
//	mo-tool ckp dump --table-id=12345 [--ts=...] [--output=table.csv] [directory]
func dumpCommand(storage *toolfs.StorageOptions) *cobra.Command {
	var (
		tableID uint64
		tsStr   string
		output  string
	)

	cmd := &cobra.Command{
		Use:   "dump [directory]",
		Short: "Dump a table from checkpoint to CSV (offline)",
		Long: `Dump a table from checkpoint to CSV with tombstone filtering applied.

The schema (column names and visible columns) is resolved from mo_tables
and mo_columns system tables in the checkpoint. If that catalog metadata is
unavailable or incomplete, the command fails instead of exporting hidden
physical columns.

Examples:
  mo-tool ckp dump --table-id=12345 /path/to/ckp          # dump to stdout
  mo-tool ckp dump --table-id=12345 -o users.csv .        # dump to file
  mo-tool ckp dump --table-id=12345 --ts=1749001234567890:1 .  # dump at specific TS`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}

			if tableID == 0 {
				return fmt.Errorf("--table-id is required")
			}

			logFile, err := setupLogFile()
			if err != nil {
				return fmt.Errorf("setup log file: %w", err)
			}
			defer logFile.Close()

			ctx := context.Background()
			reader, err := openReader(ctx, dir, *storage)
			if err != nil {
				return fmt.Errorf("open checkpoint dir: %w", err)
			}
			defer reader.Close()

			// Determine snapshot TS
			snapshotTS := types.TS{}
			if tsStr != "" {
				snapshotTS, err = parseTS(tsStr)
				if err != nil {
					return fmt.Errorf("parse --ts: %w", err)
				}
			} else {
				// Use the latest available TS
				info := reader.Info()
				if !info.LatestTS.IsEmpty() {
					snapshotTS = info.LatestTS
				}
			}

			// Open output writer
			var w = cmd.OutOrStdout()
			var outFile *os.File
			if output != "" {
				outFile, err = os.Create(output)
				if err != nil {
					return fmt.Errorf("create output file: %w", err)
				}
				defer outFile.Close()
				w = outFile
			}

			if err := reader.DumpTableCSVComposed(ctx, w, tableID, snapshotTS); err != nil {
				return fmt.Errorf("dump table %d: %w", tableID, err)
			}

			if output != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Table %d dumped to %s\n", tableID, output)
			}
			return nil
		},
	}

	cmd.Flags().Uint64Var(&tableID, "table-id", 0, "Table ID to dump (required)")
	cmd.Flags().StringVar(&tsStr, "ts", "", "Snapshot timestamp in 'physical:logical' format (default: latest)")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output CSV file path (default: stdout)")

	return cmd
}

// parseTS parses a timestamp string in "physical:logical" format.
func parseTS(s string) (types.TS, error) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) == 2 {
		physical, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return types.TS{}, fmt.Errorf("invalid physical in timestamp: %w", err)
		}
		logical, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return types.TS{}, fmt.Errorf("invalid logical in timestamp: %w", err)
		}
		return types.BuildTS(physical, uint32(logical)), nil
	}
	return types.TS{}, fmt.Errorf("timestamp must be in 'physical:logical' format, got %q", s)
}

// showCreateTableCommand implements the "ckp show-create-table" subcommand
// that prints a CREATE TABLE DDL for a table at a given checkpoint snapshot.
//
// Usage:
//
//	mo-tool ckp show-create-table --table-id=12345 [--ts=...] [directory]
func showCreateTableCommand(storage *toolfs.StorageOptions) *cobra.Command {
	var (
		tableID uint64
		tsStr   string
	)

	cmd := &cobra.Command{
		Use:   "show-create-table [directory]",
		Short: "Show CREATE TABLE DDL for a table from checkpoint",
		Long: `Display the CREATE TABLE SQL for a given table by reading the checkpoint's
mo_tables and mo_columns system tables (GCKP + following ICKPs).

The DDL is resolved from mo_tables.rel_createsql if available, otherwise
reconstructed from mo_columns visible column definitions, with hardcoded
fallbacks for core built-in system tables.

Examples:
  mo-tool ckp show-create-table --table-id=12345 /path/to/ckp
  mo-tool ckp show-create-table --table-id=2 --ts=1749001234567890:1 .`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}

			if tableID == 0 {
				return fmt.Errorf("--table-id is required")
			}

			logFile, err := setupLogFile()
			if err != nil {
				return fmt.Errorf("setup log file: %w", err)
			}
			defer logFile.Close()

			ctx := context.Background()
			reader, err := openReader(ctx, dir, *storage)
			if err != nil {
				return fmt.Errorf("open checkpoint dir: %w", err)
			}
			defer reader.Close()

			snapshotTS := types.TS{}
			if tsStr != "" {
				snapshotTS, err = parseTS(tsStr)
				if err != nil {
					return fmt.Errorf("parse --ts: %w", err)
				}
			} else {
				info := reader.Info()
				if !info.LatestTS.IsEmpty() {
					snapshotTS = info.LatestTS
				}
			}

			ddl, err := reader.ShowCreateTable(ctx, tableID, snapshotTS)
			if err != nil {
				return fmt.Errorf("show create table %d: %w", tableID, err)
			}

			fmt.Fprintln(cmd.OutOrStdout(), ddl)
			return nil
		},
	}

	cmd.Flags().Uint64Var(&tableID, "table-id", 0, "Table ID to show CREATE TABLE for (required)")
	cmd.Flags().StringVar(&tsStr, "ts", "", "Snapshot timestamp in 'physical:logical' format (default: latest)")

	return cmd
}
