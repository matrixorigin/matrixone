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
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

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
	cmd.AddCommand(listCommand(&storage))
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

func listCommand(storage *toolfs.StorageOptions) *cobra.Command {
	var (
		accountID    uint32
		databaseID   uint64
		tsStr        string
		includeViews bool
		listType     string
	)

	cmd := &cobra.Command{
		Use:   "list [directory]",
		Short: "List checkpoint catalog tables",
		Long: `List table metadata from the checkpoint catalog.

By default this lists ordinary tables. Use --include-views to include views,
and use --database-id or --account-id to narrow the result.`,
		Args: cobra.MaximumNArgs(1),
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

			snapshotTS, err := resolveSnapshotTS(ctx, reader, tsStr)
			if err != nil {
				return fmt.Errorf("resolve --ts: %w", err)
			}

			var accountFilter *uint32
			if cmd.Flags().Changed("account-id") {
				accountFilter = &accountID
			}
			var dbFilter *uint64
			if cmd.Flags().Changed("database-id") {
				dbFilter = &databaseID
			}
			tables, err := reader.ListCatalogTables(ctx, snapshotTS, checkpointtool.TableListOptions{
				AccountID:    accountFilter,
				DatabaseID:   dbFilter,
				IncludeViews: includeViews,
			})
			if err != nil {
				return fmt.Errorf("list checkpoint catalog tables: %w", err)
			}
			if err := printCatalogList(cmd.OutOrStdout(), tables, listType); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().Uint32Var(&accountID, "account-id", 0, "Account ID to list")
	cmd.Flags().Uint64Var(&databaseID, "database-id", 0, "Database ID to list")
	cmd.Flags().BoolVar(&includeViews, "include-views", false, "Include views and non-table relations")
	cmd.Flags().StringVar(&listType, "type", "tables", "List type: tables, databases, or accounts")
	cmd.Flags().StringVar(&tsStr, "ts", "", "Snapshot timestamp: physical:logical, physical-logical, RFC3339, or local time (default: latest)")
	return cmd
}

func printCatalogList(w io.Writer, tables []checkpointtool.TableCatalogEntry, listType string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	switch listType {
	case "", "tables":
		fmt.Fprintln(tw, "ACCOUNT_ID\tDATABASE\tTABLE\tTABLE_ID\tREL_KIND")
		for _, table := range tables {
			fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%s\n", table.AccountID, table.DatabaseName, table.TableName, table.TableID, table.RelKind)
		}
	case "databases", "dbs":
		type dbKey struct {
			accountID uint32
			name      string
			id        uint64
		}
		seen := make(map[dbKey]struct{})
		var dbs []dbKey
		for _, table := range tables {
			key := dbKey{accountID: table.AccountID, name: table.DatabaseName, id: table.DatabaseID}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			dbs = append(dbs, key)
		}
		sort.Slice(dbs, func(i, j int) bool {
			if dbs[i].accountID != dbs[j].accountID {
				return dbs[i].accountID < dbs[j].accountID
			}
			return dbs[i].name < dbs[j].name
		})
		fmt.Fprintln(tw, "ACCOUNT_ID\tDATABASE\tDATABASE_ID")
		for _, db := range dbs {
			fmt.Fprintf(tw, "%d\t%s\t%d\n", db.accountID, db.name, db.id)
		}
	case "accounts", "tenants":
		seen := make(map[uint32]struct{})
		var accounts []uint32
		for _, table := range tables {
			if _, ok := seen[table.AccountID]; ok {
				continue
			}
			seen[table.AccountID] = struct{}{}
			accounts = append(accounts, table.AccountID)
		}
		sort.Slice(accounts, func(i, j int) bool { return accounts[i] < accounts[j] })
		fmt.Fprintln(tw, "ACCOUNT_ID")
		for _, accountID := range accounts {
			fmt.Fprintf(tw, "%d\n", accountID)
		}
	default:
		return fmt.Errorf("unknown --type %q; expected tables, databases, or accounts", listType)
	}
	return tw.Flush()
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
		tableID      uint64
		tableName    string
		accountID    uint32
		databaseID   uint64
		tsStr        string
		output       string
		outputDir    string
		jobs         int
		metaComments bool
		header       bool
		loadScript   bool
		noLoad       bool
		rowOrder     string
		cpuProfile   string
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
  mo-tool ckp dump --table-id=12345 --ts=1749001234567890:1 .  # dump at specific TS
  mo-tool ckp dump --table-id=12345 --load-script -o /tmp/ .
  mo-tool ckp dump --database-id=9001 --table=users -o users.csv .
  mo-tool ckp dump --database-id=9001 --output-dir=/tmp/test-dump --jobs=4 .
  mo-tool ckp dump --database-id=9001 --load-script -o /tmp/test-dump .`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := "."
			if len(args) == 1 {
				dir = args[0]
			}

			if cpuProfile != "" {
				f, err := os.Create(cpuProfile)
				if err != nil {
					return fmt.Errorf("create cpuprofile: %w", err)
				}
				defer f.Close()
				if err := pprof.StartCPUProfile(f); err != nil {
					return fmt.Errorf("start cpuprofile: %w", err)
				}
				defer pprof.StopCPUProfile()
			}

			accountIDSet := cmd.Flags().Changed("account-id")
			tableName = strings.TrimSpace(tableName)
			batchDump := tableID == 0 && tableName == ""
			databaseIDSet := cmd.Flags().Changed("database-id")
			if tableID == 0 && tableName == "" && !databaseIDSet && !accountIDSet {
				return fmt.Errorf("--table-id, --table, or at least one of --database-id/--account-id is required")
			}
			if noLoad && !loadScript {
				return fmt.Errorf("--no-load requires --load-script")
			}
			if tableID != 0 && tableName != "" {
				return fmt.Errorf("--table-id cannot be combined with --table")
			}
			if tableID != 0 && (databaseIDSet || accountIDSet || (outputDir != "" && !loadScript)) {
				return fmt.Errorf("--table-id cannot be combined with --database-id, --account-id, or --output-dir")
			}
			if tableName != "" && outputDir != "" && !loadScript {
				return fmt.Errorf("--output-dir is only valid for --database-id/--account-id batch dumps")
			}
			if batchDump && output != "" && !loadScript {
				return fmt.Errorf("--output cannot be used when dumping by --database-id or --account-id; use --output-dir")
			}
			if loadScript && output == "" {
				return fmt.Errorf("--output/-o directory is required with --load-script")
			}
			if loadScript && outputDir == "" {
				outputDir = output
			}
			if batchDump && outputDir == "" {
				return fmt.Errorf("--output-dir is required when dumping by --database-id or --account-id")
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

			snapshotTS, err := resolveSnapshotTS(ctx, reader, tsStr)
			if err != nil {
				return fmt.Errorf("resolve --ts: %w", err)
			}

			var w = cmd.OutOrStdout()
			var outFile *os.File
			if output != "" && !loadScript {
				outFile, err = os.Create(output)
				if err != nil {
					return fmt.Errorf("create output file: %w", err)
				}
				defer outFile.Close()
				w = outFile
			}

			parsedRowOrder, err := checkpointtool.ParseCSVRowOrder(rowOrder)
			if err != nil {
				return err
			}

			if tableName != "" {
				var accountFilter *uint32
				if accountIDSet {
					accountFilter = &accountID
				}
				var dbFilter *uint64
				if databaseIDSet {
					dbFilter = &databaseID
				}
				table, err := resolveTableByName(ctx, reader, snapshotTS, tableName, dbFilter, accountFilter)
				if err != nil {
					return err
				}
				tableID = table.TableID
			}
			effectiveHeader := header || (loadScript && !noLoad)

			if batchDump {
				var accountFilter *uint32
				if accountIDSet {
					accountFilter = &accountID
				}
				var dbFilter *uint64
				if databaseIDSet {
					dbFilter = &databaseID
				}
				tables, err := reader.ListCatalogTables(ctx, snapshotTS, checkpointtool.TableListOptions{
					AccountID:  accountFilter,
					DatabaseID: dbFilter,
				})
				if err != nil {
					return fmt.Errorf("list checkpoint catalog tables: %w", err)
				}
				if len(tables) == 0 {
					return fmt.Errorf("no checkpoint tables match database-id-set=%v database-id=%d account-id-set=%v account-id=%d", databaseIDSet, databaseID, accountIDSet, accountID)
				}
				if err := os.MkdirAll(outputDir, 0o755); err != nil {
					return fmt.Errorf("create output dir: %w", err)
				}
				if !noLoad {
					if jobs < 1 {
						jobs = 1
					}
					if jobs > len(tables) {
						jobs = len(tables)
					}
					if err := dumpTablesConcurrently(ctx, reader, tables, snapshotTS, outputDir, jobs, parsedRowOrder, metaComments, effectiveHeader, cmd.OutOrStdout()); err != nil {
						return err
					}
					fmt.Fprintf(cmd.OutOrStdout(), "Dumped %d tables to %s\n", len(tables), outputDir)
				}
				if loadScript {
					scriptPath, err := writeRestoreScript(ctx, reader, tables, snapshotTS, output, outputDir, !noLoad, effectiveHeader)
					if err != nil {
						return err
					}
					fmt.Fprintf(cmd.OutOrStdout(), "Restore script written to %s\n", scriptPath)
				}
				return nil
			}

			var tableEntry checkpointtool.TableCatalogEntry
			if loadScript {
				tableEntry, err = resolveTableByID(ctx, reader, snapshotTS, tableID)
				if err != nil {
					return err
				}
				if err := os.MkdirAll(outputDir, 0o755); err != nil {
					return fmt.Errorf("create output dir: %w", err)
				}
				if !noLoad {
					if err := dumpOneTable(ctx, reader, tableEntry, snapshotTS, outputDir, parsedRowOrder, metaComments, effectiveHeader, cmd.OutOrStdout(), &sync.Mutex{}); err != nil {
						return err
					}
				}
				scriptPath, err := writeRestoreScript(ctx, reader, []checkpointtool.TableCatalogEntry{tableEntry}, snapshotTS, output, outputDir, !noLoad, effectiveHeader)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Restore script written to %s\n", scriptPath)
				return nil
			}

			if err := reader.DumpTableCSVComposed(
				ctx,
				w,
				tableID,
				snapshotTS,
				checkpointtool.WithCSVMetaComments(metaComments),
				checkpointtool.WithCSVHeader(effectiveHeader),
				checkpointtool.WithCSVRowOrder(parsedRowOrder),
			); err != nil {
				return fmt.Errorf("dump table %d: %w", tableID, err)
			}

			if output != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Table %d dumped to %s\n", tableID, output)
			}
			return nil
		},
	}

	cmd.Flags().Uint64Var(&tableID, "table-id", 0, "Table ID to dump")
	cmd.Flags().StringVar(&tableName, "table", "", "Table name to dump; use --database-id and/or --account-id to disambiguate")
	cmd.Flags().Uint32Var(&accountID, "account-id", 0, "Account ID to dump tables for; combine with --database-id to narrow the result")
	cmd.Flags().Uint64Var(&databaseID, "database-id", 0, "Database ID to dump tables for")
	cmd.Flags().StringVar(&tsStr, "ts", "", "Snapshot timestamp: physical:logical, physical-logical, RFC3339, or '2006-01-02 15:04:05' in local time (default: latest)")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output CSV file path (default: stdout)")
	cmd.Flags().StringVar(&outputDir, "output-dir", "", "Output directory for database/account dumps")
	cmd.Flags().IntVar(&jobs, "jobs", 1, "Concurrent table dumps for --database-id/--account-id batch dumps")
	cmd.Flags().BoolVar(&metaComments, "meta-comments", false, "Prepend DDL and row-count comment lines (disabled by default so output can be loaded directly)")
	cmd.Flags().BoolVar(&header, "header", false, "Include a CSV header row with column names")
	cmd.Flags().BoolVar(&loadScript, "load-script", false, "Generate restore.sql with CREATE DATABASE, CREATE TABLE, and LOAD DATA statements; --output/-o is treated as a directory")
	cmd.Flags().BoolVar(&noLoad, "no-load", false, "With --load-script, generate only DDL and skip CSV dump and LOAD DATA statements")
	cmd.Flags().StringVar(&rowOrder, "row-order", string(checkpointtool.CSVRowOrderStorage), "CSV row order: storage (streaming, large-table friendly) or lexical (sort by visible CSV values in memory)")
	cmd.Flags().StringVar(&cpuProfile, "cpuprofile", "", "Write CPU profile to file")

	return cmd
}

func parseTS(s string) (types.TS, error) {
	s = strings.TrimSpace(s)
	for _, sep := range []string{":", "-"} {
		parts := strings.SplitN(s, sep, 2)
		if len(parts) != 2 {
			continue
		}
		if !allDigits(parts[0]) || !allDigits(parts[1]) {
			continue
		}
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

	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	} {
		t, err := time.ParseInLocation(layout, s, time.Local)
		if err == nil {
			return types.BuildTS(t.UnixNano(), 0), nil
		}
	}
	return types.TS{}, fmt.Errorf("timestamp must be physical:logical, physical-logical, RFC3339, or local time, got %q", s)
}

func resolveSnapshotTS(ctx context.Context, reader *checkpointtool.CheckpointReader, tsStr string) (types.TS, error) {
	info := reader.Info()
	if strings.TrimSpace(tsStr) == "" {
		if info.LatestTS.IsEmpty() {
			return types.TS{}, fmt.Errorf("no checkpoint timestamp is available")
		}
		if err := reader.ValidateSnapshot(ctx, info.LatestTS); err != nil {
			return types.TS{}, err
		}
		return info.LatestTS, nil
	}

	ts, err := parseTS(tsStr)
	if err != nil {
		return types.TS{}, err
	}
	if ts.IsEmpty() {
		return types.TS{}, fmt.Errorf("timestamp must not be empty")
	}
	if !info.EarliestTS.IsEmpty() && ts.LT(&info.EarliestTS) {
		return types.TS{}, fmt.Errorf("timestamp %s is earlier than earliest checkpoint %s", ts.ToString(), info.EarliestTS.ToString())
	}
	if !info.LatestTS.IsEmpty() && info.LatestTS.LT(&ts) {
		return types.TS{}, fmt.Errorf("timestamp %s is newer than latest checkpoint %s", ts.ToString(), info.LatestTS.ToString())
	}
	if err := reader.ValidateSnapshot(ctx, ts); err != nil {
		return types.TS{}, err
	}
	return ts, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func resolveTableByName(
	ctx context.Context,
	reader *checkpointtool.CheckpointReader,
	snapshotTS types.TS,
	tableName string,
	databaseID *uint64,
	accountID *uint32,
) (checkpointtool.TableCatalogEntry, error) {
	tables, err := reader.ListCatalogTables(ctx, snapshotTS, checkpointtool.TableListOptions{
		AccountID:  accountID,
		DatabaseID: databaseID,
	})
	if err != nil {
		return checkpointtool.TableCatalogEntry{}, fmt.Errorf("list checkpoint catalog tables: %w", err)
	}
	var matches []checkpointtool.TableCatalogEntry
	for _, table := range tables {
		if table.TableName == tableName {
			matches = append(matches, table)
		}
	}
	if len(matches) == 0 {
		return checkpointtool.TableCatalogEntry{}, fmt.Errorf("table %q not found in checkpoint catalog", tableName)
	}
	if len(matches) > 1 {
		var names []string
		for _, table := range matches {
			names = append(names, fmt.Sprintf("account=%d database=%s table-id=%d", table.AccountID, table.DatabaseName, table.TableID))
		}
		return checkpointtool.TableCatalogEntry{}, fmt.Errorf("table %q is ambiguous; use --database-id/--account-id/--table-id (%s)", tableName, strings.Join(names, "; "))
	}
	return matches[0], nil
}

func resolveTableByID(
	ctx context.Context,
	reader *checkpointtool.CheckpointReader,
	snapshotTS types.TS,
	tableID uint64,
) (checkpointtool.TableCatalogEntry, error) {
	tables, err := reader.ListCatalogTables(ctx, snapshotTS, checkpointtool.TableListOptions{
		IncludeViews: true,
	})
	if err != nil {
		return checkpointtool.TableCatalogEntry{}, fmt.Errorf("list checkpoint catalog tables: %w", err)
	}
	for _, table := range tables {
		if table.TableID == tableID {
			return table, nil
		}
	}
	return checkpointtool.TableCatalogEntry{}, fmt.Errorf("table %d not found in checkpoint catalog", tableID)
}

func writeRestoreScript(
	ctx context.Context,
	reader *checkpointtool.CheckpointReader,
	tables []checkpointtool.TableCatalogEntry,
	snapshotTS types.TS,
	scriptDir string,
	csvRoot string,
	includeLoad bool,
	csvHasHeader bool,
) (string, error) {
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		return "", fmt.Errorf("create script dir: %w", err)
	}
	scriptPath := filepath.Join(scriptDir, "restore.sql")
	f, err := os.Create(scriptPath)
	if err != nil {
		return "", fmt.Errorf("create restore script: %w", err)
	}
	defer f.Close()

	currentDB := ""
	for _, table := range tables {
		if table.DatabaseName == "" {
			return "", fmt.Errorf("table %d has empty database name in checkpoint catalog", table.TableID)
		}
		if table.DatabaseName != currentDB {
			if currentDB != "" {
				if _, err := fmt.Fprintln(f); err != nil {
					return "", err
				}
			}
			if _, err := fmt.Fprintf(f, "CREATE DATABASE IF NOT EXISTS %s;\n", quoteSQLIdent(table.DatabaseName)); err != nil {
				return "", err
			}
			if _, err := fmt.Fprintf(f, "USE %s;\n\n", quoteSQLIdent(table.DatabaseName)); err != nil {
				return "", err
			}
			currentDB = table.DatabaseName
		}

		ddl, err := reader.ShowCreateTable(ctx, table.TableID, snapshotTS)
		if err != nil {
			return "", fmt.Errorf("show create table %d: %w", table.TableID, err)
		}
		ddl = normalizeCreateTableDDLName(ddl, table)
		if _, err := fmt.Fprintln(f, strings.TrimRight(ddl, " ;\n\t")); err != nil {
			return "", err
		}
		if _, err := fmt.Fprintln(f, ";"); err != nil {
			return "", err
		}
		if includeLoad {
			if _, err := fmt.Fprintf(f, "\nLOAD DATA INFILE %s\n", quoteSQLString(tableCSVPath(csvRoot, table))); err != nil {
				return "", err
			}
			if _, err := fmt.Fprintf(f, "INTO TABLE %s\n", quoteSQLIdent(table.TableName)); err != nil {
				return "", err
			}
			if _, err := fmt.Fprintln(f, "FIELDS TERMINATED BY ','"); err != nil {
				return "", err
			}
			if _, err := fmt.Fprintln(f, "ENCLOSED BY '\"'"); err != nil {
				return "", err
			}
			if _, err := fmt.Fprintln(f, "LINES TERMINATED BY '\\n'"); err != nil {
				return "", err
			}
			if csvHasHeader {
				if _, err := fmt.Fprintln(f, "IGNORE 1 LINES"); err != nil {
					return "", err
				}
			}
			if _, err := fmt.Fprintln(f, "parallel 'true'"); err != nil {
				return "", err
			}
			if _, err := fmt.Fprintln(f, ";"); err != nil {
				return "", err
			}
		}
		if _, err := fmt.Fprintln(f); err != nil {
			return "", err
		}
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close restore script: %w", err)
	}
	return scriptPath, nil
}

func quoteSQLIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func quoteSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return "'" + s + "'"
}

func normalizeCreateTableDDLName(ddl string, table checkpointtool.TableCatalogEntry) string {
	nameStart, nameEnd, ok := createTableNameRange(ddl)
	if !ok {
		return ddl
	}
	target := quoteSQLIdent(table.TableName)
	if table.DatabaseName != "" {
		target = quoteSQLIdent(table.DatabaseName) + "." + target
	}
	return ddl[:nameStart] + target + ddl[nameEnd:]
}

func createTableNameRange(sql string) (int, int, bool) {
	i, ok := consumeSQLKeyword(sql, 0, "create")
	if !ok {
		return 0, 0, false
	}
	i, ok = consumeSQLKeyword(sql, i, "table")
	if !ok {
		return 0, 0, false
	}
	if next, ok := consumeSQLKeyword(sql, i, "if"); ok {
		if next, ok = consumeSQLKeyword(sql, next, "not"); ok {
			if next, ok = consumeSQLKeyword(sql, next, "exists"); ok {
				i = next
			}
		}
	}
	i = skipSQLSpace(sql, i)
	nameStart := i
	i, ok = consumeSQLIdentifier(sql, i)
	if !ok {
		return 0, 0, false
	}
	if j := skipSQLSpace(sql, i); j < len(sql) && sql[j] == '.' {
		j = skipSQLSpace(sql, j+1)
		if end, ok := consumeSQLIdentifier(sql, j); ok {
			i = end
		}
	}
	return nameStart, i, true
}

func consumeSQLKeyword(sql string, i int, keyword string) (int, bool) {
	i = skipSQLSpace(sql, i)
	if len(sql)-i < len(keyword) || !strings.EqualFold(sql[i:i+len(keyword)], keyword) {
		return i, false
	}
	end := i + len(keyword)
	if end < len(sql) && isSQLIdentByte(sql[end]) {
		return i, false
	}
	return end, true
}

func skipSQLSpace(sql string, i int) int {
	for i < len(sql) {
		switch sql[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func consumeSQLIdentifier(sql string, i int) (int, bool) {
	if i >= len(sql) {
		return i, false
	}
	if sql[i] == '`' {
		i++
		for i < len(sql) {
			if sql[i] != '`' {
				i++
				continue
			}
			if i+1 < len(sql) && sql[i+1] == '`' {
				i += 2
				continue
			}
			return i + 1, true
		}
		return i, false
	}
	start := i
	for i < len(sql) && isSQLIdentByte(sql[i]) {
		i++
	}
	return i, i > start
}

func isSQLIdentByte(b byte) bool {
	return b == '_' || b == '$' ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}

func dumpTablesConcurrently(
	ctx context.Context,
	reader *checkpointtool.CheckpointReader,
	tables []checkpointtool.TableCatalogEntry,
	snapshotTS types.TS,
	outputDir string,
	jobs int,
	rowOrder checkpointtool.CSVRowOrder,
	metaComments bool,
	header bool,
	out io.Writer,
) error {
	tableCh := make(chan checkpointtool.TableCatalogEntry)
	errCh := make(chan error, 1)
	var outMu sync.Mutex
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for table := range tableCh {
			if err := dumpOneTable(ctx, reader, table, snapshotTS, outputDir, rowOrder, metaComments, header, out, &outMu); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go worker()
	}
	for _, table := range tables {
		select {
		case err := <-errCh:
			close(tableCh)
			wg.Wait()
			return err
		case tableCh <- table:
		}
	}
	close(tableCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func tableCSVPath(outputDir string, table checkpointtool.TableCatalogEntry) string {
	return filepath.Join(
		outputDir,
		fmt.Sprintf("account_%d", table.AccountID),
		fmt.Sprintf("db_%d", table.DatabaseID),
		fmt.Sprintf("%s_%d.csv", safePathPart(table.TableName), table.TableID),
	)
}

func dumpOneTable(
	ctx context.Context,
	reader *checkpointtool.CheckpointReader,
	table checkpointtool.TableCatalogEntry,
	snapshotTS types.TS,
	outputDir string,
	rowOrder checkpointtool.CSVRowOrder,
	metaComments bool,
	header bool,
	out io.Writer,
	outMu *sync.Mutex,
) error {
	filePath := tableCSVPath(outputDir, table)
	tableDir := filepath.Dir(filePath)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		return fmt.Errorf("create table output dir: %w", err)
	}
	outFile, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create output file for table %d: %w", table.TableID, err)
	}
	err = reader.DumpTableCSVComposed(
		ctx,
		outFile,
		table.TableID,
		snapshotTS,
		checkpointtool.WithCSVMetaComments(metaComments),
		checkpointtool.WithCSVHeader(header),
		checkpointtool.WithCSVRowOrder(rowOrder),
	)
	closeErr := outFile.Close()
	if err != nil {
		return fmt.Errorf("dump table %d (%s.%s): %w", table.TableID, table.DatabaseName, table.TableName, err)
	}
	if closeErr != nil {
		return fmt.Errorf("close output file for table %d: %w", table.TableID, closeErr)
	}
	outMu.Lock()
	fmt.Fprintf(out, "Table %d %s.%s dumped to %s\n", table.TableID, table.DatabaseName, table.TableName, filePath)
	outMu.Unlock()
	return nil
}

func safePathPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
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

			snapshotTS, err := resolveSnapshotTS(ctx, reader, tsStr)
			if err != nil {
				return fmt.Errorf("resolve --ts: %w", err)
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
	cmd.Flags().StringVar(&tsStr, "ts", "", "Snapshot timestamp: physical:logical, physical-logical, RFC3339, or local time (default: latest)")

	return cmd
}
