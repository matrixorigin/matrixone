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
