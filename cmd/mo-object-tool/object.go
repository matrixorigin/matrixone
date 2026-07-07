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

package object

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool/interactive"
	"github.com/matrixorigin/matrixone/pkg/tools/toolfs"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	var storage toolfs.StorageOptions
	cmd := &cobra.Command{
		Use:   "object [file]",
		Short: "Object file tools",
		Long:  "Tools for analyzing and browsing MatrixOne object files",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// If file path is provided, enter view mode directly
			if len(args) == 1 {
				kind, err := kindFromFlags(cmd)
				if err != nil {
					return err
				}
				return runObjectView(context.Background(), args[0], storage, kind)
			}
			// Otherwise show help
			return cmd.Help()
		},
	}
	addStorageFlags(cmd, &storage)

	addOfflineKindFlags(cmd)
	cmd.AddCommand(viewCommand(&storage))
	cmd.AddCommand(infoCommand(&storage))

	return cmd
}

func addStorageFlags(cmd *cobra.Command, storage *toolfs.StorageOptions) {
	cmd.PersistentFlags().StringVar(&storage.FSConfig, "fs-config", "", "MO config TOML containing fileservice settings")
	cmd.PersistentFlags().StringVar(&storage.FSName, "fs-name", "SHARED", "fileservice name to use from --fs-config")
	cmd.PersistentFlags().StringVar(&storage.S3, "s3", "", "S3 arguments, for example bucket=...,endpoint=...,region=...,key-prefix=...,key-id=...,key-secret=...")
	cmd.PersistentFlags().StringVar(&storage.Backend, "backend", "", "remote backend for --s3: S3 or MINIO")
}

func runObjectView(ctx context.Context, path string, storage toolfs.StorageOptions, kind string) error {
	if !storage.IsRemote() {
		return interactive.RunWithKind(path, kind)
	}
	fs, _, err := toolfs.Open(ctx, storage)
	if err != nil {
		return err
	}
	defer fs.Close(ctx)
	return interactive.RunWithFS(ctx, fs, path)
}

// addOfflineKindFlags registers the --local / --s3 / --local2 format flags as
// persistent flags so subcommands inherit them. Exactly one must be set.
func addOfflineKindFlags(cmd *cobra.Command) {
	fs := cmd.PersistentFlags()
	fs.Bool("local", false, "local DISK (CRC) format")
	fs.Bool("local2", false, "local DISK-V2 (raw) format")
}

// kindFromFlags resolves the offline fs kind; exactly one of --local/--s3/
// --local2 must be set, else it returns an error.
func kindFromFlags(cmd *cobra.Command) (string, error) {
	local, _ := cmd.Flags().GetBool("local")
	local2, _ := cmd.Flags().GetBool("local2")
	if local && local2 {
		_, err := objectio.OfflineKindStrict(true, false, true)
		return "", err
	}
	if local2 {
		return objectio.OfflineKindLocal2, nil
	}
	return objectio.OfflineKindLocal, nil
}
