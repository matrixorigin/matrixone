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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool/interactive"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "object [file]",
		Short: "Object file tools",
		Long:  "Tools for analyzing and browsing MatrixOne object files",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// If file path is provided, enter view mode directly
			if len(args) == 1 {
				return interactive.RunWithKind(args[0], kindFromFlags(cmd))
			}
			// Otherwise show help
			return cmd.Help()
		},
	}

	// Persistent so the view/info subcommands inherit them. Default (none set) is
	// local DISK (CRC). --local2 / --s3 select the raw formats.
	addOfflineKindFlags(cmd)

	cmd.AddCommand(viewCommand())
	cmd.AddCommand(infoCommand())

	return cmd
}

// addOfflineKindFlags registers the --local / --s3 / --local2 format flags as
// persistent flags so subcommands inherit them.
func addOfflineKindFlags(cmd *cobra.Command) {
	fs := cmd.PersistentFlags()
	fs.Bool("local", false, "local DISK (CRC) format (default)")
	fs.Bool("s3", false, "S3 (raw) format")
	fs.Bool("local2", false, "local DISK-V2 (raw) format")
}

// kindFromFlags resolves the offline fs kind from the format flags, defaulting
// to local.
func kindFromFlags(cmd *cobra.Command) string {
	local, _ := cmd.Flags().GetBool("local")
	s3, _ := cmd.Flags().GetBool("s3")
	local2, _ := cmd.Flags().GetBool("local2")
	return objectio.OfflineKind(local, s3, local2, objectio.OfflineKindLocal)
}
