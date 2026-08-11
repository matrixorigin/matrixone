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

package main

import (
	"os"
	"strings"

	dashboard "github.com/matrixorigin/matrixone/cmd/mo-dashboard"
	debug "github.com/matrixorigin/matrixone/cmd/mo-debug"
	inspect "github.com/matrixorigin/matrixone/cmd/mo-inspect"
	object "github.com/matrixorigin/matrixone/cmd/mo-object-tool"
	ckp "github.com/matrixorigin/matrixone/cmd/mo-object-tool/ckp"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "mo-tool",
		Short: "Mo tool",
		Long:  "Mo tool is a multifunctional development tool",
	}

	rootCmd.AddCommand(debug.PrepareCommand())
	rootCmd.AddCommand(inspect.PrepareCommand())
	rootCmd.AddCommand(dashboard.PrepareCommand())
	rootCmd.AddCommand(object.PrepareCommand())
	rootCmd.AddCommand(ckp.PrepareCommand())

	rootCmd.SetArgs(normalizeLegacyRemoteS3Args(os.Args[1:]))
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// normalizeLegacyRemoteS3Args keeps ckp invocations from before --remote-s3
// was introduced working without changing the local S3FS selector semantics of
// a bare --s3 flag.
func normalizeLegacyRemoteS3Args(args []string) []string {
	normalized := append([]string(nil), args...)
	if len(normalized) == 0 || normalized[0] != "ckp" {
		return normalized
	}

	for i := 1; i < len(normalized); i++ {
		switch {
		case normalized[i] == "--s3" &&
			i+1 < len(normalized) &&
			looksLikeRemoteS3Arguments(normalized[i+1]):
			normalized[i] = "--remote-s3"
			i++
		case strings.HasPrefix(normalized[i], "--s3=") &&
			looksLikeRemoteS3Arguments(strings.TrimPrefix(normalized[i], "--s3=")):
			normalized[i] = "--remote-s3=" + strings.TrimPrefix(normalized[i], "--s3=")
		}
	}

	return normalized
}

func looksLikeRemoteS3Arguments(value string) bool {
	for _, option := range strings.Split(value, ",") {
		key, optionValue, ok := strings.Cut(strings.TrimSpace(option), "=")
		if !ok || optionValue == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(key), "bucket") {
			return true
		}
	}
	return false
}
