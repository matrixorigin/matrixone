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

package plan

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/stretchr/testify/require"
)

func TestJSONLineLegacyBinaryOption(t *testing.T) {
	for name, init := range map[string]func(*tree.ExternParam) error{
		"infile": InitInfileParam,
		"s3":     InitS3Param,
		"stage": func(param *tree.ExternParam) error {
			return InitStageS3Param(param, stage.StageDef{
				Url: &url.URL{Scheme: "s3", Host: "bucket", Path: "/prefix/"},
				Credentials: map[string]string{
					stage.PARAMKEY_AWS_KEY_ID: "key", stage.PARAMKEY_AWS_SECRET_KEY: "secret",
					stage.PARAMKEY_AWS_REGION: "region", stage.PARAMKEY_ENDPOINT: "https://example.com",
				},
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				options []string
				want    string
				err     string
			}{
				{name: "default", options: []string{"jsondata", "object"}},
				{name: "legacy", options: []string{"jsondata", "object", JSONBinaryOptionKey, "legacy_base64"}, want: JSONBinaryLegacyBase64},
				{name: "casing", options: []string{"JSON_BINARY", "LEGACY_BASE64", "jsondata", "array"}, want: JSONBinaryLegacyBase64},
				{name: "empty", options: []string{"jsondata", "object", JSONBinaryOptionKey, ""}, err: "unsupported json_binary"},
				{name: "unknown", options: []string{"jsondata", "object", JSONBinaryOptionKey, "auto"}, err: "unsupported json_binary"},
				{name: "csv", options: []string{"format", "csv", JSONBinaryOptionKey, "legacy_base64"}, err: "requires format='jsonline'"},
				{name: "duplicate", options: []string{"jsondata", "object", JSONBinaryOptionKey, "legacy_base64", "JSON_BINARY", "legacy_base64"}, err: "duplicate json_binary"},
				{name: "writable", options: []string{"jsondata", "object", JSONBinaryOptionKey, "legacy_base64", ExternalWriteFilePatternKey, "stage://s/data.jsonl"}, err: "read-only"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Option: append([]string{"filepath", "/legacy.jsonl"}, tc.options...)}}
					param.Ctx = context.Background()
					err := init(param)
					if tc.err != "" {
						require.ErrorContains(t, err, tc.err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, param.JsonBinary)
					if tc.want == "" {
						return
					}
					param.Ctx = nil
					encoded, err := json.Marshal(param)
					require.NoError(t, err)
					var restored tree.ExternParam
					require.NoError(t, json.Unmarshal(encoded, &restored))
					require.Equal(t, param.JsonBinary, restored.JsonBinary)
					require.Equal(t, param.Option, restored.Option)
					require.Contains(t, formatInfileExternalOptionsForShowCreate(param, ""), "'JSON_BINARY'='legacy_base64'")
					require.Contains(t, formatS3ExternalOptionsForShowCreate(param, ""), "'json_binary'='legacy_base64'")
				})
			}
		})
	}
}
