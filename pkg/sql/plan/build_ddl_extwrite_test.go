// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestValidateWriteFilePattern(t *testing.T) {
	ctx := context.Background()

	// read-only table: no option => ok
	require.NoError(t, validateWriteFilePattern(ctx, &tree.ExternParam{}))

	// valid csv write pattern
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/part-%U.csv"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p))

	// valid jsonline, format taken from Option
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "write_file_pattern", "stage://s/part-%6N.jl"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p))

	// not a stage path
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "/tmp/part-%U.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p))

	// unsupported format
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.PARQUET,
		Option: []string{"write_file_pattern", "stage://s/part-%U.pq"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p))

	// bad strftime directive
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/part-%Q.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p))

	// jsonline with jsondata 'array' is not writable (writer emits objects)
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "array", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p))

	// jsonline with jsondata from the materialized field
	p = &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.JSONLINE,
			Option: []string{"write_file_pattern", "stage://s/part-%U.jl"},
		},
		ExParam: tree.ExParam{JsonData: tree.ARRAY},
	}
	require.Error(t, validateWriteFilePattern(ctx, p))

	// jsonline with jsondata 'object' stays writable
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p))
}
