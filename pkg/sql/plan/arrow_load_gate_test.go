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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRequireArrowLoadEnabled(t *testing.T) {
	local := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Format: tree.ARROW, ScanType: tree.INFILE},
	}
	require.NoError(t, requireArrowLoadGateError(nil,
		&tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.PARQUET}}))
	require.ErrorContains(t, requireArrowLoadGateError(nil, local), "configuration is unavailable")

	proc := testutil.NewProc(t)
	frontend := &config.FrontendParameters{}
	frontend.SetDefaultValues()
	proc.Ctx = context.WithValue(
		context.Background(), config.ParameterUnitKey,
		config.NewParameterUnit(frontend, nil, nil, nil),
	)
	require.NoError(t, requireArrowLoadGateError(proc, local))

	directS3 := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Format: tree.ARROW, ScanType: tree.S3},
	}
	dynamicMinIO := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format:   tree.ARROW,
			ScanType: tree.INFILE,
			Filepath: "minio,localhost:9000,us-east-1,bucket,key,secret,prefix:input.arrow",
		},
	}
	namedS3, err := fileservice.NewS3FS(context.Background(), fileservice.ObjectStorageArguments{
		Name: "archive", Endpoint: "disk", Bucket: t.TempDir(), NoBucketValidation: true,
	}, fileservice.DisabledCacheConfig, nil, true, true)
	require.NoError(t, err)
	t.Cleanup(func() { namedS3.Close(context.Background()) })
	services, err := fileservice.NewFileServices("archive", namedS3)
	require.NoError(t, err)
	aliasedS3 := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.ARROW, ScanType: tree.INFILE,
			Filepath: namedS3.Name() + ":input.arrow",
		},
		ExParam: tree.ExParam{FileService: services},
	}
	require.ErrorContains(t, requireArrowLoadGateError(proc, directS3), "S3 or stage")
	require.ErrorContains(t, requireArrowLoadGateError(proc, dynamicMinIO), "S3 or stage")
	require.ErrorContains(t, requireArrowLoadGateError(proc, aliasedS3), "S3 or stage")

	frontend.ArrowLoad.Enabled = false
	require.ErrorContains(t, requireArrowLoadGateError(proc, local), "disabled by configuration")
	frontend.ArrowLoad.Enabled = true

	frontend.ArrowLoad.S3Enabled = true
	require.NoError(t, requireArrowLoadGateError(proc, directS3))
	require.NoError(t, requireArrowLoadGateError(proc, dynamicMinIO))
	require.NoError(t, requireArrowLoadGateError(proc, aliasedS3))

	frontend.ArrowLoad.S3Enabled = false
	require.ErrorContains(t, requireArrowLoadGateError(proc, directS3), "S3 or stage")
	require.ErrorContains(t, requireArrowLoadGateError(proc, dynamicMinIO), "S3 or stage")
	require.ErrorContains(t, requireArrowLoadGateError(proc, aliasedS3), "S3 or stage")
	frontend.ArrowLoad.S3Enabled = true
	require.NoError(t, requireArrowLoadGateError(proc, directS3))
	require.NoError(t, requireArrowLoadGateError(proc, dynamicMinIO))
	require.NoError(t, requireArrowLoadGateError(proc, aliasedS3))
}

func requireArrowLoadGateError(proc *process.Process, param *tree.ExternParam) error {
	_, err := RequireArrowLoadEnabled(proc, param)
	return err
}
