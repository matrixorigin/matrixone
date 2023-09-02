// Copyright 2023 Matrix Origin
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

package ctl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHandleEnableFSSpan(t *testing.T) {
	var a1, a2, a3, a4 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}

	// testing query with wrong serviceType
	a1.service = tn
	ret, err := handleEnableFSSpan(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Equal(t, ret, pb.CtlResult{})
	require.Equal(t, err, moerr.NewWrongServiceNoCtx("CN", string(a1.service)))

	// testing query with wrong parameter
	a2.service = cn
	a2.parameter = "test"
	ret, err = handleEnableFSSpan(a2.proc, a2.service, a2.parameter, a2.sender)
	require.Equal(t, ret, pb.CtlResult{})
	require.Equal(t, err, moerr.NewInvalidArgNoCtx("parameter", "expected enable or disable"))

	// testing query enable
	a3.service = cn
	a3.parameter = "enable"
	ret, err = handleEnableFSSpan(a3.proc, a3.service, a3.parameter, a3.sender)
	require.Equal(t, ret, pb.CtlResult{
		Method: pb.CmdMethod_EnableFSSpan.String(),
		Data:   "S3FS and LocalFS are all enabled",
	})
	require.Equal(t, err, nil)

	require.Equal(t, true, trace.MOCtledSpanEnableConfig.EnableS3FSSpan)
	require.Equal(t, true, trace.MOCtledSpanEnableConfig.EnableLocalFSSpan)

	// testing query disable
	a4.service = cn
	a4.parameter = "disable"
	ret, err = handleEnableFSSpan(a4.proc, a4.service, a4.parameter, a4.sender)
	require.Equal(t, ret, pb.CtlResult{
		Method: pb.CmdMethod_EnableFSSpan.String(),
		Data:   "S3FS and LocalFS are all disabled",
	})
	require.Equal(t, err, nil)

	require.Equal(t, false, trace.MOCtledSpanEnableConfig.EnableS3FSSpan)
	require.Equal(t, false, trace.MOCtledSpanEnableConfig.EnableLocalFSSpan)

}
